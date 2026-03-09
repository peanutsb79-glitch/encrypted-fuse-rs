use fuser::{
    FileAttr, FileType, Filesystem, ReplyAttr, ReplyData, ReplyDirectory, ReplyEntry, ReplyWrite,
    ReplyEmpty, Request,
};
use libc::{ENOENT, EIO};
use std::ffi::OsStr;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::runtime::Handle;

use crate::cache::CacheManager;
use crate::db::MetadataDb;
use crate::types::{BlockId, InodeMetadata, LOGICAL_BLOCK_SIZE};

const TTL: Duration = Duration::from_secs(1); // Standard FUSE TTL

pub struct EncryptedFs {
    db: Arc<MetadataDb>,
    cache: Arc<CacheManager>,
    rt: Handle,
}

impl EncryptedFs {
    pub fn new(db: Arc<MetadataDb>, cache: Arc<CacheManager>, rt: Handle) -> Self {
        Self { db, cache, rt }
    }

    fn system_time_from_i64(secs: i64) -> SystemTime {
        UNIX_EPOCH + Duration::from_secs(secs as u64)
    }

    fn attr_from_inode(&self, inode: &InodeMetadata) -> FileAttr {
        let kind = if inode.mode & libc::S_IFDIR as u32 != 0 {
            FileType::Directory
        } else {
            FileType::RegularFile
        };

        FileAttr {
            ino: inode.ino,
            size: inode.size,
            blocks: (inode.size + 511) / 512,
            atime: Self::system_time_from_i64(inode.atime),
            mtime: Self::system_time_from_i64(inode.mtime),
            ctime: Self::system_time_from_i64(inode.ctime),
            crtime: Self::system_time_from_i64(inode.ctime),
            kind,
            perm: (inode.mode & 0o777) as u16,
            nlink: inode.nlink,
            uid: inode.uid,
            gid: inode.gid,
            rdev: 0,
            flags: 0,
            blksize: 4096,
        }
    }
}

impl Filesystem for EncryptedFs {
    fn lookup(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEntry) {
        let name_str = name.to_string_lossy();
        match self.db.lookup_dentry(parent, &name_str) {
            Ok(Some(child_ino)) => {
                match self.db.get_inode(child_ino) {
                    Ok(Some(inode)) => {
                        reply.entry(&TTL, &self.attr_from_inode(&inode), 0);
                    }
                    _ => reply.error(ENOENT),
                }
            }
            Ok(None) => reply.error(ENOENT),
            Err(_) => reply.error(EIO),
        }
    }

    fn getattr(&mut self, _req: &Request<'_>, ino: u64, reply: ReplyAttr) {
        match self.db.get_inode(ino) {
            Ok(Some(inode)) => reply.attr(&TTL, &self.attr_from_inode(&inode)),
            Ok(None) => reply.error(ENOENT),
            Err(_) => reply.error(EIO),
        }
    }

    fn read(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyData,
    ) {
        let mut offset = offset as u64;
        let mut size = size as usize;
        let mut output = Vec::with_capacity(size);

        // Fetch actual file size to avoid reading past EOF
        let file_size = match self.db.get_inode(ino) {
            Ok(Some(inode)) => inode.size,
            _ => {
                reply.error(EIO);
                return;
            }
        };

        if offset >= file_size {
            reply.data(&[]);
            return;
        }

        if offset + size as u64 > file_size {
            size = (file_size - offset) as usize;
        }

        let mut remaining = size;

        while remaining > 0 {
            let (block_id, internal_offset) = BlockId::from_offset(ino, offset);
            let chunk_size = std::cmp::min(remaining, LOGICAL_BLOCK_SIZE - internal_offset);

            // Block on async cache get
            let block_data = self.rt.block_on(self.cache.get_block(&block_id));
            let data = match block_data {
                Some(cached) => cached,
                None => {
                    // Logic to download from remote would go here.
                    // For now, we simulate zero-fill if not found locally.
                    vec![0u8; LOGICAL_BLOCK_SIZE]
                }
            };

            let end = std::cmp::min(internal_offset + chunk_size, data.len());
            if internal_offset < end {
                output.extend_from_slice(&data[internal_offset..end]);
            } else {
                // Should not happen unless data is unexpectedly short
                output.extend(vec![0; chunk_size]);
            }

            offset += chunk_size as u64;
            remaining -= chunk_size;
        }

        reply.data(&output);
    }

    fn write(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        data: &[u8],
        _write_flags: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyWrite,
    ) {
        let mut offset = offset as u64;
        let mut remaining = data.len();
        let mut data_ptr = 0;

        while remaining > 0 {
            let (block_id, internal_offset) = BlockId::from_offset(ino, offset);
            let chunk_size = std::cmp::min(remaining, LOGICAL_BLOCK_SIZE - internal_offset);

            // Fetch existing block if partial write
            let mut block_data = if internal_offset > 0 || chunk_size < LOGICAL_BLOCK_SIZE {
                let existing = self.rt.block_on(self.cache.get_block(&block_id));
                existing.unwrap_or_else(|| vec![0u8; LOGICAL_BLOCK_SIZE])
            } else {
                vec![0u8; LOGICAL_BLOCK_SIZE]
            };

            // Ensure block has logical size
            if block_data.len() < LOGICAL_BLOCK_SIZE {
                block_data.resize(LOGICAL_BLOCK_SIZE, 0);
            }

            // Write the chunk
            block_data[internal_offset..internal_offset + chunk_size]
                .copy_from_slice(&data[data_ptr..data_ptr + chunk_size]);

            // Save back to cache asynchronously (but we block_on to ensure it's recorded)
            self.rt.block_on(self.cache.write_block(block_id, block_data));

            offset += chunk_size as u64;
            data_ptr += chunk_size;
            remaining -= chunk_size;
        }

        // Update inode size
        if let Ok(Some(inode)) = self.db.get_inode(ino) {
            let new_size = std::cmp::max(inode.size, offset);
            let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64;
            let _ = self.db.update_inode_size_and_mtime(ino, new_size, now);
        }

        reply.written(data.len() as u32);
    }

    fn flush(&mut self, _req: &Request<'_>, ino: u64, _fh: u64, _lock_owner: u64, reply: ReplyEmpty) {
        // "Forza l'upload sincrono di tutti i blocchi Dirty associati a un inode"
        let cache_clone = self.cache.clone();
        
        // Find blocks for this inode and flush them
        self.rt.block_on(async {
            let mut keys_to_flush = Vec::new();
            {
                let cache_lock = cache_clone.blocks.read().await;
                for (id, entry) in cache_lock.iter() {
                    if id.ino == ino && entry.state == crate::types::CacheBlockState::Dirty {
                        keys_to_flush.push(id.clone());
                    }
                }
            }
            
            for id in keys_to_flush {
                let _ = cache_clone.flush_block(&id).await;
            }
        });

        reply.ok();
    }

    fn fsync(&mut self, req: &Request<'_>, ino: u64, fh: u64, _datasync: bool, reply: ReplyEmpty) {
        // Fsync has the same semantics as flush for our data integrity
        self.flush(req, ino, fh, 0, reply);
    }

    fn readdir(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        if offset > 0 {
            reply.ok();
            return;
        }

        match self.db.read_dir(ino) {
            Ok(entries) => {
                let _ = reply.add(ino, 1, FileType::Directory, ".");
                let _ = reply.add(ino, 2, FileType::Directory, "..");
                for (i, (name, child_ino)) in entries.iter().enumerate() {
                    // Offset i+3 because of . and ..
                    if reply.add(*child_ino, (i + 3) as i64, FileType::RegularFile, name) {
                        break;
                    }
                }
                reply.ok();
            }
            Err(_) => reply.error(EIO),
        }
    }

    fn mknod(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        _rdev: u32,
        reply: ReplyEntry,
    ) {
        let name_str = name.to_string_lossy();
        if let Ok(Some(_)) = self.db.lookup_dentry(parent, &name_str) {
            reply.error(libc::EEXIST);
            return;
        }

        let new_ino = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos() as u64; // Simple ino generation
        
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64;
        let inode = InodeMetadata {
            ino: new_ino,
            mode,
            uid: _req.uid(),
            gid: _req.gid(),
            size: 0,
            atime: now,
            mtime: now,
            ctime: now,
            nlink: 1,
        };

        if self.db.insert_inode(&inode).is_ok() && self.db.insert_dentry(parent, &name_str, new_ino).is_ok() {
            reply.entry(&TTL, &self.attr_from_inode(&inode), 0);
        } else {
            reply.error(EIO);
        }
    }

    fn setattr(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        _atime: Option<fuser::TimeOrNow>,
        _mtime: Option<fuser::TimeOrNow>,
        _ctime: Option<std::time::SystemTime>,
        _fh: Option<u64>,
        _crtime: Option<std::time::SystemTime>,
        _chgtime: Option<std::time::SystemTime>,
        _bkuptime: Option<std::time::SystemTime>,
        _flags: Option<u32>,
        reply: ReplyAttr,
    ) {
        if let Ok(Some(mut inode)) = self.db.get_inode(ino) {
            if let Some(m) = mode { inode.mode = m; }
            if let Some(u) = uid { inode.uid = u; }
            if let Some(g) = gid { inode.gid = g; }
            if let Some(s) = size { inode.size = s; }
            
            let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64;
            inode.ctime = now;
            
            // Just updating size and mtime for now to satisfy truncate
            if self.db.update_inode_size_and_mtime(ino, inode.size, now).is_ok() {
                reply.attr(&TTL, &self.attr_from_inode(&inode));
            } else {
                reply.error(EIO);
            }
        } else {
            reply.error(ENOENT);
        }
    }
}
