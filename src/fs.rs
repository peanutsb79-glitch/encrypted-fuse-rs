use fuser::{
    FileAttr, FileType, Filesystem, ReplyAttr, ReplyData, ReplyDirectory, ReplyEntry, ReplyWrite,
    ReplyEmpty, ReplyOpen, ReplyCreate, Request,
};
use libc::{ENOENT, EIO, ENOTEMPTY, EEXIST, EISDIR, ENOTDIR};
use std::ffi::OsStr;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::runtime::Handle;

use crate::cache::CacheManager;
use crate::db::MetadataDb;
use crate::types::{BlockId, CacheBlockState, InodeMetadata, LOGICAL_BLOCK_SIZE};

const TTL: Duration = Duration::from_secs(1);

pub struct EncryptedFs {
    db: Arc<MetadataDb>,
    cache: Arc<CacheManager>,
    rt: Handle,
}

impl EncryptedFs {
    pub fn new(db: Arc<MetadataDb>, cache: Arc<CacheManager>, rt: Handle) -> Self {
        Self { db, cache, rt }
    }

    fn now_secs() -> i64 {
        SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64
    }

    fn system_time_from_i64(secs: i64) -> SystemTime {
        UNIX_EPOCH + Duration::from_secs(secs as u64)
    }

    fn file_type_from_mode(mode: u32) -> FileType {
        let fmt = mode & (libc::S_IFMT as u32);
        if fmt == libc::S_IFDIR as u32 {
            FileType::Directory
        } else if fmt == libc::S_IFLNK as u32 {
            FileType::Symlink
        } else {
            FileType::RegularFile
        }
    }

    fn attr_from_inode(inode: &InodeMetadata) -> FileAttr {
        FileAttr {
            ino: inode.ino,
            size: inode.size,
            blocks: (inode.size + 511) / 512,
            atime: Self::system_time_from_i64(inode.atime),
            mtime: Self::system_time_from_i64(inode.mtime),
            ctime: Self::system_time_from_i64(inode.ctime),
            crtime: Self::system_time_from_i64(inode.ctime),
            kind: Self::file_type_from_mode(inode.mode),
            perm: (inode.mode & 0o777) as u16,
            nlink: inode.nlink,
            uid: inode.uid,
            gid: inode.gid,
            rdev: 0,
            flags: 0,
            blksize: 4096,
        }
    }

    /// Helper to evict all cached blocks belonging to an inode.
    fn evict_inode_blocks_from_cache(&self, ino: u64) {
        self.rt.block_on(async {
            let mut cache = self.cache.blocks.write().await;
            let keys: Vec<BlockId> = cache.iter()
                .filter(|(id, _)| id.ino == ino)
                .map(|(id, _)| id.clone())
                .collect();
            for key in keys {
                cache.pop(&key);
            }
        });
    }
}

impl Filesystem for EncryptedFs {
    // ─── lookup ─────────────────────────────────────────────────
    fn lookup(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEntry) {
        let name_str = name.to_string_lossy();
        eprintln!("[FUSE] lookup parent={} name={}", parent, name_str);
        match self.db.lookup_dentry(parent, &name_str) {
            Ok(Some(child_ino)) => match self.db.get_inode(child_ino) {
                Ok(Some(inode)) => reply.entry(&TTL, &Self::attr_from_inode(&inode), 0),
                _ => reply.error(ENOENT),
            },
            Ok(None) => reply.error(ENOENT),
            Err(_) => reply.error(EIO),
        }
    }

    // ─── getattr ────────────────────────────────────────────────
    fn getattr(&mut self, _req: &Request<'_>, ino: u64, reply: ReplyAttr) {
        eprintln!("[FUSE] getattr ino={}", ino);
        match self.db.get_inode(ino) {
            Ok(Some(inode)) => reply.attr(&TTL, &Self::attr_from_inode(&inode)),
            Ok(None) => reply.error(ENOENT),
            Err(_) => reply.error(EIO),
        }
    }

    // ─── setattr (chmod, chown, truncate) ───────────────────────
    fn setattr(
        &mut self, _req: &Request<'_>, ino: u64,
        mode: Option<u32>, uid: Option<u32>, gid: Option<u32>, size: Option<u64>,
        _atime: Option<fuser::TimeOrNow>, _mtime: Option<fuser::TimeOrNow>,
        _ctime: Option<SystemTime>, _fh: Option<u64>,
        _crtime: Option<SystemTime>, _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>, _flags: Option<u32>,
        reply: ReplyAttr,
    ) {
        eprintln!("[FUSE] setattr ino={} mode={:?} size={:?}", ino, mode, size);
        match self.db.get_inode(ino) {
            Ok(Some(mut inode)) => {
                if let Some(m) = mode { inode.mode = (inode.mode & libc::S_IFMT as u32) | (m & 0o7777); }
                if let Some(u) = uid { inode.uid = u; }
                if let Some(g) = gid { inode.gid = g; }
                if let Some(s) = size { inode.size = s; }
                inode.ctime = Self::now_secs();

                match self.db.update_inode(&inode) {
                    Ok(()) => reply.attr(&TTL, &Self::attr_from_inode(&inode)),
                    Err(_) => reply.error(EIO),
                }
            }
            Ok(None) => reply.error(ENOENT),
            Err(_) => reply.error(EIO),
        }
    }

    // ─── mknod (create regular file) ────────────────────────────
    fn mknod(
        &mut self, _req: &Request<'_>, parent: u64, name: &OsStr,
        mode: u32, _umask: u32, _rdev: u32, reply: ReplyEntry,
    ) {
        let name_str = name.to_string_lossy();
        if let Ok(Some(_)) = self.db.lookup_dentry(parent, &name_str) {
            reply.error(EEXIST);
            return;
        }

        let new_ino = match self.db.alloc_ino() {
            Ok(ino) => ino,
            Err(_) => { reply.error(EIO); return; }
        };

        let now = Self::now_secs();
        let inode = InodeMetadata {
            ino: new_ino, mode, uid: _req.uid(), gid: _req.gid(),
            size: 0, atime: now, mtime: now, ctime: now, nlink: 1,
        };

        if self.db.insert_inode(&inode).is_ok() && self.db.insert_dentry(parent, &name_str, new_ino).is_ok() {
            reply.entry(&TTL, &Self::attr_from_inode(&inode), 0);
        } else {
            reply.error(EIO);
        }
    }

    // ─── open (pass-through, always succeeds) ───────────────────
    fn open(&mut self, _req: &Request<'_>, ino: u64, _flags: i32, reply: ReplyOpen) {
        eprintln!("[FUSE] open ino={}", ino);
        reply.opened(ino, 0);
    }

    // ─── access (always OK, permission checks handled in setattr) ─
    fn access(&mut self, _req: &Request<'_>, ino: u64, mask: i32, reply: ReplyEmpty) {
        eprintln!("[FUSE] access ino={} mask={}", ino, mask);
        reply.ok();
    }

    // ─── create (atomic mknod + open, used by O_CREAT) ─────────
    fn create(
        &mut self, _req: &Request<'_>, parent: u64, name: &OsStr,
        mode: u32, _umask: u32, _flags: i32, reply: ReplyCreate,
    ) {
        let name_str = name.to_string_lossy();
        eprintln!("[FUSE] create parent={} name={} mode={:#o}", parent, name_str, mode);
        if let Ok(Some(_)) = self.db.lookup_dentry(parent, &name_str) {
            // File already exists, just open it
            match self.db.lookup_dentry(parent, &name_str) {
                Ok(Some(ino)) => match self.db.get_inode(ino) {
                    Ok(Some(inode)) => {
                        reply.created(&TTL, &Self::attr_from_inode(&inode), 0, ino, 0);
                        return;
                    }
                    _ => { reply.error(EIO); return; }
                },
                _ => { reply.error(EIO); return; }
            }
        }

        let new_ino = match self.db.alloc_ino() {
            Ok(ino) => { eprintln!("[FUSE] create: allocated ino={}", ino); ino },
            Err(e) => { eprintln!("[FUSE] create: alloc_ino failed: {:?}", e); reply.error(EIO); return; }
        };

        let now = Self::now_secs();
        let file_mode = libc::S_IFREG as u32 | (mode & 0o7777);
        let inode = InodeMetadata {
            ino: new_ino, mode: file_mode, uid: _req.uid(), gid: _req.gid(),
            size: 0, atime: now, mtime: now, ctime: now, nlink: 1,
        };

        match self.db.insert_inode(&inode) {
            Ok(()) => eprintln!("[FUSE] create: inserted inode ino={}", new_ino),
            Err(e) => { eprintln!("[FUSE] create: insert_inode failed: {:?}", e); reply.error(EIO); return; }
        }
        match self.db.insert_dentry(parent, &name_str, new_ino) {
            Ok(()) => eprintln!("[FUSE] create: inserted dentry parent={} name={}", parent, name_str),
            Err(e) => { eprintln!("[FUSE] create: insert_dentry failed: {:?}", e); reply.error(EIO); return; }
        }
        eprintln!("[FUSE] create: SUCCESS ino={}", new_ino);
        reply.created(&TTL, &Self::attr_from_inode(&inode), 0, new_ino, 0);
    }

    // ─── mkdir ──────────────────────────────────────────────────
    fn mkdir(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, mode: u32, _umask: u32, reply: ReplyEntry) {
        let name_str = name.to_string_lossy();
        if let Ok(Some(_)) = self.db.lookup_dentry(parent, &name_str) {
            reply.error(EEXIST);
            return;
        }

        let new_ino = match self.db.alloc_ino() {
            Ok(ino) => ino,
            Err(_) => { reply.error(EIO); return; }
        };

        let now = Self::now_secs();
        let dir_mode = libc::S_IFDIR as u32 | (mode & 0o7777);
        let inode = InodeMetadata {
            ino: new_ino, mode: dir_mode, uid: _req.uid(), gid: _req.gid(),
            size: 4096, atime: now, mtime: now, ctime: now, nlink: 2,
        };

        if self.db.insert_inode(&inode).is_ok() && self.db.insert_dentry(parent, &name_str, new_ino).is_ok() {
            // Bump parent nlink
            if let Ok(Some(mut parent_inode)) = self.db.get_inode(parent) {
                parent_inode.nlink += 1;
                let _ = self.db.update_inode(&parent_inode);
            }
            reply.entry(&TTL, &Self::attr_from_inode(&inode), 0);
        } else {
            reply.error(EIO);
        }
    }

    // ─── unlink (remove file) ───────────────────────────────────
    fn unlink(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        let name_str = name.to_string_lossy();

        let child_ino = match self.db.lookup_dentry(parent, &name_str) {
            Ok(Some(ino)) => ino,
            Ok(None) => { reply.error(ENOENT); return; }
            Err(_) => { reply.error(EIO); return; }
        };

        // Ensure it's not a directory
        if let Ok(Some(inode)) = self.db.get_inode(child_ino) {
            if (inode.mode & libc::S_IFMT as u32) == libc::S_IFDIR as u32 {
                reply.error(EISDIR);
                return;
            }
        }

        // Remove dentry, blocks, cache entries, and inode
        let _ = self.db.delete_dentry(parent, &name_str);
        let _ = self.db.delete_blocks_for_inode(child_ino);
        let _ = self.db.delete_inode(child_ino);
        self.evict_inode_blocks_from_cache(child_ino);

        reply.ok();
    }

    // ─── rmdir ──────────────────────────────────────────────────
    fn rmdir(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        let name_str = name.to_string_lossy();

        let child_ino = match self.db.lookup_dentry(parent, &name_str) {
            Ok(Some(ino)) => ino,
            Ok(None) => { reply.error(ENOENT); return; }
            Err(_) => { reply.error(EIO); return; }
        };

        // Ensure it is a directory
        if let Ok(Some(inode)) = self.db.get_inode(child_ino) {
            if (inode.mode & libc::S_IFMT as u32) != libc::S_IFDIR as u32 {
                reply.error(ENOTDIR);
                return;
            }
        }

        // Ensure the directory is empty
        match self.db.read_dir(child_ino) {
            Ok(entries) if !entries.is_empty() => { reply.error(ENOTEMPTY); return; }
            Err(_) => { reply.error(EIO); return; }
            _ => {}
        }

        let _ = self.db.delete_dentry(parent, &name_str);
        let _ = self.db.delete_inode(child_ino);

        // Decrement parent nlink
        if let Ok(Some(mut parent_inode)) = self.db.get_inode(parent) {
            if parent_inode.nlink > 2 { parent_inode.nlink -= 1; }
            let _ = self.db.update_inode(&parent_inode);
        }

        reply.ok();
    }

    // ─── rename ─────────────────────────────────────────────────
    fn rename(
        &mut self, _req: &Request<'_>,
        parent: u64, name: &OsStr,
        newparent: u64, newname: &OsStr,
        _flags: u32, reply: ReplyEmpty,
    ) {
        let old_name = name.to_string_lossy();
        let new_name = newname.to_string_lossy();

        let child_ino = match self.db.lookup_dentry(parent, &old_name) {
            Ok(Some(ino)) => ino,
            Ok(None) => { reply.error(ENOENT); return; }
            Err(_) => { reply.error(EIO); return; }
        };

        match self.db.rename_dentry(parent, &old_name, newparent, &new_name, child_ino) {
            Ok(()) => reply.ok(),
            Err(_) => reply.error(EIO),
        }
    }

    // ─── read ───────────────────────────────────────────────────
    fn read(
        &mut self, _req: &Request<'_>, ino: u64, _fh: u64,
        offset: i64, size: u32, _flags: i32, _lock_owner: Option<u64>,
        reply: ReplyData,
    ) {
        let mut offset = offset as u64;
        let mut size = size as usize;
        let mut output = Vec::with_capacity(size);

        let file_size = match self.db.get_inode(ino) {
            Ok(Some(inode)) => inode.size,
            _ => { reply.error(EIO); return; }
        };

        if offset >= file_size { reply.data(&[]); return; }
        if offset + size as u64 > file_size { size = (file_size - offset) as usize; }

        let mut remaining = size;
        while remaining > 0 {
            let (block_id, internal_offset) = BlockId::from_offset(ino, offset);
            let chunk_size = std::cmp::min(remaining, LOGICAL_BLOCK_SIZE - internal_offset);

            let block_data = self.rt.block_on(self.cache.get_block(&block_id));
            let data = block_data.unwrap_or_else(|| vec![0u8; LOGICAL_BLOCK_SIZE]);

            let end = std::cmp::min(internal_offset + chunk_size, data.len());
            if internal_offset < end {
                output.extend_from_slice(&data[internal_offset..end]);
            } else {
                output.extend(vec![0; chunk_size]);
            }

            offset += chunk_size as u64;
            remaining -= chunk_size;
        }

        reply.data(&output);
    }

    // ─── write ──────────────────────────────────────────────────
    fn write(
        &mut self, _req: &Request<'_>, ino: u64, _fh: u64,
        offset: i64, data: &[u8], _write_flags: u32, _flags: i32,
        _lock_owner: Option<u64>, reply: ReplyWrite,
    ) {
        eprintln!("[FUSE] write ino={} offset={} len={}", ino, offset, data.len());
        let mut offset = offset as u64;
        let mut remaining = data.len();
        let mut data_ptr = 0;

        while remaining > 0 {
            let (block_id, internal_offset) = BlockId::from_offset(ino, offset);
            let chunk_size = std::cmp::min(remaining, LOGICAL_BLOCK_SIZE - internal_offset);

            let mut block_data = if internal_offset > 0 || chunk_size < LOGICAL_BLOCK_SIZE {
                let existing = self.rt.block_on(self.cache.get_block(&block_id));
                existing.unwrap_or_else(|| vec![0u8; LOGICAL_BLOCK_SIZE])
            } else {
                vec![0u8; LOGICAL_BLOCK_SIZE]
            };

            if block_data.len() < LOGICAL_BLOCK_SIZE {
                block_data.resize(LOGICAL_BLOCK_SIZE, 0);
            }

            block_data[internal_offset..internal_offset + chunk_size]
                .copy_from_slice(&data[data_ptr..data_ptr + chunk_size]);

            self.rt.block_on(self.cache.write_block(block_id, block_data));

            offset += chunk_size as u64;
            data_ptr += chunk_size;
            remaining -= chunk_size;
        }

        if let Ok(Some(inode)) = self.db.get_inode(ino) {
            let new_size = std::cmp::max(inode.size, offset);
            let _ = self.db.update_inode_size_and_mtime(ino, new_size, Self::now_secs());
        }

        reply.written(data.len() as u32);
    }

    // ─── flush ──────────────────────────────────────────────────
    fn flush(&mut self, _req: &Request<'_>, ino: u64, _fh: u64, _lock_owner: u64, reply: ReplyEmpty) {
        let cache_clone = self.cache.clone();
        self.rt.block_on(async {
            let mut keys_to_flush = Vec::new();
            {
                let cache_lock = cache_clone.blocks.read().await;
                for (id, entry) in cache_lock.iter() {
                    if id.ino == ino && entry.state == CacheBlockState::Dirty {
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

    // ─── fsync ──────────────────────────────────────────────────
    fn fsync(&mut self, req: &Request<'_>, ino: u64, fh: u64, _datasync: bool, reply: ReplyEmpty) {
        self.flush(req, ino, fh, 0, reply);
    }

    // ─── readdir ────────────────────────────────────────────────
    fn readdir(
        &mut self, _req: &Request<'_>, ino: u64, _fh: u64,
        offset: i64, mut reply: ReplyDirectory,
    ) {
        if offset > 0 { reply.ok(); return; }

        match self.db.read_dir(ino) {
            Ok(entries) => {
                let _ = reply.add(ino, 1, FileType::Directory, ".");
                let _ = reply.add(ino, 2, FileType::Directory, "..");
                for (i, (name, child_ino)) in entries.iter().enumerate() {
                    // Determine the correct file type from the database
                    let ftype = match self.db.get_inode(*child_ino) {
                        Ok(Some(child_inode)) => Self::file_type_from_mode(child_inode.mode),
                        _ => FileType::RegularFile,
                    };
                    if reply.add(*child_ino, (i + 3) as i64, ftype, name) {
                        break;
                    }
                }
                reply.ok();
            }
            Err(_) => reply.error(EIO),
        }
    }
}
