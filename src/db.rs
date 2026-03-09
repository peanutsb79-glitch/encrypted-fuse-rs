use rusqlite::{params, Connection, Result as SqlResult, OptionalExtension};
use std::path::Path;
use crate::error::Result;
use crate::types::InodeMetadata;
use crate::crypto;

pub struct MetadataDb {
    conn: Connection,
    db_path: Option<String>,
}

impl MetadataDb {
    /// Opens or creates the database at the given path.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path_str = path.as_ref().to_string_lossy().to_string();
        let conn = Connection::open(&path)?;
        let db = Self { conn, db_path: Some(path_str) };
        db.init_schema()?;
        Ok(db)
    }

    /// Creates an in-memory database (for testing).
    pub fn new_in_memory() -> Result<Self> {
        let conn = Connection::open_in_memory()?;
        let db = Self { conn, db_path: None };
        db.init_schema()?;
        Ok(db)
    }

    fn init_schema(&self) -> SqlResult<()> {
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS inodes (
                ino INTEGER PRIMARY KEY,
                mode INTEGER NOT NULL,
                uid INTEGER NOT NULL,
                gid INTEGER NOT NULL,
                size INTEGER NOT NULL,
                atime INTEGER NOT NULL,
                mtime INTEGER NOT NULL,
                ctime INTEGER NOT NULL,
                nlink INTEGER NOT NULL
            )",
            [],
        )?;

        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS dentries (
                parent_ino INTEGER NOT NULL,
                name TEXT NOT NULL,
                child_ino INTEGER NOT NULL,
                PRIMARY KEY (parent_ino, name)
            )",
            [],
        )?;

        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS blocks (
                ino INTEGER NOT NULL,
                block_index INTEGER NOT NULL,
                remote_id TEXT NOT NULL,
                PRIMARY KEY (ino, block_index)
            )",
            [],
        )?;

        // Persistent counter for inode allocation (avoids nanos collision)
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS counters (
                key TEXT PRIMARY KEY,
                value INTEGER NOT NULL
            )",
            [],
        )?;

        // Seed the inode counter at 2 (1 is reserved for root)
        self.conn.execute(
            "INSERT OR IGNORE INTO counters (key, value) VALUES ('next_ino', 2)",
            [],
        )?;

        Ok(())
    }

    // ─── Inode Counter ──────────────────────────────────────────

    /// Atomically allocates the next unique inode number.
    pub fn alloc_ino(&self) -> Result<u64> {
        self.conn.execute(
            "UPDATE counters SET value = value + 1 WHERE key = 'next_ino'",
            [],
        )?;
        let ino: u64 = self.conn.query_row(
            "SELECT value FROM counters WHERE key = 'next_ino'",
            [],
            |row| row.get(0),
        )?;
        Ok(ino)
    }

    // ─── Inode CRUD ─────────────────────────────────────────────

    /// Inserts a new inode record.
    pub fn insert_inode(&self, inode: &InodeMetadata) -> Result<()> {
        self.conn.execute(
            "INSERT INTO inodes (ino, mode, uid, gid, size, atime, mtime, ctime, nlink)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params![
                inode.ino, inode.mode, inode.uid, inode.gid, inode.size,
                inode.atime, inode.mtime, inode.ctime, inode.nlink
            ],
        )?;
        Ok(())
    }

    /// Gets an inode by its number.
    pub fn get_inode(&self, ino: u64) -> Result<Option<InodeMetadata>> {
        let mut stmt = self.conn.prepare(
            "SELECT mode, uid, gid, size, atime, mtime, ctime, nlink FROM inodes WHERE ino = ?1",
        )?;
        let inode = stmt.query_row(params![ino], |row| {
            Ok(InodeMetadata {
                ino,
                mode: row.get(0)?,
                uid: row.get(1)?,
                gid: row.get(2)?,
                size: row.get(3)?,
                atime: row.get(4)?,
                mtime: row.get(5)?,
                ctime: row.get(6)?,
                nlink: row.get(7)?,
            })
        }).optional()?;
        Ok(inode)
    }

    /// Updates size and mtime of an inode (used by write).
    pub fn update_inode_size_and_mtime(&self, ino: u64, size: u64, mtime: i64) -> Result<()> {
        self.conn.execute(
            "UPDATE inodes SET size = ?1, mtime = ?2 WHERE ino = ?3",
            params![size, mtime, ino],
        )?;
        Ok(())
    }

    /// Fully updates all mutable fields of an inode (used by setattr/chmod/chown).
    pub fn update_inode(&self, inode: &InodeMetadata) -> Result<()> {
        self.conn.execute(
            "UPDATE inodes SET mode=?1, uid=?2, gid=?3, size=?4, atime=?5, mtime=?6, ctime=?7, nlink=?8
             WHERE ino = ?9",
            params![
                inode.mode, inode.uid, inode.gid, inode.size,
                inode.atime, inode.mtime, inode.ctime, inode.nlink, inode.ino
            ],
        )?;
        Ok(())
    }

    /// Deletes an inode record entirely.
    pub fn delete_inode(&self, ino: u64) -> Result<()> {
        self.conn.execute("DELETE FROM inodes WHERE ino = ?1", params![ino])?;
        Ok(())
    }

    // ─── Dentry CRUD ────────────────────────────────────────────

    /// Inserts a directory entry linking parent to child by name.
    pub fn insert_dentry(&self, parent_ino: u64, name: &str, child_ino: u64) -> Result<()> {
        self.conn.execute(
            "INSERT INTO dentries (parent_ino, name, child_ino) VALUES (?1, ?2, ?3)",
            params![parent_ino, name, child_ino],
        )?;
        Ok(())
    }

    /// Looks up a child inode by parent and name.
    pub fn lookup_dentry(&self, parent_ino: u64, name: &str) -> Result<Option<u64>> {
        let mut stmt = self.conn.prepare(
            "SELECT child_ino FROM dentries WHERE parent_ino = ?1 AND name = ?2",
        )?;
        let child_ino: Option<u64> = stmt.query_row(params![parent_ino, name], |row| row.get(0)).optional()?;
        Ok(child_ino)
    }

    /// Lists all entries in a directory.
    pub fn read_dir(&self, parent_ino: u64) -> Result<Vec<(String, u64)>> {
        let mut stmt = self.conn.prepare(
            "SELECT name, child_ino FROM dentries WHERE parent_ino = ?1",
        )?;
        let entries = stmt.query_map(params![parent_ino], |row| {
            Ok((row.get(0)?, row.get(1)?))
        })?;
        let mut result = Vec::new();
        for entry in entries {
            result.push(entry?);
        }
        Ok(result)
    }

    /// Deletes a single directory entry.
    pub fn delete_dentry(&self, parent_ino: u64, name: &str) -> Result<()> {
        self.conn.execute(
            "DELETE FROM dentries WHERE parent_ino = ?1 AND name = ?2",
            params![parent_ino, name],
        )?;
        Ok(())
    }

    /// Atomic rename: removes the old dentry and inserts a new one.
    pub fn rename_dentry(
        &self,
        old_parent: u64, old_name: &str,
        new_parent: u64, new_name: &str,
        child_ino: u64,
    ) -> Result<()> {
        let tx = self.conn.unchecked_transaction()?;
        tx.execute(
            "DELETE FROM dentries WHERE parent_ino = ?1 AND name = ?2",
            params![old_parent, old_name],
        )?;
        // Overwrite if target already exists
        tx.execute(
            "DELETE FROM dentries WHERE parent_ino = ?1 AND name = ?2",
            params![new_parent, new_name],
        )?;
        tx.execute(
            "INSERT INTO dentries (parent_ino, name, child_ino) VALUES (?1, ?2, ?3)",
            params![new_parent, new_name, child_ino],
        )?;
        tx.commit()?;
        Ok(())
    }

    // ─── Blocks CRUD ────────────────────────────────────────────

    /// Inserts or replaces a block mapping.
    pub fn insert_block(&self, ino: u64, block_index: u64, remote_id: &str) -> Result<()> {
        self.conn.execute(
            "INSERT OR REPLACE INTO blocks (ino, block_index, remote_id) VALUES (?1, ?2, ?3)",
            params![ino, block_index, remote_id],
        )?;
        Ok(())
    }

    /// Gets the remote ID for a block.
    pub fn get_block_remote_id(&self, ino: u64, block_index: u64) -> Result<Option<String>> {
        let mut stmt = self.conn.prepare(
            "SELECT remote_id FROM blocks WHERE ino = ?1 AND block_index = ?2",
        )?;
        let remote_id: Option<String> = stmt.query_row(params![ino, block_index], |row| row.get(0)).optional()?;
        Ok(remote_id)
    }

    /// Deletes all block mappings for a given inode (used on unlink).
    pub fn delete_blocks_for_inode(&self, ino: u64) -> Result<()> {
        self.conn.execute("DELETE FROM blocks WHERE ino = ?1", params![ino])?;
        Ok(())
    }

    // ─── DB Encryption (unmount sync) ───────────────────────────

    /// Encrypts the entire SQLite file as a binary blob and writes it to disk.
    /// Called during unmount or global sync. The encrypted blob can then be
    /// uploaded to object storage as a single opaque object.
    pub fn export_encrypted(&self, key: &[u8; 32], output_path: &str) -> Result<()> {
        let db_path = self.db_path.as_deref()
            .ok_or_else(|| crate::error::FuseError::Vfs("Cannot export in-memory DB".into()))?;

        // Checkpoint WAL to ensure all data is in the main file
        self.conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")?;

        let plain_bytes = std::fs::read(db_path)?;
        let encrypted = crypto::encrypt_block(key, &plain_bytes)?;
        std::fs::write(output_path, &encrypted)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_db_operations() {
        let db = MetadataDb::new_in_memory().unwrap();

        let inode = InodeMetadata {
            ino: 1, mode: 0o755, uid: 1000, gid: 1000, size: 0,
            atime: 0, mtime: 0, ctime: 0, nlink: 2,
        };
        db.insert_inode(&inode).unwrap();

        let retrieved = db.get_inode(1).unwrap().unwrap();
        assert_eq!(retrieved.mode, 0o755);

        db.insert_dentry(1, "test.txt", 2).unwrap();
        assert_eq!(db.lookup_dentry(1, "test.txt").unwrap(), Some(2));
        assert_eq!(db.lookup_dentry(1, "missing.txt").unwrap(), None);

        let files = db.read_dir(1).unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].0, "test.txt");
        assert_eq!(files[0].1, 2);

        db.insert_block(2, 0, "remote_1").unwrap();
        assert_eq!(db.get_block_remote_id(2, 0).unwrap(), Some("remote_1".to_string()));
    }

    #[test]
    fn test_inode_counter() {
        let db = MetadataDb::new_in_memory().unwrap();
        let ino1 = db.alloc_ino().unwrap();
        let ino2 = db.alloc_ino().unwrap();
        // Counter starts at 2 (root=1), so first alloc returns 3, next 4
        assert_eq!(ino1, 3);
        assert_eq!(ino2, 4);
        assert_ne!(ino1, ino2);
    }

    #[test]
    fn test_rename_dentry() {
        let db = MetadataDb::new_in_memory().unwrap();
        db.insert_dentry(1, "old.txt", 42).unwrap();
        db.rename_dentry(1, "old.txt", 1, "new.txt", 42).unwrap();
        assert_eq!(db.lookup_dentry(1, "old.txt").unwrap(), None);
        assert_eq!(db.lookup_dentry(1, "new.txt").unwrap(), Some(42));
    }

    #[test]
    fn test_delete_inode_and_blocks() {
        let db = MetadataDb::new_in_memory().unwrap();
        let inode = InodeMetadata {
            ino: 10, mode: 0o644, uid: 0, gid: 0, size: 100,
            atime: 0, mtime: 0, ctime: 0, nlink: 1,
        };
        db.insert_inode(&inode).unwrap();
        db.insert_block(10, 0, "blk_a").unwrap();
        db.insert_block(10, 1, "blk_b").unwrap();

        db.delete_blocks_for_inode(10).unwrap();
        assert_eq!(db.get_block_remote_id(10, 0).unwrap(), None);

        db.delete_inode(10).unwrap();
        assert!(db.get_inode(10).unwrap().is_none());
    }
}
