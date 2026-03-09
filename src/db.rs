use rusqlite::{params, Connection, Result as SqlResult, OptionalExtension};
use std::path::Path;
use crate::error::Result;
use crate::types::InodeMetadata;

pub struct MetadataDb {
    conn: Connection,
}

impl MetadataDb {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let conn = Connection::open(path)?;
        Self::init_schema(&conn)?;
        Ok(Self { conn })
    }

    pub fn new_in_memory() -> Result<Self> {
        let conn = Connection::open_in_memory()?;
        Self::init_schema(&conn)?;
        Ok(Self { conn })
    }

    fn init_schema(conn: &Connection) -> SqlResult<()> {
        conn.execute(
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

        conn.execute(
            "CREATE TABLE IF NOT EXISTS dentries (
                parent_ino INTEGER NOT NULL,
                name TEXT NOT NULL,
                child_ino INTEGER NOT NULL,
                PRIMARY KEY (parent_ino, name)
            )",
            [],
        )?;

        conn.execute(
            "CREATE TABLE IF NOT EXISTS blocks (
                ino INTEGER NOT NULL,
                block_index INTEGER NOT NULL,
                remote_id TEXT NOT NULL,
                PRIMARY KEY (ino, block_index)
            )",
            [],
        )?;

        Ok(())
    }

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

    pub fn get_inode(&self, ino: u64) -> Result<Option<InodeMetadata>> {
        let mut stmt = self.conn.prepare("SELECT mode, uid, gid, size, atime, mtime, ctime, nlink FROM inodes WHERE ino = ?1")?;
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

    pub fn update_inode_size_and_mtime(&self, ino: u64, size: u64, mtime: i64) -> Result<()> {
        self.conn.execute(
            "UPDATE inodes SET size = ?1, mtime = ?2 WHERE ino = ?3",
            params![size, mtime, ino],
        )?;
        Ok(())
    }

    pub fn insert_dentry(&self, parent_ino: u64, name: &str, child_ino: u64) -> Result<()> {
        self.conn.execute(
            "INSERT INTO dentries (parent_ino, name, child_ino) VALUES (?1, ?2, ?3)",
            params![parent_ino, name, child_ino],
        )?;
        Ok(())
    }

    pub fn lookup_dentry(&self, parent_ino: u64, name: &str) -> Result<Option<u64>> {
        let mut stmt = self.conn.prepare("SELECT child_ino FROM dentries WHERE parent_ino = ?1 AND name = ?2")?;
        let child_ino: Option<u64> = stmt.query_row(params![parent_ino, name], |row| row.get(0)).optional()?;
        Ok(child_ino)
    }

    pub fn read_dir(&self, parent_ino: u64) -> Result<Vec<(String, u64)>> {
        let mut stmt = self.conn.prepare("SELECT name, child_ino FROM dentries WHERE parent_ino = ?1")?;
        let entries = stmt.query_map(params![parent_ino], |row| {
            Ok((row.get(0)?, row.get(1)?))
        })?;
        let mut result = Vec::new();
        for entry in entries {
            result.push(entry?);
        }
        Ok(result)
    }

    pub fn delete_dentry(&self, parent_ino: u64, name: &str) -> Result<()> {
        self.conn.execute(
            "DELETE FROM dentries WHERE parent_ino = ?1 AND name = ?2",
            params![parent_ino, name],
        )?;
        Ok(())
    }

    pub fn insert_block(&self, ino: u64, block_index: u64, remote_id: &str) -> Result<()> {
        self.conn.execute(
            "INSERT OR REPLACE INTO blocks (ino, block_index, remote_id) VALUES (?1, ?2, ?3)",
            params![ino, block_index, remote_id],
        )?;
        Ok(())
    }

    pub fn get_block_remote_id(&self, ino: u64, block_index: u64) -> Result<Option<String>> {
        let mut stmt = self.conn.prepare("SELECT remote_id FROM blocks WHERE ino = ?1 AND block_index = ?2")?;
        let remote_id: Option<String> = stmt.query_row(params![ino, block_index], |row| row.get(0)).optional()?;
        Ok(remote_id)
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
}
