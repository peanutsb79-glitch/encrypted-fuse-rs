// No unused libc imports

/// Nominal size of the remote object (exact size when encrypted and uploaded).
pub const REMOTE_BLOCK_SIZE: usize = 16_777_216; // 16MB

/// Overhead introduced by ChaCha20-Poly1305 (12 bytes Nonce + 16 bytes MAC).
pub const CRYPTO_OVERHEAD: usize = 12 + 16; 

/// The actual useful plaintext size for a logical block.
pub const LOGICAL_BLOCK_SIZE: usize = REMOTE_BLOCK_SIZE - CRYPTO_OVERHEAD;

/// Represents a physical block on the remote object storage.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BlockId {
    pub ino: u64,
    pub index: u64,
}

/// Metadata representation corresponding to the SQLite `inodes` table.
#[derive(Debug, Clone)]
pub struct InodeMetadata {
    pub ino: u64,
    pub mode: u32,
    pub uid: u32,
    pub gid: u32,
    pub size: u64,
    pub atime: i64, // using standard i64 for seconds since epoch
    pub mtime: i64,
    pub ctime: i64,
    pub nlink: u32,
}

/// States of a block in the write-back cache.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CacheBlockState {
    /// The block is in sync with the remote storage.
    Clean,
    /// The block has been modified locally and needs to be uploaded.
    Dirty,
    /// The block is currently being uploaded to remote storage.
    Uploading,
}

/// Represents an entry in the LRU Write-Back Cache.
#[derive(Debug)]
pub struct CacheEntry {
    pub state: CacheBlockState,
    pub data: Vec<u8>,
    pub last_modified: std::time::Instant,
}

/// Helper methods for block indexing and offset calculation.
impl BlockId {
    /// Translates a logical POSIX offset into a block index and internal offset.
    pub fn from_offset(ino: u64, offset: u64) -> (Self, usize) {
        let index = offset / (LOGICAL_BLOCK_SIZE as u64);
        let internal_offset = (offset % (LOGICAL_BLOCK_SIZE as u64)) as usize;
        (
            Self { ino, index },
            internal_offset,
        )
    }
}
