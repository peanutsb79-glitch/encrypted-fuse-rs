use thiserror::Error;

#[derive(Error, Debug)]
pub enum FuseError {
    #[error("Database error: {0}")]
    Database(#[from] rusqlite::Error),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Cryptography error: {0}")]
    Crypto(String),

    #[error("Cache error: {0}")]
    Cache(String),

    #[error("VFS logical error: {0}")]
    Vfs(String),
    
    #[error("Invalid arguments: {0}")]
    InvalidArgument(String),
}

pub type Result<T> = std::result::Result<T, FuseError>;
