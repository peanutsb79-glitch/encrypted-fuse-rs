use std::sync::Arc;
use tokio::runtime::Runtime;
use std::time::Duration;
use fuse::cache::{CacheManager, RemoteStorage};
use fuse::db::MetadataDb;
use fuse::fs::EncryptedFs;
use fuse::types::BlockId;

struct DummyRemoteStorage;

impl RemoteStorage for DummyRemoteStorage {
    fn upload(&self, _block_id: &BlockId, _data: Vec<u8>) -> std::pin::Pin<Box<dyn std::future::Future<Output = fuse::error::Result<String>> + Send>> {
        Box::pin(async move {
            Ok("dummy_remote_id".to_string())
        })
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 3 {
        eprintln!("Usage: {} <db_path> <mount_point>", args[0]);
        std::process::exit(1);
    }

    let db_path = &args[1];
    let mount_point = &args[2];

    println!("Initializing local metadata database at {}...", db_path);
    let db = Arc::new(MetadataDb::open(db_path)?);

    // Bootstrap root directory if not present
    if db.get_inode(1)?.is_none() {
        let root_inode = fuse::types::InodeMetadata {
            ino: 1,
            mode: libc::S_IFDIR | 0o755,
            uid: unsafe { libc::getuid() },
            gid: unsafe { libc::getgid() },
            size: 4096,
            atime: 0,
            mtime: 0,
            ctime: 0,
            nlink: 2,
        };
        db.insert_inode(&root_inode)?;
    }

    println!("Starting Tokio runtime for background daemon...");
    let rt = Runtime::new()?;
    let handle = rt.handle().clone();

    // 64 blocks max, 30s TTL
    let remote = Arc::new(DummyRemoteStorage);
    let cache = Arc::new(CacheManager::new(64, Duration::from_secs(30), remote));

    // Start block flush daemon
    let daemon_handle = cache.clone().spawn_daemon(&handle);

    println!("Mounting FUSE filesystem at {}...", mount_point);
    let fs = EncryptedFs::new(db, cache, handle);

    let options = vec![
        fuser::MountOption::FSName("encrypted_fuse".to_string()),
    ];

    fuser::mount2(fs, mount_point, &options)?;

    drop(daemon_handle);
    Ok(())
}
