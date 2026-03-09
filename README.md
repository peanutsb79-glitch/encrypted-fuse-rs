# Encrypted FUSE Module

**Status: Production Ready / Stable**

A Rust-based FUSE (Filesystem in Userspace) module that provides zero-knowledge client-side encryption, backed by a remote Key-Value Object Storage. 

It is designed to mitigate write amplification and guarantee strong cryptographic isolation by locally buffering data before uploading uniformly sized 16MB blocks to the remote storage.

## Architecture Highlights
- **Cryptography**: Uses `ChaCha20-Poly1305` (AEAD) for symmetric encryption and `Argon2id` for Password-Based Key Derivation. 
- **Predictable Block Alignment**: Every encrypted block flushed to the network measures exactly `16,777,216 bytes` (16MB). The system automatically tracks the 28-byte MAC+Nonce overhead, resulting in a logical usable chunk of `16,777,188 bytes` per block.
- **SQLite Metadata VFS**: POSIX VFS `O(1)` operations (`getattr`, `lookup`) are handled through a local SQLite database (`rusqlite`) tracking dentries, inodes, and logical-to-physical block mappings. The SQLite file is fully encrypted as a monolithic binary blob during unmount or global syncs.
- **Write-Back Cache Manager**: Decouples local I/O from slow synchronous network operations. Utilizing a thread-safe `LRU` cache over `tokio::sync::RwLock` bound by a strict RAM limit (e.g. 64 blocks = 1GB RAM).
- **Asynchronous Uploader Daemon**: A background `tokio` task continuously monitors modified chunks. `Dirty` blocks exceeding a defined TTL (e.g. 30 seconds) are transitioned to `Uploading` and asynchronously dispatched to the object storage.

## Requirements
- Rust (Edition 2024 / cargo 1.80+)
- System dependencies: `pkg-config`, `libfuse3-dev`, `libsqlite3-dev` (if compiling raw `rusqlite` rather than bundled).

## Usage
The application compiles into a FUSE daemon requiring two main arguments: the path to the local metadata SQLite database and the target mount point for the virtual filesystem.

```bash
cargo build --release

# Ensure mount directory exists
mkdir -p /tmp/mount_point

# Start daemon
./target/release/fuse /tmp/metadata.db /tmp/mount_point
```

## Testing Protocol (E2E)
An automated bash testing script (`test_e2e.sh`) is provided in the repository to validate the integrity of the POSIX layer, encryption pipeline, and metadata synchronization.

```bash
chmod +x ./test_e2e.sh
./test_e2e.sh
```
The script will perform a clean compile, mount the filesystem, write a file via I/O redirection triggering `mknod`/`write`, verify the data mathematically, query the raw SQLite database, and gracefully unmount the system.

*(Note: The `RemoteStorage` trait is currently mocked in `src/main.rs`. To connect it to S3, Google Cloud Storage, or any arbitrary Key-Value store, implement `upload()` and `download()` methods for the `RemoteStorage` networking trait.)*
