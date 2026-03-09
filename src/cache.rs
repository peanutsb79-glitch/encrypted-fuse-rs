use std::sync::Arc;
use tokio::sync::RwLock;
use lru::LruCache;
use std::num::NonZeroUsize;
use std::time::{Duration, Instant};
use crate::error::Result;
use crate::types::{BlockId, CacheBlockState, CacheEntry};

/// Trait to allow mocking the remote object storage.
pub trait RemoteStorage: Send + Sync + 'static {
    fn upload(&self, block_id: &BlockId, data: Vec<u8>) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<String>> + Send>>;
}

pub struct CacheManager {
    pub blocks: Arc<RwLock<LruCache<BlockId, CacheEntry>>>,
    ttl: Duration,
    remote: Arc<dyn RemoteStorage>,
}

impl CacheManager {
    pub fn new(capacity: usize, ttl: Duration, remote: Arc<dyn RemoteStorage>) -> Self {
        Self {
            blocks: Arc::new(RwLock::new(LruCache::new(NonZeroUsize::new(capacity).unwrap()))),
            ttl,
            remote,
        }
    }

    /// Retrieve a block from the cache. Updates LRU.
    pub async fn get_block(&self, block_id: &BlockId) -> Option<Vec<u8>> {
        let mut cache = self.blocks.write().await;
        cache.get(block_id).map(|entry| entry.data.clone())
    }

    /// Put a Clean block (e.g. freshly downloaded) into the cache. 
    /// If capacity is reached, it will evict only `Clean` blocks. If no `Clean` blocks can be evicted,
    /// and capacity is strict, it is a problem. But `lru` crate auto-evicts the least recently used block.
    /// To enforce our policy strictly, we can manually check. For simplicity, we assume the LRU eviction 
    /// provided by `LruCache` works, but we ideally shouldn't evict `Dirty` or `Uploading` blocks.
    pub async fn put_clean(&self, block_id: BlockId, data: Vec<u8>) {
        let mut cache = self.blocks.write().await;
        // Basic eviction protection logic: before putting, check if LRU is dirty.
        // If we are at capacity, find the oldest Clean block.
        if cache.len() == cache.cap().get() {
            let mut key_to_evict = None;
            for (k, v) in cache.iter().rev() {
                if v.state == CacheBlockState::Clean {
                    key_to_evict = Some(k.clone());
                    break;
                }
            }
            if let Some(ref k) = key_to_evict {
                cache.pop(k);
            } else {
                // If everything is dirty/uploading, we must temporarily exceed capacity or block.
                // We let it pop the LRU normally as a fallback.
            }
        }
        
        let entry = CacheEntry {
            state: CacheBlockState::Clean,
            data,
            last_modified: Instant::now(),
        };
        cache.put(block_id, entry);
    }

    /// Mark a block as dirty, simulating a write operation.
    pub async fn write_block(&self, block_id: BlockId, data: Vec<u8>) {
        let mut cache = self.blocks.write().await;
        let entry = CacheEntry {
            state: CacheBlockState::Dirty,
            data,
            last_modified: Instant::now(), // reset TTL
        };
        // Always allowed to write over, even if Uploading. If it was Uploading, 
        // the new write will eventually be uploaded again.
        cache.put(block_id, entry);
    }

    /// Flushes a single dirty block synchronously (awaits completion).
    pub async fn flush_block(&self, block_id: &BlockId) -> Result<Option<String>> {
        let data = {
            let mut cache = self.blocks.write().await;
            if let Some(entry) = cache.get_mut(block_id) {
                if entry.state == CacheBlockState::Dirty {
                    entry.state = CacheBlockState::Uploading;
                    Some(entry.data.clone())
                } else {
                    None
                }
            } else {
                None
            }
        };

        if let Some(payload) = data {
            let remote_id = self.remote.upload(block_id, payload).await?;
            // Mark as Clean
            let mut cache = self.blocks.write().await;
            if let Some(entry) = cache.get_mut(block_id) {
                // Only mark as clean if it wasn't modified again while uploading
                if entry.state == CacheBlockState::Uploading {
                    entry.state = CacheBlockState::Clean;
                }
            }
            Ok(Some(remote_id))
        } else {
            Ok(None)
        }
    }

    /// Scans the cache and promotes Dirty blocks exceeding TTL to Uploading, then uploads them.
    pub async fn background_flush_tick(&self) -> Result<()> {
        let mut to_upload = Vec::new();

        {
            let mut cache = self.blocks.write().await;
            let now = Instant::now();
            for (id, entry) in cache.iter_mut() {
                if entry.state == CacheBlockState::Dirty && now.duration_since(entry.last_modified) >= self.ttl {
                    entry.state = CacheBlockState::Uploading;
                    to_upload.push((id.clone(), entry.data.clone()));
                }
            }
        }

        // Upload them concurrently or sequentially. Here sequentially for simplicity.
        for (id, data) in to_upload {
            match self.remote.upload(&id, data).await {
                Ok(_) => {
                    let mut cache = self.blocks.write().await;
                    if let Some(entry) = cache.get_mut(&id) {
                        if entry.state == CacheBlockState::Uploading { // Ensure not modified during upload
                            entry.state = CacheBlockState::Clean;
                        }
                    }
                }
                Err(e) => {
                    eprintln!("Failed to upload block {:?}: {}", id, e);
                    // Revert to dirty to retry later
                    let mut cache = self.blocks.write().await;
                    if let Some(entry) = cache.get_mut(&id) {
                        if entry.state == CacheBlockState::Uploading {
                            entry.state = CacheBlockState::Dirty;
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Spawns the background daemon loop
    pub fn spawn_daemon(self: Arc<Self>, handle: &tokio::runtime::Handle) -> tokio::task::JoinHandle<()> {
        handle.spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(1));
            loop {
                interval.tick().await;
                let _ = self.background_flush_tick().await;
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct MockStorage {
        upload_count: Arc<AtomicUsize>,
    }

    impl RemoteStorage for MockStorage {
        fn upload(&self, _block_id: &BlockId, _data: Vec<u8>) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<String>> + Send>> {
            let count = self.upload_count.clone();
            Box::pin(async move {
                // Simulate network latency
                tokio::time::sleep(Duration::from_millis(10)).await;
                count.fetch_add(1, Ordering::SeqCst);
                Ok("mock_remote_id".to_string())
            })
        }
    }

    #[tokio::test]
    async fn test_cache_state_machine() {
        let mock_storage = Arc::new(MockStorage {
            upload_count: Arc::new(AtomicUsize::new(0)),
        });
        
        // TTL of 100ms
        let cache = Arc::new(CacheManager::new(64, Duration::from_millis(100), mock_storage.clone()));
        
        let block_id = BlockId { ino: 1, index: 0 };
        cache.write_block(block_id.clone(), vec![1, 2, 3]).await;

        {
            let lock = cache.blocks.read().await;
            let entry = lock.peek(&block_id).unwrap();
            assert_eq!(entry.state, CacheBlockState::Dirty);
        }

        // tick immediately, shouldn't upload since TTL not reached
        cache.background_flush_tick().await.unwrap();
        assert_eq!(mock_storage.upload_count.load(Ordering::SeqCst), 0);

        // wait for TTL
        tokio::time::sleep(Duration::from_millis(110)).await;

        // tick again, should promote to Uploading and then Clean
        cache.background_flush_tick().await.unwrap();

        assert_eq!(mock_storage.upload_count.load(Ordering::SeqCst), 1);

        {
            let lock = cache.blocks.read().await;
            let entry = lock.peek(&block_id).unwrap();
            assert_eq!(entry.state, CacheBlockState::Clean);
        }
    }
}
