use crate::block::BlockManager;
use crate::metadata::Extent;
use crate::{Result, VfsConfig};
use async_trait::async_trait;
use memmap2::MmapMut;
use opendal::Operator;
use std::collections::hash_map::DefaultHasher;
use std::fs::OpenOptions;
use std::hash::{Hash, Hasher};
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::{RwLock, mpsc};

/// Trait to abstract cloud block uploading logic for extensibility
#[async_trait]
pub trait CloudUploader: Send + Sync {
    async fn upload_block(&self, s3_path: &str, local_path: &std::path::Path) -> Result<()>;
}

/// Zero-copy Uploader that executes a single OpenDAL PUT request using Memory Mapping
pub struct ZeroCopySinglePutUploader {
    storage: Operator,
}

impl ZeroCopySinglePutUploader {
    pub fn new(storage: Operator) -> Self {
        Self { storage }
    }
}

#[async_trait]
impl CloudUploader for ZeroCopySinglePutUploader {
    async fn upload_block(&self, s3_path: &str, local_path: &std::path::Path) -> Result<()> {
        let file = std::fs::File::open(local_path).map_err(|e| {
            crate::VfsError::Internal(format!("Failed to open local block file for upload: {}", e))
        })?;

        // Zero-copy mmap
        let mmap = unsafe { memmap2::Mmap::map(&file) }.map_err(|e| {
            crate::VfsError::Internal(format!("Failed to mmap local block file: {}", e))
        })?;

        // Transmute is safe here because `bytes` does not outlive the `write().await` execution.
        let slice: &'static [u8] = unsafe { std::mem::transmute(&mmap[..]) };
        let bytes = bytes::Bytes::from_static(slice);

        // Single PUT request via OpenDAL
        self.storage.write(s3_path, bytes).await.map_err(|e| {
            crate::VfsError::Internal(format!("OpenDAL single-put write failed: {}", e))
        })?;

        // mmap is safely dropped here since .await has completed.
        Ok(())
    }
}

static BLOCK_COUNTER: AtomicU64 = AtomicU64::new(0);

pub struct SealedBlock {
    pub block_id: u64,
    pub path: PathBuf,
}

pub struct ActiveBlock {
    pub block_id: u64,
    pub mmap: MmapMut,
    pub ptr: *mut u8,
    pub current_offset: AtomicU64,
    pub block_size: u64,
}

unsafe impl Send for ActiveBlock {}
unsafe impl Sync for ActiveBlock {}

impl ActiveBlock {
    /// Appends data to the block. Returns `(offset, bytes_written)`.
    /// If the block hits the block_size ceiling mid-write, it only writes up to the boundary.
    pub fn try_append(&self, data: &[u8]) -> (u64, usize) {
        let mut current = self.current_offset.load(Ordering::Relaxed);
        loop {
            if current >= self.block_size {
                return (current, 0); // Block is completely full
            }

            // Calculate how much we can fit in this block without crossing the boundary
            let write_len = std::cmp::min(data.len() as u64, self.block_size - current);
            let new_offset = current + write_len;

            // Atomically reserve the space
            match self.current_offset.compare_exchange_weak(
                current,
                new_offset,
                Ordering::SeqCst,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    // Space reserved. Write directly to memory map.
                    unsafe {
                        std::ptr::copy_nonoverlapping(
                            data.as_ptr(),
                            self.ptr.add(current as usize),
                            write_len as usize,
                        );
                    }
                    return (current, write_len as usize);
                }
                Err(actual) => current = actual, // Another thread beat us, retry loop
            }
        }
    }
}

pub struct MmapBlockManager {
    config: VfsConfig,
    active_block: Arc<RwLock<ActiveBlock>>,
    storage: Operator,
    upload_tx: mpsc::Sender<SealedBlock>,
}

impl MmapBlockManager {
    /// P0-4 Fix: Guaranteed cluster-unique monotonic block ID
    fn generate_block_id(node_ip: &str) -> u64 {
        let mut hasher = DefaultHasher::new();
        node_ip.hash(&mut hasher);
        let node_hash = hasher.finish();

        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let seq = BLOCK_COUNTER.fetch_add(1, Ordering::SeqCst);

        ((ts & 0xFFFFFFFF) << 32) | ((node_hash & 0xFFFF) << 16) | (seq & 0xFFFF)
    }

    /// Helper to allocate a physical 5GB file on disk and memory map it
    fn create_active_block(config: &VfsConfig, block_id: u64) -> Result<ActiveBlock> {
        let file_path = config.local_cache_dir.join(format!("{}.bin", block_id));

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&file_path)?;

        // Pre-allocate the full block size (e.g., 5GB)
        file.set_len(config.block_size_bytes)?;

        // Memory map the file
        let mut mmap = unsafe { MmapMut::map_mut(&file)? };
        let ptr = mmap.as_mut_ptr();

        Ok(ActiveBlock {
            block_id,
            mmap,
            ptr,
            current_offset: AtomicU64::new(0),
            block_size: config.block_size_bytes,
        })
    }
}

#[async_trait]
impl BlockManager for MmapBlockManager {
    async fn init(config: &VfsConfig) -> Result<Self> {
        // Ensure cache directory exists
        tokio::fs::create_dir_all(&config.local_cache_dir).await?;

        // Generate the first active block ID
        let initial_block_id = Self::generate_block_id(&config.node_ip);

        let active_block = Self::create_active_block(config, initial_block_id)?;

        // Initialize OpenDAL dynamically using the provided configuration scheme
        let scheme = opendal::Scheme::from_str(&config.storage_backend)
            .map_err(|e| crate::VfsError::Internal(format!("Invalid storage backend: {e}")))?;

        let storage = Operator::via_map(scheme, config.storage_config.clone())
            .map_err(|e| crate::VfsError::Internal(format!("Failed to build storage: {e}")))?;

        // Setup background upload channel
        let (upload_tx, mut upload_rx) = mpsc::channel::<SealedBlock>(100);

        // P0-3 Fix: Boot Reconciliation. Scan local cache for orphaned blocks and re-queue them.
        let mut read_dir = tokio::fs::read_dir(&config.local_cache_dir).await?;
        while let Ok(Some(entry)) = read_dir.next_entry().await {
            let path = entry.path();
            if path.extension().and_then(|s| s.to_str()) == Some("bin")
                && let Some(stem) = path.file_stem().and_then(|s| s.to_str())
                    && let Ok(block_id) = stem.parse::<u64>() {
                        // Skip the initial block we just created
                        if block_id != initial_block_id {
                            tracing::info!("Re-queuing orphaned block {} for upload", block_id);
                            let sealed = SealedBlock {
                                block_id,
                                path: path.clone(),
                            };
                            let _ = upload_tx.send(sealed).await;
                        }
                    }
        }

        let uploader = Arc::new(ZeroCopySinglePutUploader::new(storage.clone()));

        // Spawn background tokio thread to handle S3 uploads
        tokio::spawn(async move {
            while let Some(sealed) = upload_rx.recv().await {
                let s3_path = format!("blocks/{}.bin", sealed.block_id);
                let mut retry_count = 0;
                loop {
                    match uploader.upload_block(&s3_path, &sealed.path).await {
                        Ok(_) => {
                            // CRITICAL: The file is now safely stored in the cloud.
                            // We MUST delete the local NVMe file to prevent disk exhaustion.
                            match tokio::fs::remove_file(&sealed.path).await {
                                Ok(_) => tracing::info!(
                                    "Successfully streamed block {} to backend in ONE request",
                                    sealed.block_id
                                ),
                                Err(e) => tracing::error!(
                                    "Failed to delete local cache file for block {}: {:?}",
                                    sealed.block_id,
                                    e
                                ),
                            }
                            break; // Success
                        }
                        Err(e) => {
                            retry_count += 1;
                            tracing::error!(
                                "Failed to upload block {} (attempt {}): {:?}. Retrying in 5s...",
                                sealed.block_id,
                                retry_count,
                                e
                            );
                            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                        }
                    }
                }
            }
        });

        Ok(Self {
            config: config.clone(),
            active_block: Arc::new(RwLock::new(active_block)),
            storage,
            upload_tx,
        })
    }

    async fn append(&self, mut data: &[u8]) -> Result<Vec<Extent>> {
        let mut extents = Vec::new();

        while !data.is_empty() {
            let active = self.active_block.read().await;
            let (offset, bytes_written) = active.try_append(data);

            if bytes_written > 0 {
                extents.push(Extent {
                    block_id: active.block_id,
                    offset,
                    length: bytes_written as u64,
                    node_ip: self.config.node_ip.clone(),
                });
                data = &data[bytes_written..];
            }
            drop(active);

            if !data.is_empty() {
                // Block full. Upgrade to write lock to seal and rotate.
                let mut active_write = self.active_block.write().await;

                // Double check idiom: did another thread rotate it while we waited for the write lock?
                let (_, late_written) = active_write.try_append(data);
                if late_written > 0 {
                    extents.push(Extent {
                        block_id: active_write.block_id,
                        offset: 0,
                        length: late_written as u64,
                        node_ip: self.config.node_ip.clone(),
                    });
                    data = &data[late_written..];
                    continue; // Skip sealing since it's already a new block
                }

                // Seal the old block and dispatch to background uploader
                let old_block_id = active_write.block_id;
                let sealed = SealedBlock {
                    block_id: old_block_id,
                    path: self
                        .config
                        .local_cache_dir
                        .join(format!("{}.bin", old_block_id)),
                };

                // Open new block
                let new_block_id = Self::generate_block_id(&self.config.node_ip);
                *active_write = Self::create_active_block(&self.config, new_block_id)?;

                // Send to background upload
                let _ = self.upload_tx.send(sealed).await;
            }
        }

        Ok(extents)
    }

    async fn read_extent(&self, extent: &Extent) -> Result<Vec<u8>> {
        // P1-7 Fix: Zero length panic prevention
        if extent.length == 0 {
            return Ok(Vec::new());
        }

        // P0-1 Fix: Local NVMe cache-hit path
        let local_path = self
            .config
            .local_cache_dir
            .join(format!("{}.bin", extent.block_id));
        if let Ok(mut file) = tokio::fs::File::open(&local_path).await {
            use tokio::io::{AsyncReadExt, AsyncSeekExt};
            let mut buf = vec![0u8; extent.length as usize];
            if file
                .seek(std::io::SeekFrom::Start(extent.offset))
                .await
                .is_ok()
                && file.read_exact(&mut buf).await.is_ok() {
                    return Ok(buf);
                }
        }

        // Cache Miss: Fetch from infinite cloud backend
        let s3_path = format!("blocks/{}.bin", extent.block_id);
        let end = extent.offset + extent.length - 1;

        let data = self
            .storage
            .read_with(&s3_path)
            .range(extent.offset..=end)
            .await
            .map_err(|e| crate::VfsError::Internal(e.to_string()))?;

        Ok(data.to_vec())
    }
}
