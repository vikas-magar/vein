use crate::block::BlockManager;
use crate::metadata::MetadataEngine;
use crate::{Result, VfsConfig};
use bytes::BytesMut;
use dashmap::DashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

pub struct IngestionBuffer {
    config: VfsConfig,
    buffers: DashMap<String, BytesMut>,
    file_locks: DashMap<String, Arc<tokio::sync::Mutex<()>>>,
    total_memory_usage: AtomicUsize,
    block_manager: Arc<dyn BlockManager>,
    metadata_engine: Arc<dyn MetadataEngine>,
}

impl IngestionBuffer {
    pub fn new(
        config: VfsConfig,
        block_manager: Arc<dyn BlockManager>,
        metadata_engine: Arc<dyn MetadataEngine>,
    ) -> Self {
        Self {
            config,
            buffers: DashMap::new(),
            file_locks: DashMap::new(),
            total_memory_usage: AtomicUsize::new(0),
            block_manager,
            metadata_engine,
        }
    }

    /// Appends a raw chunk of data to the logical file path buffer in RAM.
    /// If the buffer exceeds the merge threshold (e.g., 64MB), it automatically
    /// flushes to the block manager and records the physical extents in metadata.
    pub async fn ingest_chunk(&self, path: &str, chunk: &[u8]) -> Result<()> {
        // P0-15 Fix: Acquire an asynchronous lock for this specific logical file.
        // This guarantees that if two network streams push to the same file simultaneously, 
        // their flushes are strictly serialized, preventing extent permutation.
        let file_lock = self.file_locks.entry(path.to_string()).or_insert_with(|| Arc::new(tokio::sync::Mutex::new(()))).clone();
        let _guard = file_lock.lock().await;

        // 1. Get or create the memory buffer for this file
        let mut entry = self.buffers.entry(path.to_string()).or_insert_with(BytesMut::new);
        entry.extend_from_slice(chunk);
        
        self.total_memory_usage.fetch_add(chunk.len(), Ordering::Relaxed);

        // 2. Check if we hit the 64MB merge threshold
        if entry.len() >= self.config.max_ingestion_buffer_bytes {
            // Take the bytes safely
            let data_to_flush = entry.split().freeze();
            drop(entry); // Release the DashMap shard lock instantly
            
            // 3. Sequential 64MB write to the NVMe Memory Map
            match self.block_manager.append(&data_to_flush).await {
                Ok(extents) => {
                    self.total_memory_usage.fetch_sub(data_to_flush.len(), Ordering::Relaxed);
                    // 4. Update the Database with the new physical addresses
                    for extent in extents {
                        self.metadata_engine.append_extent(path, extent).await?;
                    }
                },
                Err(e) => {
                    // P0-16 Fix: Append failed (e.g. disk full). We MUST restore the bytes to the buffer.
                    // Re-acquire lock and prepend the failed data so we don't lose it.
                    let mut recovery_entry = self.buffers.entry(path.to_string()).or_insert_with(BytesMut::new);
                    let mut new_buf = BytesMut::with_capacity(data_to_flush.len() + recovery_entry.len());
                    new_buf.extend_from_slice(&data_to_flush);
                    new_buf.extend_from_slice(&recovery_entry);
                    *recovery_entry = new_buf;
                    
                    return Err(e);
                }
            }
        }
        
        // (OOM Protection): If `total_memory_usage` exceeds system thresholds (e.g. 80% RAM), 
        // we can trigger a background task here to forcefully flush the largest buffers.
        
        Ok(())
    }

    /// Called when the Executor sends an EOF signal for a specific file.
    /// Flushes any remaining bytes in the buffer to disk.
    pub async fn flush_file(&self, path: &str) -> Result<()> {
        // Acquire file lock for EOF flush
        let file_lock = self.file_locks.entry(path.to_string()).or_insert_with(|| Arc::new(tokio::sync::Mutex::new(()))).clone();
        let _guard = file_lock.lock().await;

        // P0-16 Fix: Only remove from DashMap if flush succeeds
        let data_to_flush = {
            if let Some(mut entry) = self.buffers.get_mut(path) {
                if !entry.is_empty() {
                    entry.split().freeze()
                } else {
                    return Ok(());
                }
            } else {
                return Ok(());
            }
        };

        match self.block_manager.append(&data_to_flush).await {
            Ok(extents) => {
                self.total_memory_usage.fetch_sub(data_to_flush.len(), Ordering::Relaxed);
                for extent in extents {
                    self.metadata_engine.append_extent(path, extent).await?;
                }
                self.buffers.remove(path);
                self.file_locks.remove(path);
                Ok(())
            },
            Err(e) => {
                // Restore bytes
                let mut recovery_entry = self.buffers.entry(path.to_string()).or_insert_with(BytesMut::new);
                let mut new_buf = BytesMut::with_capacity(data_to_flush.len() + recovery_entry.len());
                new_buf.extend_from_slice(&data_to_flush);
                new_buf.extend_from_slice(&recovery_entry);
                *recovery_entry = new_buf;
                Err(e)
            }
        }
    }
}
