use std::collections::HashMap;
use std::path::PathBuf;

/// Configuration for the VFS node to prevent hardcoding.
#[derive(Debug, Clone)]
pub struct VfsConfig {
    /// Local directory where NVMe memory-mapped blocks are stored
    pub local_cache_dir: PathBuf,
    
    /// Path to the local redb metadata database
    pub metadata_db_path: PathBuf,
    
    /// The size of a single block in bytes. Configurable for testing.
    pub block_size_bytes: u64,
    
    /// Maximum size the in-memory ingestion buffer can grow before flushing
    pub max_ingestion_buffer_bytes: usize,
    
    /// IP address of this node (to record data locality)
    pub node_ip: String,

    /// The OpenDAL storage scheme (e.g., "s3", "gcs", "azblob", "fs")
    pub storage_backend: String,
    
    /// Generic key-value pairs for the OpenDAL builder
    pub storage_config: HashMap<String, String>,
}

impl Default for VfsConfig {
    fn default() -> Self {
        let mut storage_config = HashMap::new();
        // Default to a local FS mock for out-of-the-box local testing
        storage_config.insert("root".to_string(), "/tmp/veinfs/infinite_blocks".to_string());

        Self {
            local_cache_dir: PathBuf::from("/tmp/veinfs/blocks"),
            metadata_db_path: PathBuf::from("/tmp/veinfs/metadata.redb"),
            block_size_bytes: 5 * 1024 * 1024 * 1024, // 5 GB default
            max_ingestion_buffer_bytes: 64 * 1024 * 1024, // 64 MB default
            node_ip: "127.0.0.1".to_string(),
            storage_backend: "fs".to_string(),
            storage_config,
        }
    }
}
