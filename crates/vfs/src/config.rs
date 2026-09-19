use std::collections::HashMap;
use std::path::PathBuf;

#[derive(Debug, Clone, Default)]
pub enum HashAlgo {
    SipHash24,
    #[default]
    Blake3,
}

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

    /// The algorithm used for hashing metadata keys
    pub metadata_hash_algo: HashAlgo,
}

impl VfsConfig {
    /// Builds a [`VfsConfig`] rooted at `data_dir`.
    ///
    /// All sub-paths are derived from the single root:
    /// ```text
    /// <data_dir>/
    ///   blocks/           ← mmap'd block files  (MmapBlockManager)
    ///   infinite_blocks/  ← OpenDAL fs root     (overflow / remote tier)
    ///   metadata.redb     ← redb extent catalog
    /// ```
    pub fn from_data_dir(data_dir: &std::path::Path) -> Self {
        let mut storage_config = HashMap::new();
        storage_config.insert(
            "root".to_string(),
            data_dir.join("infinite_blocks").to_string_lossy().into_owned(),
        );
        Self {
            local_cache_dir: data_dir.join("blocks"),
            metadata_db_path: data_dir.join("metadata.redb"),
            block_size_bytes: 5 * 1024 * 1024 * 1024,
            max_ingestion_buffer_bytes: 64 * 1024 * 1024,
            node_ip: "127.0.0.1".to_string(),
            storage_backend: "fs".to_string(),
            storage_config,
            metadata_hash_algo: HashAlgo::Blake3,
        }
    }

    /// Builds a [`VfsConfig`] from environment variables, falling back to
    /// `/tmp/veinfs` when none are set.
    ///
    /// | Variable              | Default            | Description                        |
    /// |-----------------------|--------------------|------------------------------------|
    /// | `VEIN_DATA_DIR`       | `/tmp/veinfs`      | Root for all VFS data              |
    /// | `VEIN_BLOCK_SIZE_MB`  | `5120` (5 GB)      | Block file size in mebibytes       |
    /// | `VEIN_NODE_IP`        | `127.0.0.1`        | Node IP recorded for data locality |
    ///
    /// All path sub-dirs follow the same layout as [`Self::from_data_dir`].
    pub fn from_env() -> Self {
        let data_dir = std::env::var("VEIN_DATA_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from("/tmp/veinfs"));

        let block_size_bytes = std::env::var("VEIN_BLOCK_SIZE_MB")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(5 * 1024)   // 5120 MiB = 5 GiB
            * 1024 * 1024;

        let node_ip = std::env::var("VEIN_NODE_IP")
            .unwrap_or_else(|_| "127.0.0.1".to_string());

        let mut cfg = Self::from_data_dir(&data_dir);
        cfg.block_size_bytes = block_size_bytes;
        cfg.node_ip = node_ip;
        cfg
    }
}

impl Default for VfsConfig {
    /// Equivalent to [`VfsConfig::from_env()`] — honours `VEIN_DATA_DIR` etc.
    /// Falls back to `/tmp/veinfs` when the variables are absent.
    fn default() -> Self {
        Self::from_env()
    }
}
