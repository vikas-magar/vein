use crate::{Result, VfsConfig};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

/// Represents a physical location of a chunk of data.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Extent {
    pub block_id: u64,
    pub offset: u64,
    pub length: u64,
    pub node_ip: String,
}

/// The core Metadata Engine trait, allowing alternative implementations for testing.
#[async_trait]
pub trait MetadataEngine: Send + Sync {
    /// Initializes the metadata engine with the given config.
    async fn init(config: &VfsConfig) -> Result<Self>
    where
        Self: Sized;

    /// Appends a new extent to a logical file path.
    async fn append_extent(&self, path: &str, extent: Extent) -> Result<()>;

    /// Gets all extents for a logical file path.
    async fn get_file(&self, path: &str) -> Result<Vec<Extent>>;

    /// Removes a logical file path entirely.
    async fn delete_file(&self, path: &str) -> Result<()>;
}

pub mod redb_engine;
pub use redb_engine::RedbMetadataEngine;
