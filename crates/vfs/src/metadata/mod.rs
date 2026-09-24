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

/// A replacement for one logical metadata file in an atomic metadata commit.
#[derive(Debug, Clone)]
pub struct MetadataFileUpdate {
    pub path: String,
    pub extents: Vec<Extent>,
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

    /// Replaces all extents for `path` with a fresh set.
    ///
    /// This is the correct operation for manifest files (`_partitions.json`,
    /// `_schema.json`) that are overwritten on every update. The default
    /// implementation deletes the existing extents then appends the new ones.
    async fn replace_file(&self, path: &str, extents: Vec<Extent>) -> Result<()> {
        self.replace_files_atomically(vec![MetadataFileUpdate {
            path: path.to_string(),
            extents,
        }])
        .await
    }

    /// Atomically replaces multiple logical metadata files.
    async fn replace_files_atomically(&self, updates: Vec<MetadataFileUpdate>) -> Result<()> {
        for update in updates {
            self.replace_file_sequentially(&update.path, update.extents).await?;
        }
        Ok(())
    }

    /// Fallback for metadata backends without transactional batch support.
    async fn replace_file_sequentially(&self, path: &str, extents: Vec<Extent>) -> Result<()> {
        let _ = self.delete_file(path).await;
        for extent in extents {
            self.append_extent(path, extent).await?;
        }
        Ok(())
    }

    /// Persists a consensus proposal to storage.
    async fn persist_proposal(
        &self,
        pid_bytes: [u8; 16],
        status: u8,
        op_bytes: &[u8],
    ) -> Result<()>;

    /// Loads all consensus proposals from storage.
    async fn load_consensus_log(&self) -> Result<Vec<([u8; 16], u8, Vec<u8>)>>;

    /// Loads the persisted sequence counter.
    async fn load_sequence(&self) -> Result<u64>;

    /// Persists the sequence counter to storage.
    async fn persist_sequence(&self, seq: u64) -> Result<()>;
}

pub mod redb_engine;
pub use redb_engine::RedbMetadataEngine;
