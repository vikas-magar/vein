use crate::metadata::Extent;
use crate::{Result, VfsConfig};
use async_trait::async_trait;

/// The Block Manager handles physical storage across NVMe memory maps and object storage (S3).
#[async_trait]
pub trait BlockManager: Send + Sync {
    /// Initializes the Block Manager and its background upload tasks.
    async fn init(config: &VfsConfig) -> Result<Self>
    where
        Self: Sized;

    /// Appends data to the virtual memory array. 
    /// Returns a list of Extents. If the data crossed a block boundary, 
    /// multiple extents will be returned seamlessly.
    async fn append(&self, data: &[u8]) -> Result<Vec<Extent>>;

    /// Reads exactly the bytes requested in the extent.
    /// Uses NVMe Cache if present, otherwise pulls specific byte-range from S3.
    async fn read_extent(&self, extent: &Extent) -> Result<Vec<u8>>;
}

pub mod mmap_manager;
pub use mmap_manager::MmapBlockManager;
