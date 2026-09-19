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

    /// Reads a byte range inside an extent. The default implementation preserves
    /// compatibility for block managers that only support whole-extent reads.
    async fn read_extent_range(
        &self,
        extent: &Extent,
        offset: u64,
        length: u64,
    ) -> Result<Vec<u8>> {
        if offset.checked_add(length).is_none() || offset + length > extent.length {
            return Err(crate::VfsError::Internal(format!(
                "Extent range out of bounds: offset={offset}, length={length}, extent_length={}",
                extent.length
            )));
        }
        let bytes = self.read_extent(extent).await?;
        Ok(bytes[offset as usize..(offset + length) as usize].to_vec())
    }
}

pub mod key;
pub mod mmap_manager;

pub use key::{BlockKey, PlaneType, SegmentKey};
pub use mmap_manager::MmapBlockManager;
