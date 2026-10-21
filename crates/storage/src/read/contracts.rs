use std::ops::Range;
use std::sync::Arc;

use bytes::Bytes;
use error::{Result, VeinError};
use futures::future::BoxFuture;
use futures::stream::{self, BoxStream};

use super::{StorageFileMetadata, StorageLocation, StorageObjectPath};

/// Read-only access to objects under one storage root.
///
/// Range reads return owned byte handles. Implementations may choose buffering,
/// mapping, or provider-specific transfer strategies behind this contract.
pub trait ReadStorage: Send + Sync {
    /// Reads object metadata without transferring its contents.
    fn stat<'a>(&'a self, path: &'a StorageObjectPath) -> BoxFuture<'a, Result<StorageFileMetadata>>;

    /// Reads the half-open byte range from a root-relative object path.
    fn read_range<'a>(
        &'a self,
        path: &'a StorageObjectPath,
        range: Range<u64>,
    ) -> BoxFuture<'a, Result<Bytes>>;

    /// Reads a small complete object when it exists, returning `None` only for a missing object.
    ///
    /// The default uses the storage metadata and range APIs. Backends may override it when their
    /// native read operation can provide a stronger atomic snapshot for objects that can change.
    fn read_optional<'a>(
        &'a self,
        path: &'a StorageObjectPath,
    ) -> BoxFuture<'a, Result<Option<Bytes>>> {
        Box::pin(async move {
            let metadata = match self.stat(path).await {
                Ok(metadata) => metadata,
                Err(VeinError::NotFound(_)) => return Ok(None),
                Err(error) => return Err(error),
            };
            self.read_range(path, 0..metadata.size_bytes)
                .await
                .map(Some)
        })
    }

    /// Streams root-relative object paths recursively beneath `prefix`.
    ///
    /// Backends without listing support return one `Unsupported` error item.
    fn list<'a>(&'a self, _prefix: &'a StorageObjectPath) -> BoxStream<'a, Result<StorageObjectPath>> {
        Box::pin(stream::once(async {
            Err(VeinError::unsupported(
                "storage backend does not support object listing",
                None,
            ))
        }))
    }
}

/// Opens a backend handle rooted at a catalog-resolved storage location.
pub trait ReadStorageFactory: Send + Sync {
    /// Opens a shared read handle for `location`.
    fn open(&self, location: &StorageLocation) -> Result<Arc<dyn ReadStorage>>;
}
