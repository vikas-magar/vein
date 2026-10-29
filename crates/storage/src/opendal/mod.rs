//! One OpenDAL adapter shared by native and browser storage services.

mod dispatch;
mod listing;
mod metadata;

#[cfg(feature = "opendal-fs")]
mod filesystem;
#[cfg(feature = "opendal-fs")]
pub use filesystem::OpenDalReadStorageFactory;

#[cfg(test)]
mod tests;

use std::ops::Range;
use std::sync::Arc;

use bytes::Bytes;
use error::{ErrorCategory, ExternalError, Result, VeinError, VeinResultExt};
use futures::future::BoxFuture;
use opendal::Operator;

use crate::{ReadStorage, ReadStorageFactory, StorageFileMetadata, StorageLocation, StorageObjectPath};

/// A root-bound OpenDAL operator implementing Vein's storage and factory contracts.
///
/// Clones share the operator's service and never duplicate object payloads.
#[derive(Clone)]
pub struct OpenDalReadStorage {
    operator: Operator,
    location: StorageLocation,
}

impl OpenDalReadStorage {
    /// Wraps a configured operator for one explicitly identified storage namespace.
    pub fn new(location: StorageLocation, operator: Operator) -> Self {
        Self { operator, location }
    }
}

impl ReadStorageFactory for OpenDalReadStorage {
    fn open(&self, location: &StorageLocation) -> Result<Arc<dyn ReadStorage>> {
        if location != &self.location {
            return Err(VeinError::invalid_input("OpenDAL operator does not own this storage location"));
        }
        Ok(Arc::new(self.clone()))
    }
}

impl ReadStorage for OpenDalReadStorage {
    fn stat<'a>(&'a self, path: &'a StorageObjectPath) -> BoxFuture<'a, Result<StorageFileMetadata>> {
        let storage = self.clone();
        let path = path.clone();
        dispatch::future(async move { metadata::stat(&storage, &path).await })
    }

    fn read_range<'a>(&'a self, path: &'a StorageObjectPath, range: Range<u64>) -> BoxFuture<'a, Result<Bytes>> {
        let operator = self.operator.clone();
        let path = path.clone();
        dispatch::future(async move {
            operator.read_with(path.as_str()).range(range).await
                .map_external_err(ErrorCategory::Storage, format!("read storage range {}", path.as_str()))
                .map(|buffer| buffer.to_bytes())
        })
    }

    fn read_optional<'a>(&'a self, path: &'a StorageObjectPath) -> BoxFuture<'a, Result<Option<Bytes>>> {
        let operator = self.operator.clone();
        let path = path.clone();
        dispatch::future(async move {
            match operator.read(path.as_str()).await {
                Ok(buffer) => Ok(Some(buffer.to_bytes())),
                Err(error) if error.kind() == opendal::ErrorKind::NotFound => Ok(None),
                Err(error) => Err(storage_error(error, format!("read optional storage object {}", path.as_str()))),
            }
        })
    }

    fn list<'a>(&'a self, prefix: &'a StorageObjectPath) -> futures::stream::BoxStream<'a, Result<StorageObjectPath>> {
        dispatch::stream(listing::list(self, prefix))
    }
}

fn storage_error(error: opendal::Error, context: impl Into<String>) -> VeinError {
    ExternalError::new(ErrorCategory::Storage, context, error).into()
}
