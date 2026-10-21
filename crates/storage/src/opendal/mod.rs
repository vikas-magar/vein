//! OpenDAL adapters for the generic storage read contracts.

#[cfg(test)]
mod tests;

mod listing;
mod metadata;

use std::ops::Range;
use std::path::PathBuf;
use std::sync::Arc;

use bytes::Bytes;
use error::{ErrorCategory, ExternalError, Result, VeinError, VeinResultExt};
use futures::future::BoxFuture;
use opendal::Operator;
use opendal::services::Fs;

use crate::{
    ReadStorage, ReadStorageFactory, StorageFileMetadata, StorageLocation, StorageObjectPath,
};

/// Factory for root-scoped OpenDAL filesystem readers.
#[derive(Debug, Clone, Copy, Default)]
pub struct OpenDalReadStorageFactory;

impl ReadStorageFactory for OpenDalReadStorageFactory {
    fn open(&self, location: &StorageLocation) -> Result<Arc<dyn ReadStorage>> {
        let root = filesystem_root(location)?;
        let root_text = root.to_str().ok_or_else(|| {
            VeinError::unsupported(
                "OpenDAL filesystem storage requires a UTF-8 root path",
                None,
            )
        })?;
        let operator = Operator::new(Fs::default().root(root_text))
            .map_external_err(ErrorCategory::Storage, "opening filesystem storage root")?;
        Ok(Arc::new(OpenDalReadStorage {
            operator,
            location: StorageLocation::local(root),
        }))
    }
}

struct OpenDalReadStorage {
    operator: Operator,
    location: StorageLocation,
}

impl ReadStorage for OpenDalReadStorage {
    fn stat<'a>(&'a self, path: &'a StorageObjectPath) -> BoxFuture<'a, Result<StorageFileMetadata>> {
        Box::pin(async move { metadata::stat(self, path).await })
    }

    fn read_range<'a>(
        &'a self,
        path: &'a StorageObjectPath,
        range: Range<u64>,
    ) -> BoxFuture<'a, Result<Bytes>> {
        Box::pin(async move {
            self.operator
                .read_with(path.as_str())
                .range(range)
                .await
                .map_external_err(
                    ErrorCategory::Storage,
                    format!("read storage range {}", path.as_str()),
                )
                .map(|buffer| buffer.to_bytes())
        })
    }

    fn read_optional<'a>(
        &'a self,
        path: &'a StorageObjectPath,
    ) -> BoxFuture<'a, Result<Option<Bytes>>> {
        Box::pin(async move {
            match self.operator.read(path.as_str()).await {
                Ok(buffer) => Ok(Some(buffer.to_bytes())),
                Err(error) if error.kind() == opendal::ErrorKind::NotFound => Ok(None),
                Err(error) => Err(storage_error(
                    error,
                    format!("read optional storage object {}", path.as_str()),
                )),
            }
        })
    }

    fn list<'a>(
        &'a self,
        prefix: &'a StorageObjectPath,
    ) -> futures::stream::BoxStream<'a, Result<StorageObjectPath>> {
        Box::pin(listing::list(self, prefix))
    }
}

fn filesystem_root(location: &StorageLocation) -> Result<PathBuf> {
    let root = location.as_local_path().ok_or_else(|| {
        VeinError::unsupported(
            "OpenDAL filesystem storage requires a local filesystem location",
            None,
        )
    })?;
    std::fs::canonicalize(root)
        .map_err(|error| VeinError::io("resolving filesystem storage root", error))
}

fn storage_error(error: opendal::Error, context: impl Into<String>) -> VeinError {
    ExternalError::new(ErrorCategory::Storage, context, error).into()
}
