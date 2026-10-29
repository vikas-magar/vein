//! Immutable, zero-copy in-memory object storage.

#[cfg(test)]
mod tests;

use std::collections::HashMap;
use std::ops::Range;
use std::sync::Arc;

use bytes::Bytes;
use error::{Result, VeinError};
use futures::future::BoxFuture;
use futures::stream::{self, BoxStream};

use crate::{
    ReadStorage, ReadStorageFactory, StorageFileMetadata, StorageLocation,
    StorageObjectIdentity, StorageObjectPath, StorageObjectVersion,
};

/// Read-storage factory backed by immutable, caller-provided byte buffers.
#[derive(Clone)]
pub struct MemoryReadStorageFactory {
    location: StorageLocation,
    objects: Arc<HashMap<StorageObjectPath, Bytes>>,
    version: StorageObjectVersion,
}

impl MemoryReadStorageFactory {
    /// Creates a provider with default, unknown object-version metadata.
    pub fn new(
        location: StorageLocation,
        objects: impl IntoIterator<Item=(StorageObjectPath, Bytes)>,
    ) -> Result<Self> {
        Self::with_version(location, objects, StorageObjectVersion::default())
    }

    /// Creates a provider whose objects share the supplied snapshot version.
    pub fn with_version(
        location: StorageLocation,
        objects: impl IntoIterator<Item=(StorageObjectPath, Bytes)>,
        version: StorageObjectVersion,
    ) -> Result<Self> {
        let mut indexed = HashMap::new();
        for (path, bytes) in objects {
            validate_object_path(&path)?;
            if indexed.insert(path.clone(), bytes).is_some() {
                return Err(VeinError::invalid_input(format!(
                    "duplicate in-memory storage object {}", path.as_str()
                )));
            }
        }
        Ok(Self { location, objects: Arc::new(indexed), version })
    }
}

impl ReadStorageFactory for MemoryReadStorageFactory {
    fn open(&self, location: &StorageLocation) -> Result<Arc<dyn ReadStorage>> {
        if location != &self.location {
            return Err(VeinError::invalid_input(format!(
                "in-memory storage is rooted at {:?}, not {:?}", self.location, location
            )));
        }
        Ok(Arc::new(MemoryReadStorage {
            location: self.location.clone(),
            objects: Arc::clone(&self.objects),
            version: self.version.clone(),
        }))
    }
}

struct MemoryReadStorage {
    location: StorageLocation,
    objects: Arc<HashMap<StorageObjectPath, Bytes>>,
    version: StorageObjectVersion,
}

impl ReadStorage for MemoryReadStorage {
    fn stat<'a>(&'a self, path: &'a StorageObjectPath) -> BoxFuture<'a, Result<StorageFileMetadata>> {
        Box::pin(async move {
            let bytes = self.object(path)?;
            Ok(StorageFileMetadata {
                identity: StorageObjectIdentity::new(self.location.clone(), path.clone()),
                size_bytes: bytes.len() as u64,
                version: self.version.clone(),
            })
        })
    }

    fn read_range<'a>(
        &'a self,
        path: &'a StorageObjectPath,
        range: Range<u64>,
    ) -> BoxFuture<'a, Result<Bytes>> {
        Box::pin(async move {
            let bytes = self.object(path)?;
            let start = usize::try_from(range.start)
                .map_err(|_| invalid_range(path, &range, bytes.len()))?;
            let end = usize::try_from(range.end)
                .map_err(|_| invalid_range(path, &range, bytes.len()))?;
            if start > end || end > bytes.len() {
                return Err(invalid_range(path, &range, bytes.len()));
            }
            Ok(bytes.slice(start..end))
        })
    }

    fn list<'a>(&'a self, prefix: &'a StorageObjectPath) -> BoxStream<'a, Result<StorageObjectPath>> {
        let mut paths = self.objects.keys()
            .filter(|path| is_within_prefix(path, prefix))
            .cloned()
            .collect::<Vec<_>>();
        paths.sort_by(|left, right| left.as_str().cmp(right.as_str()));
        Box::pin(stream::iter(paths.into_iter().map(Ok)))
    }
}

impl MemoryReadStorage {
    fn object(&self, path: &StorageObjectPath) -> Result<&Bytes> {
        self.objects.get(path).ok_or_else(|| {
            VeinError::not_found(format!("in-memory storage object {}", path.as_str()))
        })
    }
}

fn validate_object_path(path: &StorageObjectPath) -> Result<()> {
    let value = path.as_str();
    if value.is_empty() || value.starts_with('/') || value.split('/').any(|part| part == "..") {
        return Err(VeinError::invalid_input(format!(
            "invalid root-relative in-memory object path {value:?}"
        )));
    }
    Ok(())
}

fn is_within_prefix(path: &StorageObjectPath, prefix: &StorageObjectPath) -> bool {
    let prefix = prefix.as_str().trim_end_matches('/');
    let path = path.as_str();
    prefix.is_empty() || path == prefix
        || path.strip_prefix(prefix).is_some_and(|tail| tail.starts_with('/'))
}

fn invalid_range(path: &StorageObjectPath, range: &Range<u64>, size: usize) -> VeinError {
    VeinError::invalid_input(format!(
        "byte range {}..{} is outside in-memory object {} ({} bytes)",
        range.start, range.end, path.as_str(), size
    ))
}
