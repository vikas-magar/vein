//! Read-only OpenDAL service over registered files and an injected range reader.

mod backend;
mod config;
mod lister;
mod reader;
#[cfg(test)]
mod tests;

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::sync::Arc;

use opendal::{Builder, Metadata, MetadataBuilder, Result, raw::Service};
use serde::{Deserialize, Serialize};

use backend::FileBackend;
use config::FileStorageConfig;

/// Reads a bounded byte range directly into an engine-owned destination.
pub(crate) trait FileRangeReader: Debug + Send + Sync + 'static {
    fn read_into(&self, path: &str, offset: u64, output: &mut [u8]) -> Result<()>;
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct FileMetadata {
    pub(crate) size: u64,
    pub(crate) version: String,
}

impl FileMetadata {
    fn metadata(&self) -> Metadata {
        let mut metadata = MetadataBuilder::file(self.size);
        metadata.version(&self.version);
        metadata.build()
    }
}

pub(crate) type FileIndex = Arc<BTreeMap<String, FileMetadata>>;

/// Builder whose configuration contains metadata, never file contents.
pub(crate) struct FileStorage<R: FileRangeReader> {
    config: FileStorageConfig<R>,
}

impl<R: FileRangeReader> FileStorage<R> {
    pub(crate) fn new(namespace: String, files: FileIndex, reader: Arc<R>) -> Self {
        Self { config: FileStorageConfig { namespace, files, reader: Some(reader) } }
    }
}

impl<R: FileRangeReader> Default for FileStorage<R> {
    fn default() -> Self { Self { config: FileStorageConfig::default() } }
}

impl<R: FileRangeReader> Builder for FileStorage<R> {
    type Config = FileStorageConfig<R>;
    fn build(self) -> Result<impl Service> { FileBackend::new(self.config) }
}
