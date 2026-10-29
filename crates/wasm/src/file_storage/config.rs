use std::sync::Arc;

use opendal::Configurator;
use serde::{Deserialize, Serialize};

use super::{FileIndex, FileRangeReader, FileStorage};

#[derive(Debug, Serialize, Deserialize)]
#[serde(bound = "")]
pub(crate) struct FileStorageConfig<R: FileRangeReader> {
    pub(super) namespace: String,
    pub(super) files: FileIndex,
    // A live I/O provider is explicitly injected, never serialized as data.
    #[serde(skip)]
    pub(super) reader: Option<Arc<R>>,
}

impl<R: FileRangeReader> Default for FileStorageConfig<R> {
    fn default() -> Self {
        Self { namespace: String::new(), files: Arc::default(), reader: None }
    }
}

impl<R: FileRangeReader> Configurator for FileStorageConfig<R> {
    type Builder = FileStorage<R>;
    fn into_builder(self) -> Self::Builder { FileStorage { config: self } }
}
