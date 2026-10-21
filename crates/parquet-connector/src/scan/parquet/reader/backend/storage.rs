use std::sync::Arc;

use bytes::Bytes;
use futures::future::BoxFuture;
use parquet::errors::{ParquetError, Result};
use ::storage::{ReadStorage, StorageObjectPath};

use super::ParquetRangeSource;

pub(super) struct StorageRangeSource {
    storage: Arc<dyn ReadStorage>,
    path: StorageObjectPath,
}

impl StorageRangeSource {
    pub(super) fn new(storage: Arc<dyn ReadStorage>, path: &str) -> Self {
        Self {
            storage,
            path: StorageObjectPath::new(path),
        }
    }
}

impl ParquetRangeSource for StorageRangeSource {
    fn read_range(&self, range: std::ops::Range<u64>) -> BoxFuture<'_, Result<Bytes>> {
        Box::pin(async move {
            self.storage
                .read_range(&self.path, range)
                .await
                .map_err(|error| ParquetError::General(error.to_string()))
        })
    }

    fn representation(&self) -> &'static str {
        "storage_range_buffer"
    }
}
