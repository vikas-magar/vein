use std::sync::Arc;

use error::{ErrorCategory, Result, VeinError, VeinResultExt};
use opendal::{Operator, services::Fs};

use crate::{ReadStorage, ReadStorageFactory, StorageLocation};
use super::OpenDalReadStorage;

/// Factory for root-scoped OpenDAL filesystem readers.
#[derive(Debug, Clone, Copy, Default)]
pub struct OpenDalReadStorageFactory;

impl ReadStorageFactory for OpenDalReadStorageFactory {
    fn open(&self, location: &StorageLocation) -> Result<Arc<dyn ReadStorage>> {
        let root = location.as_local_path().ok_or_else(|| VeinError::unsupported(
            "OpenDAL filesystem storage requires a local filesystem location", None,
        ))?;
        let root = std::fs::canonicalize(root)
            .map_err(|error| VeinError::io("resolving filesystem storage root", error))?;
        let root_text = root.to_str().ok_or_else(|| VeinError::unsupported(
            "OpenDAL filesystem storage requires a UTF-8 root path", None,
        ))?;
        let operator = Operator::new(Fs::default().root(root_text))
            .map_external_err(ErrorCategory::Storage, "opening filesystem storage root")?;
        Ok(Arc::new(OpenDalReadStorage::new(StorageLocation::local(root), operator)))
    }
}
