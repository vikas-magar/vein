//! Browser-safe adapters for engine capabilities unavailable in a page sandbox.

use std::sync::Arc;

use common::protocol::StorageTarget;
use common::{Result, VeinError};
use exec::operators::sink::{WriteStorage, WriteTransaction};
use exec::spill::{SpillRunId, SpillStorage, SpillStorageFactory, SpillStream};

pub(super) struct ReadOnlyWriteStorage;

impl WriteStorage for ReadOnlyWriteStorage {
    fn validate_target(&self, _uri: &str, _options: &[(String, String)]) -> Result<()> {
        Err(read_only_error())
    }

    fn begin_write(&self, _target: &StorageTarget) -> Result<Box<dyn WriteTransaction>> {
        Err(read_only_error())
    }
}

fn read_only_error() -> VeinError {
    VeinError::unsupported("browser engine is read-only", None)
}

pub(super) struct BrowserSpillFactory;

impl SpillStorageFactory for BrowserSpillFactory {
    fn create_for_query(&self) -> Result<Arc<dyn SpillStorage>> {
        Ok(Arc::new(BrowserSpillStorage))
    }
}

struct BrowserSpillStorage;

impl SpillStorage for BrowserSpillStorage {
    fn create(&self) -> Result<(SpillRunId, Box<dyn SpillStream>)> {
        Err(spill_unavailable())
    }

    fn open(&self, _id: &SpillRunId) -> Result<Box<dyn SpillStream>> {
        Err(spill_unavailable())
    }

    fn remove(&self, _id: &SpillRunId) -> Result<()> {
        Ok(())
    }
}

fn spill_unavailable() -> VeinError {
    VeinError::unsupported(
        "browser query spilling requires a persistent browser storage adapter",
        None,
    )
}
