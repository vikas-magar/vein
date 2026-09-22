use std::sync::{Arc, RwLock};

use crate::{
    ArtifactId, ArtifactRef, DistributedError, MetadataCommand, MetadataStateMachine, Result,
};

pub trait MetadataStore: Send + Sync {
    fn read(&self) -> Result<crate::CatalogView>;
    fn apply(&self, command: MetadataCommand) -> Result<crate::CommitReceipt>;
}

pub trait ArtifactStore: Send + Sync {
    fn put(&self, id: &ArtifactId, bytes: &[u8]) -> Result<ArtifactRef>;
    fn get(&self, id: &ArtifactId) -> Result<Vec<u8>>;
}

#[derive(Clone, Default)]
pub struct InMemoryMetadataStore {
    state_machine: Arc<RwLock<MetadataStateMachine>>,
}

impl InMemoryMetadataStore {
    pub fn new() -> Self {
        Self::default()
    }
}

impl MetadataStore for InMemoryMetadataStore {
    fn read(&self) -> Result<crate::CatalogView> {
        self.state_machine
            .read()
            .map(|state| state.state().clone())
            .map_err(|_| DistributedError::StorePoisoned)
    }

    fn apply(&self, command: MetadataCommand) -> Result<crate::CommitReceipt> {
        self.state_machine
            .write()
            .map_err(|_| DistributedError::StorePoisoned)?
            .apply(command)
    }
}
