use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use async_trait::async_trait;

use crate::{
    CatalogView, CommitReceipt, DistributedError, DurableRaftStorage, MetadataCommand,
    MetadataStore, Result, VeinRaft,
};

pub trait ConsensusDriver: Send + Sync {
    fn read(&self) -> Result<CatalogView>;
    fn propose(&self, command: MetadataCommand) -> Result<CommitReceipt>;
    fn is_leader(&self) -> bool;
}

/// Async consensus boundary used by catalog services that submit commands to Raft.
#[async_trait]
pub trait ReplicatedMetadataWriter: Send + Sync {
    /// Commits one metadata command through the current Raft leader.
    async fn propose(&self, command: MetadataCommand) -> Result<CommitReceipt>;

    /// Reads the committed metadata view from this node's durable state machine.
    async fn read_view(&self) -> Result<CatalogView>;
}

/// OpenRaft-backed metadata writer and committed-view reader.
pub struct OpenRaftMetadataWriter {
    raft: Arc<VeinRaft>,
    storage: DurableRaftStorage,
}

impl OpenRaftMetadataWriter {
    /// Creates a writer backed by one running OpenRaft node.
    pub fn new(raft: Arc<VeinRaft>, storage: DurableRaftStorage) -> Self {
        Self { raft, storage }
    }
}

#[async_trait]
impl ReplicatedMetadataWriter for OpenRaftMetadataWriter {
    async fn propose(&self, command: MetadataCommand) -> Result<CommitReceipt> {
        self.raft
            .client_write(command)
            .await
            .map(|response| response.data)
            .map_err(|error| DistributedError::Consensus(error.to_string()))
    }

    async fn read_view(&self) -> Result<CatalogView> {
        self.storage.read_catalog()
    }
}

#[derive(Clone)]
pub struct LocalConsensusDriver {
    store: Arc<dyn MetadataStore>,
    leader: Arc<AtomicBool>,
}

impl LocalConsensusDriver {
    pub fn new(store: Arc<dyn MetadataStore>) -> Self {
        Self {
            store,
            leader: Arc::new(AtomicBool::new(true)),
        }
    }

    pub fn set_leader(&self, is_leader: bool) {
        self.leader.store(is_leader, Ordering::Release);
    }
}

impl ConsensusDriver for LocalConsensusDriver {
    fn read(&self) -> Result<CatalogView> {
        self.store.read()
    }

    fn propose(&self, command: MetadataCommand) -> Result<CommitReceipt> {
        if !self.is_leader() {
            return Err(DistributedError::NotLeader);
        }

        self.store.apply(command)
    }

    fn is_leader(&self) -> bool {
        self.leader.load(Ordering::Acquire)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        ArtifactId, ArtifactRef, CatalogRevision, InMemoryMetadataStore, SnapshotId, TableId,
        TableSnapshot, TransactionId,
    };

    fn command(transaction: &str, snapshot: &str) -> MetadataCommand {
        MetadataCommand::PublishTableSnapshot {
            transaction_id: TransactionId::from(transaction),
            table_id: TableId::from("orders"),
            expected_revision: CatalogRevision::INITIAL,
            snapshot: TableSnapshot {
                id: SnapshotId::from(snapshot),
                manifest: ArtifactRef {
                    id: ArtifactId::from(format!("manifest-{snapshot}")),
                    uri: format!("vfs://manifests/{snapshot}"),
                    checksum: format!("checksum-{snapshot}"),
                    size_bytes: 128,
                },
                schema_fingerprint: "schema-v1".to_owned(),
                metadata_payload: Vec::new(),
            },
        }
    }

    #[test]
    fn proposes_through_metadata_store() {
        let store = Arc::new(InMemoryMetadataStore::new());
        let driver = LocalConsensusDriver::new(store);

        let receipt = driver.propose(command("txn-1", "snapshot-1")).unwrap();

        assert_eq!(receipt.revision, CatalogRevision::new(1));
        assert_eq!(driver.read().unwrap().revision, CatalogRevision::new(1));
    }

    #[test]
    fn rejects_proposals_when_not_leader() {
        let store = Arc::new(InMemoryMetadataStore::new());
        let driver = LocalConsensusDriver::new(store);
        driver.set_leader(false);

        let error = driver.propose(command("txn-1", "snapshot-1")).unwrap_err();

        assert!(matches!(error, DistributedError::NotLeader));
        assert_eq!(driver.read().unwrap().revision, CatalogRevision::INITIAL);
    }

    #[test]
    fn leader_flag_is_shared_by_clones() {
        let store = Arc::new(InMemoryMetadataStore::new());
        let driver = LocalConsensusDriver::new(store);
        let follower = driver.clone();

        driver.set_leader(false);

        assert!(!follower.is_leader());
    }
}
