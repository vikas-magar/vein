//! Distributed metadata and artifact coordination primitives for Vein.

mod command;
mod consensus;
mod error;
mod identity;
mod network;
mod persistent;
mod raft;
mod raft_storage;
mod revision;
mod rpc_service;
mod snapshot;
mod state_machine;
mod store;
mod write;

pub use command::MetadataCommand;
pub use consensus::{
    ConsensusDriver, LocalConsensusDriver, OpenRaftMetadataWriter, ReplicatedMetadataWriter,
};
pub use error::{DistributedError, Result};
pub use identity::{ArtifactId, SnapshotId, TableId, TransactionId, WriteId};
pub use network::{QuicRaftNetwork, QuicRaftNetworkFactory, RaftRpcIdGenerator};
pub use persistent::PersistentMetadataStore;
pub use raft::{VeinRaft, VeinRaftConfig, start_raft_node};
pub use raft_storage::DurableRaftStorage;
pub use revision::CatalogRevision;
pub use rpc_service::{OpenRaftRpcHandler, RaftRpcHandler};
pub use snapshot::{
    exchange_key_id, ArtifactRef, CatalogView, CommitReceipt, TableRecord, TableSnapshot,
};
pub use state_machine::MetadataStateMachine;
pub use store::{ArtifactStore, InMemoryMetadataStore, MetadataStore};
pub use write::{TableWriteIntent, TableWriteMode, TableWritePart, TableWriteState};
