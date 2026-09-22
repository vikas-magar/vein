use crate::{CatalogRevision, TransactionId};
use thiserror::Error;

pub type Result<T> = std::result::Result<T, DistributedError>;

#[derive(Debug, Error)]
pub enum DistributedError {
    #[error("catalog revision conflict: expected {expected}, actual {actual}")]
    RevisionConflict {
        expected: CatalogRevision,
        actual: CatalogRevision,
    },

    #[error("transaction {transaction_id} was already committed with a different command")]
    TransactionConflict { transaction_id: TransactionId },

    #[error("artifact {0} is not available")]
    MissingArtifact(String),

    #[error("invalid distributed command: {0}")]
    InvalidCommand(String),

    #[error("distributed store lock was poisoned")]
    StorePoisoned,

    #[error("consensus proposal rejected because this node is not the leader")]
    NotLeader,

    #[error("consensus proposal failed: {0}")]
    Consensus(String),

    #[error("distributed storage I/O failed: {0}")]
    Io(#[from] std::io::Error),

    #[error("distributed snapshot serialization failed: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("unsupported metadata snapshot format: expected {expected}, actual {actual}")]
    UnsupportedSnapshotFormat { expected: u32, actual: u32 },

    #[error("invalid metadata snapshot: {0}")]
    InvalidSnapshot(String),
}
