//! RPC messages for the VCS-owned exchange catalog.

use serde::{Deserialize, Serialize};

use crate::identifiers::PartitionId;
use crate::protocol::{ExchangeFragment, ExchangeKey, ExchangeManifest};
use crate::identifiers::TaskId;

/// A request sent to the VCS exchange authority.
#[derive(
    Debug, Clone, Serialize, Deserialize, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize,
)]
pub enum VcsExchangeRequest {
    /// Create or reopen an exchange.
    Create {
        /// Exchange identity.
        key: ExchangeKey,
        /// Number of reducer partitions.
        partition_count: u16,
        /// Producer tasks that must publish before sealing.
        expected_tasks: Vec<TaskId>,
        /// Fingerprint of the exchanged schema.
        schema_fingerprint: String,
    },
    /// Publish one immutable producer fragment.
    Publish {
        /// Exchange identity.
        key: ExchangeKey,
        /// Fragment descriptor.
        fragment: ExchangeFragment,
    },
    /// Seal an exchange after all producers publish.
    Seal {
        /// Exchange identity.
        key: ExchangeKey,
    },
    /// Read a sealed exchange manifest.
    GetManifest {
        /// Exchange identity.
        key: ExchangeKey,
    },
    /// Read all fragments for one reducer partition.
    GetPartition {
        /// Exchange identity.
        key: ExchangeKey,
        /// Reducer partition to read.
        partition_id: PartitionId,
    },
    /// Delete exchange metadata after retention.
    Delete {
        /// Exchange identity.
        key: ExchangeKey,
    },
}

/// A response returned by the VCS exchange authority.
#[derive(
    Debug, Clone, Serialize, Deserialize, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize,
)]
pub enum VcsExchangeResponse {
    /// An operation completed without a payload.
    Ack,
    /// The current exchange manifest.
    Manifest {
        /// Exchange metadata.
        manifest: ExchangeManifest,
    },
    /// The fragments belonging to one reducer partition.
    Partition {
        /// Fragment descriptors.
        fragments: Vec<ExchangeFragment>,
    },
    /// The request was rejected.
    Error {
        /// Human-readable failure description.
        message: String,
    },
}
