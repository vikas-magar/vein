//! Versioned catalog control-plane messages exchanged with VCS.
#![allow(missing_docs)]

use serde::{Deserialize, Serialize};

/// Version of the catalog RPC contract.
pub const CATALOG_RPC_VERSION: u16 = 1;

/// A request sent to the authoritative VCS catalog service.
#[derive(Debug, Clone, Serialize, Deserialize, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
#[rkyv(attr(allow(missing_docs)))]
pub enum VcsCatalogRequest {
    /// Start a revision-pinned planning session.
    BeginPlanning,
    /// Resolve one table in a pinned planning session.
    ResolveTable {
        /// Revision pinned by the caller.
        revision: String,
        /// User-visible or canonical table name.
        name: String,
    },
    /// Register table metadata against a revision.
    RegisterTable {
        /// Expected catalog revision.
        expected_revision: String,
        /// JSON encoded planner metadata.
        metadata: Vec<u8>,
    },
    /// Prepare one table write generation.
    PrepareTableWrite {
        /// Stable write identifier.
        write_id: String,
        /// Stable table identifier.
        table_id: String,
        /// overwrite or append.
        mode: String,
        /// Number of immutable parts required for commit.
        expected_parts: u32,
    },
    /// Publish one immutable table write part.
    PublishTableWritePart {
        /// Stable write identifier.
        write_id: String,
        /// JSON encoded write part.
        part: Vec<u8>,
    },
    /// Commit one complete table write generation.
    CommitTableWrite {
        /// Stable write identifier.
        write_id: String,
        /// JSON encoded table snapshot.
        snapshot: Vec<u8>,
    },
    /// Abort one prepared table write generation.
    AbortTableWrite {
        /// Stable write identifier.
        write_id: String,
    },
}

/// A response returned by the authoritative VCS catalog service.
#[derive(Debug, Clone, Serialize, Deserialize, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
#[rkyv(attr(allow(missing_docs)))]
pub enum VcsCatalogResponse {
    /// Current catalog revision.
    Revision {
        /// Revision identifier.
        revision: String,
    },
    /// Resolved planner metadata.
    Table {
        /// Revision at which the table was resolved.
        revision: String,
        /// JSON encoded planner metadata.
        metadata: Vec<u8>,
    },
    /// Prepared table-write intent.
    WriteIntent {
        /// JSON encoded write intent.
        intent: Vec<u8>,
    },
    /// Commit receipt.
    Receipt {
        /// JSON encoded commit receipt.
        receipt: Vec<u8>,
    },
    /// Successful mutation without a payload.
    Ack,
    /// Rejected request.
    Error {
        /// Human-readable error.
        message: String,
    },
}

impl VcsCatalogResponse {
    /// Creates a version error response.
    pub fn version_error() -> Self {
        Self::Error {
            message: "unsupported VCS catalog RPC version 1".to_string(),
        }
    }
}
