//! Versioned catalog control-plane messages exchanged with VCS.
#![allow(missing_docs)]

use serde::{Deserialize, Serialize};

/// Version of the catalog RPC contract.
pub const CATALOG_RPC_VERSION: u16 = 1;

#[derive(
    Debug, Clone, Serialize, Deserialize, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize,
)]
#[rkyv(attr(allow(missing_docs)))]
struct VcsCatalogRequestEnvelope {
    version: u16,
    request: VcsCatalogRequest,
}

#[derive(
    Debug, Clone, Serialize, Deserialize, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize,
)]
#[rkyv(attr(allow(missing_docs)))]
struct VcsCatalogResponseEnvelope {
    version: u16,
    response: VcsCatalogResponse,
}

/// Encodes a versioned catalog request.
pub fn encode_request(request: &VcsCatalogRequest) -> Result<Vec<u8>, String> {
    rkyv::to_bytes::<rkyv::rancor::Error>(&VcsCatalogRequestEnvelope {
        version: CATALOG_RPC_VERSION,
        request: request.clone(),
    })
    .map(|bytes| bytes.to_vec())
    .map_err(|error| format!("encode VCS catalog request: {error}"))
}

/// Decodes and validates a versioned catalog request.
pub fn decode_request(payload: &[u8]) -> Result<VcsCatalogRequest, String> {
    let mut aligned = rkyv::util::AlignedVec::<16>::new();
    aligned.extend_from_slice(payload);
    let envelope = rkyv::from_bytes::<VcsCatalogRequestEnvelope, rkyv::rancor::Error>(&aligned)
        .map_err(|error| format!("decode VCS catalog request: {error}"))?;
    if envelope.version != CATALOG_RPC_VERSION {
        return Err(format!(
            "unsupported VCS catalog RPC version {}",
            envelope.version
        ));
    }
    Ok(envelope.request)
}

/// Encodes a versioned catalog response.
pub fn encode_response(response: &VcsCatalogResponse) -> Result<Vec<u8>, String> {
    rkyv::to_bytes::<rkyv::rancor::Error>(&VcsCatalogResponseEnvelope {
        version: CATALOG_RPC_VERSION,
        response: response.clone(),
    })
    .map(|bytes| bytes.to_vec())
    .map_err(|error| format!("encode VCS catalog response: {error}"))
}

/// Decodes and validates a versioned catalog response.
pub fn decode_response(payload: &[u8]) -> Result<VcsCatalogResponse, String> {
    let mut aligned = rkyv::util::AlignedVec::<16>::new();
    aligned.extend_from_slice(payload);
    let envelope = rkyv::from_bytes::<VcsCatalogResponseEnvelope, rkyv::rancor::Error>(&aligned)
        .map_err(|error| format!("decode VCS catalog response: {error}"))?;
    if envelope.version != CATALOG_RPC_VERSION {
        return Err(format!(
            "unsupported VCS catalog RPC version {}",
            envelope.version
        ));
    }
    Ok(envelope.response)
}

/// Encodes a serde metadata payload for the catalog wire contract.
pub fn encode_payload<T: Serialize>(value: &T, label: &str) -> Result<Vec<u8>, String> {
    serde_json::to_vec(value).map_err(|error| format!("encode {label}: {error}"))
}

/// Decodes a serde metadata payload for the catalog wire contract.
pub fn decode_payload<T: serde::de::DeserializeOwned>(
    payload: &[u8],
    label: &str,
) -> Result<T, String> {
    serde_json::from_slice(payload).map_err(|error| format!("decode {label}: {error}"))
}

/// A request sent to the authoritative VCS catalog service.
#[derive(
    Debug, Clone, Serialize, Deserialize, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize,
)]
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
    /// Validate that a planned table snapshot is still authoritative.
    ValidateSnapshot {
        /// Stable table and snapshot identity from the planning response.
        snapshot: common::protocol::CatalogSnapshotToken,
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
#[derive(
    Debug, Clone, Serialize, Deserialize, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize,
)]
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
        /// Authoritative snapshot identity for executor validation.
        snapshot: common::protocol::CatalogSnapshotToken,
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
    /// Snapshot validation succeeded.
    SnapshotValidated,
    /// Rejected request.
    Error {
        /// Human-readable error.
        message: String,
    },
}
