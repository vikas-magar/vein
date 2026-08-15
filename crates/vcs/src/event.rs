use rkyv::rancor::Error as RkyvError;
use rkyv::{Archive, Deserialize, Serialize};

/// Events broadcast by VCS to all connected Master nodes.
/// Sent as QUIC datagrams (unreliable) — small, < 1200 bytes each.
///
/// rkyv is used for serialization: archived bytes can be read directly
/// from UMEM ring buffer memory with zero allocation on the receive path.
#[derive(Debug, Clone, Archive, Serialize, Deserialize)]
pub enum VeinEvent {
    /// A table's current snapshot has changed. Masters should invalidate
    /// any in-flight scan plans that reference the old snapshot_id.
    TableInvalidated {
        namespace: Vec<String>,
        table_name: String,
        new_snapshot_id: i64,
    },

    /// VCS has finished warming up. Masters can now send catalog queries.
    CatalogReady,

    /// Heartbeat from VCS. Masters use this to detect VCS restarts.
    Heartbeat { timestamp_ms: u64 },
}

impl VeinEvent {
    /// Serialize to bytes for QUIC datagram transmission.
    /// Returns an owned `AlignedVec` — the archived layout is written directly
    /// into a 16-byte-aligned buffer, ready to be passed to `send_datagram`.
    pub fn encode(&self) -> Result<rkyv::util::AlignedVec, RkyvError> {
        rkyv::to_bytes::<RkyvError>(self)
    }

    /// Access the archived event directly from raw bytes — zero allocation, zero copy.
    /// The bytes must come from a buffer aligned to 16 bytes (UMEM rings satisfy this).
    ///
    /// # Safety
    /// The caller guarantees `bytes` was produced by `encode()` or an equivalent
    /// rkyv serializer. Violating this is undefined behaviour.
    pub fn access(bytes: &[u8]) -> Result<&ArchivedVeinEvent, RkyvError> {
        rkyv::access::<ArchivedVeinEvent, RkyvError>(bytes)
    }

    /// Fully deserialize from archived bytes into an owned `VeinEvent`.
    /// Use this when you need an owned value (e.g. to store or mutate).
    /// Prefer `access()` when you only need to read fields once.
    pub fn decode(bytes: &[u8]) -> Result<Self, RkyvError> {
        let archived = rkyv::access::<ArchivedVeinEvent, RkyvError>(bytes)?;
        rkyv::deserialize::<VeinEvent, RkyvError>(archived)
    }
}
