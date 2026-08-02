//! Typed operation enum carried as the Archipelago consensus payload.

use crate::VfsError;

/// A typed VFS operation that consensus will replicate across the cluster.
///
/// The enum is intentionally exhaustive so that every replication-worthy
/// mutation is modelled here; adding new variants is a backwards-compatible
/// on-the-wire change as long as nodes are updated before they receive the
/// new variant.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum ConsensusOp {
    /// Append (or create) an extent at `offset` of `length` bytes within the
    /// file at `path`.
    AppendExtent { path: String, offset: u64, length: u64 },
    /// Delete all extents for the file at `path`.
    DeleteFile { path: String },
}

impl ConsensusOp {
    /// Serialise the operation to a byte vector using bincode.
    ///
    /// # Errors
    /// Returns [`VfsError::Serialization`] if bincode encoding fails.
    pub fn serialize(&self) -> Result<Vec<u8>, VfsError> {
        bincode::serialize(self).map_err(VfsError::Serialization)
    }

    /// Deserialise a [`ConsensusOp`] from a byte slice produced by
    /// [`ConsensusOp::serialize`].
    ///
    /// # Errors
    /// Returns [`VfsError::Serialization`] if bincode decoding fails.
    pub fn deserialize(bytes: &[u8]) -> Result<Self, VfsError> {
        bincode::deserialize(bytes).map_err(VfsError::Serialization)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_append_extent() {
        let op = ConsensusOp::AppendExtent {
            path: "/data/block0".to_string(),
            offset: 4096,
            length: 512,
        };
        let bytes = op.serialize().expect("serialize should not fail");
        let decoded = ConsensusOp::deserialize(&bytes).expect("deserialize should not fail");
        assert_eq!(op, decoded);
    }

    #[test]
    fn round_trip_delete_file() {
        let op = ConsensusOp::DeleteFile { path: "/data/old_file".to_string() };
        let bytes = op.serialize().expect("serialize should not fail");
        let decoded = ConsensusOp::deserialize(&bytes).expect("deserialize should not fail");
        assert_eq!(op, decoded);
    }

    #[test]
    fn deserialize_garbage_returns_serialization_error() {
        let result = ConsensusOp::deserialize(b"this is not valid bincode");
        assert!(
            matches!(result, Err(VfsError::Serialization(_))),
            "expected VfsError::Serialization, got: {:?}",
            result,
        );
    }
}
