use std::time::{SystemTime, UNIX_EPOCH};

use super::StorageObjectIdentity;

/// Optional provider facts that help detect object replacement or mutation.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct StorageObjectVersion {
    /// Provider ETag, when available.
    pub etag: Option<std::sync::Arc<str>>,
    /// Provider-specific immutable version identifier, when available.
    pub provider_version: Option<std::sync::Arc<str>>,
    /// Last-modified timestamp reported by the provider, when available.
    pub last_modified: Option<StorageTimestamp>,
}

/// Portable nanosecond timestamp used in storage snapshots and task metadata.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct StorageTimestamp(i128);

impl StorageTimestamp {
    /// Constructs a timestamp from exact, signed nanoseconds relative to the Unix epoch.
    pub const fn from_unix_nanos(nanos: i128) -> Self { Self(nanos) }

    /// Captures a provider timestamp without depending on platform-specific `SystemTime` encoding.
    pub fn from_system_time(value: SystemTime) -> Option<Self> {
        let nanos = match value.duration_since(UNIX_EPOCH) {
            Ok(duration) => i128::try_from(duration.as_nanos()).ok()?,
            Err(error) => -i128::try_from(error.duration().as_nanos()).ok()?,
        };
        Some(Self(nanos))
    }

    /// Nanoseconds relative to the Unix epoch.
    pub const fn unix_nanos(self) -> i128 {
        self.0
    }
}

/// File metadata required by execution before opening a format reader.
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct StorageFileMetadata {
    /// Stable logical identity of the object that was inspected.
    pub identity: StorageObjectIdentity,
    /// File length in bytes.
    pub size_bytes: u64,
    /// Optional provider facts describing the observed object version.
    pub version: StorageObjectVersion,
}
