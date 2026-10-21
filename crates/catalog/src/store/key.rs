use std::fmt;

use storage::StorageFileMetadata;

use super::MetadataKind;

/// Identity of one parsed object metadata value and its storage version.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct MetadataKey {
    kind: MetadataKind,
    object: storage::StorageObjectIdentity,
    size_bytes: u64,
    version: storage::StorageObjectVersion,
}

impl MetadataKey {
    /// Creates a cache key only when storage supplies a version validator.
    ///
    /// Size alone is insufficient: a same-size rewrite must not reuse a stale
    /// footer. Callers still use the snapshot's size for reading when this
    /// returns `None`, but bypass cache reads and writes.
    pub fn from_storage(kind: MetadataKind, metadata: &StorageFileMetadata) -> Option<Self> {
        let version = &metadata.version;
        let has_validator = version.etag.is_some()
            || version.provider_version.is_some()
            || version.last_modified.is_some();
        has_validator.then(|| Self {
            kind,
            object: metadata.identity.clone(),
            size_bytes: metadata.size_bytes,
            version: version.clone(),
        })
    }

    /// File length captured in the same storage snapshot used by this key.
    pub const fn size_bytes(&self) -> u64 {
        self.size_bytes
    }

    pub(super) const fn kind(&self) -> MetadataKind {
        self.kind
    }

    pub(super) fn same_object_and_kind(&self, other: &Self) -> bool {
        self.kind == other.kind && self.object == other.object
    }
}

impl fmt::Display for MetadataKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}:{}", self.kind, self.object.path().as_str())
    }
}
