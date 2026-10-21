use super::{StorageLocation, StorageObjectPath};

/// Stable logical identity for one object within a storage namespace.
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct StorageObjectIdentity {
    location: StorageLocation,
    path: StorageObjectPath,
}

impl StorageObjectIdentity {
    /// Builds an identity from a namespace and its root-relative object path.
    pub fn new(location: StorageLocation, path: StorageObjectPath) -> Self {
        Self { location, path }
    }

    /// Returns the storage namespace containing this object.
    pub fn location(&self) -> &StorageLocation {
        &self.location
    }

    /// Returns the path relative to the storage namespace.
    pub fn path(&self) -> &StorageObjectPath {
        &self.path
    }
}
