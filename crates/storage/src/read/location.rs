use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Identifies a storage namespace independently of the objects in that namespace.
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum StorageLocation {
    /// A local filesystem root.
    Local(PathBuf),
    /// A provider-defined URI such as an object-store bucket or endpoint.
    Uri(Arc<str>),
}

impl StorageLocation {
    /// Creates a local filesystem location.
    pub fn local(root: impl Into<PathBuf>) -> Self {
        Self::Local(root.into())
    }

    /// Creates a provider-defined URI location.
    pub fn uri(uri: impl Into<Arc<str>>) -> Self {
        Self::Uri(uri.into())
    }

    /// Returns the local root when this location refers to a filesystem.
    pub fn as_local_path(&self) -> Option<&Path> {
        match self {
            Self::Local(path) => Some(path),
            Self::Uri(_) => None,
        }
    }
}

/// Identifies an object by its path relative to an opened storage root.
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct StorageObjectPath(Arc<str>);

impl StorageObjectPath {
    /// Creates a root-relative object path.
    pub fn new(path: impl Into<Arc<str>>) -> Self {
        Self(path.into())
    }

    /// Returns the provider-neutral path representation.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
