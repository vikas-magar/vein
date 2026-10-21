#![deny(missing_docs)]

//! Generic storage contracts and reusable backend adapters.

/// Storage read interfaces used by execution and format readers.
pub mod read;

/// OpenDAL-backed implementations of the storage contracts.
pub mod opendal;

pub use read::{
    ReadStorage, ReadStorageFactory, StorageFileMetadata, StorageLocation,
    StorageObjectIdentity, StorageObjectPath, StorageObjectVersion, StorageTimestamp,
};
