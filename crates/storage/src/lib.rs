#![deny(missing_docs)]

//! Generic storage contracts and reusable backend adapters.

/// Storage read interfaces used by execution and format readers.
pub mod read;

/// In-memory implementation of the generic storage contracts.
///
/// This backend is useful for browser runtimes and embedded applications that
/// provide objects directly as byte buffers.
pub mod memory;

/// OpenDAL-backed implementations of the storage contracts.
#[cfg(feature = "opendal")]
pub mod opendal;

pub use read::{
    ReadStorage, ReadStorageFactory, StorageFileMetadata, StorageLocation,
    StorageObjectIdentity, StorageObjectPath, StorageObjectVersion, StorageTimestamp,
};
