//! Typed, object-safe contracts for reading storage objects.

mod contracts;
mod identity;
mod location;
mod metadata;

#[cfg(test)]
mod tests;

pub use contracts::{ReadStorage, ReadStorageFactory};
pub use identity::StorageObjectIdentity;
pub use location::{StorageLocation, StorageObjectPath};
pub use metadata::{StorageFileMetadata, StorageObjectVersion, StorageTimestamp};
