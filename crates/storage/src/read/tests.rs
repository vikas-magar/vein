use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use bytes::Bytes;

use super::{
    StorageFileMetadata, StorageLocation, StorageObjectIdentity, StorageObjectPath,
    StorageObjectVersion, StorageTimestamp,
};
use crate::{ReadStorageFactory, memory::MemoryReadStorageFactory};

#[test]
fn epoch_nanoseconds_preserve_sign_and_subsecond_precision() {
    for nanos in [-1_000_000_001, -1, 0, 1, 1_234_567_890_123_456_789] {
        assert_eq!(StorageTimestamp::from_unix_nanos(nanos).unix_nanos(), nanos);
    }
    let native = StorageTimestamp::from_system_time(UNIX_EPOCH - Duration::from_nanos(1)).unwrap();
    assert_eq!(native, StorageTimestamp::from_unix_nanos(-1));
}

#[test]
fn object_identity_includes_namespace_and_relative_path() {
    let local = StorageLocation::local("/warehouse/tpch");
    let path = StorageObjectPath::new("part/part-000.parquet");
    let identity = StorageObjectIdentity::new(local.clone(), path.clone());

    assert_eq!(identity.location(), &local);
    assert_eq!(identity.path().as_str(), "part/part-000.parquet");
    assert_ne!(
        identity,
        StorageObjectIdentity::new(StorageLocation::uri("s3://warehouse/tpch"), path)
    );
}

#[test]
fn object_versions_allow_missing_and_changing_provider_facts() {
    let unknown = StorageObjectVersion::default();
    let observed = StorageObjectVersion {
        etag: Some(Arc::from("etag-1")),
        provider_version: None,
        last_modified: StorageTimestamp::from_system_time(UNIX_EPOCH + Duration::from_secs(42)),
    };
    let changed = StorageObjectVersion {
        etag: Some(Arc::from("etag-2")),
        ..observed.clone()
    };

    assert_eq!(unknown.etag, None);
    assert_ne!(observed, changed);

    let path = StorageObjectPath::new("part/file.parquet");
    let objects = std::collections::HashMap::from([(path.clone(), Bytes::from_static(b"data"))]);
    let location = StorageLocation::uri("memory://warehouse");
    let first = MemoryReadStorageFactory::with_version(
        location.clone(), objects.clone(), observed.clone(),
    ).unwrap().open(&location).unwrap();
    let second = MemoryReadStorageFactory::with_version(
        location.clone(), objects, changed.clone(),
    ).unwrap().open(&location).unwrap();
    let first_metadata = futures::executor::block_on(first.stat(&path)).unwrap();
    let second_metadata = futures::executor::block_on(second.stat(&path)).unwrap();

    assert_eq!(first_metadata.identity, second_metadata.identity);
    assert_eq!(first_metadata.version, observed);
    assert_eq!(second_metadata.version, changed);
}

#[test]
fn file_metadata_carries_identity_without_assuming_version_availability() {
    let metadata = StorageFileMetadata {
        identity: StorageObjectIdentity::new(
            StorageLocation::local("/warehouse"),
            StorageObjectPath::new("table/file.parquet"),
        ),
        size_bytes: 128,
        version: StorageObjectVersion::default(),
    };

    assert_eq!(metadata.size_bytes, 128);
    assert!(metadata.version.provider_version.is_none());
}
