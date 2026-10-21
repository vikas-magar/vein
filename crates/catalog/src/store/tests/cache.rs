use std::sync::Arc;

use storage::{
    StorageFileMetadata, StorageLocation, StorageObjectIdentity, StorageObjectPath,
    StorageObjectVersion,
};

use super::super::{MetadataKey, MetadataKind, MetadataStore};

#[derive(Debug, PartialEq)]
struct RowCount(u64);

const FOOTER: MetadataKind = MetadataKind::new("test.footer");
const MANIFEST: MetadataKind = MetadataKind::new("test.manifest");

fn snapshot(path: &str, version: Option<&str>, size_bytes: u64) -> StorageFileMetadata {
    StorageFileMetadata {
        identity: StorageObjectIdentity::new(
            StorageLocation::uri("mem://bucket"),
            StorageObjectPath::new(path),
        ),
        size_bytes,
        version: StorageObjectVersion {
            etag: version.map(Arc::from),
            ..StorageObjectVersion::default()
        },
    }
}

fn key(path: &str, version: &str) -> MetadataKey {
    MetadataKey::from_storage(FOOTER, &snapshot(path, Some(version), 64))
        .expect("versioned storage snapshot is cacheable")
}

#[test]
fn does_not_cache_without_a_provider_version_validator() {
    assert!(MetadataKey::from_storage(FOOTER, &snapshot("part.parquet", None, 64)).is_none());
}

#[test]
fn version_and_size_invalidate_replaced_object_metadata() {
    let store = MetadataStore::new();
    let old = key("part.parquet", "v1");
    store.insert(old.clone(), Arc::new(RowCount(1)));

    let new_version = MetadataKey::from_storage(
        FOOTER,
        &snapshot("part.parquet", Some("v2"), 64),
    )
    .unwrap();
    assert!(store.get::<RowCount>(&new_version).is_none());
    store.insert(new_version.clone(), Arc::new(RowCount(2)));

    assert_eq!(store.len(), 1);
    assert!(store.get::<RowCount>(&old).is_none());
    assert_eq!(*store.get::<RowCount>(&new_version).unwrap(), RowCount(2));

    let changed_size = MetadataKey::from_storage(
        FOOTER,
        &snapshot("part.parquet", Some("v2"), 65),
    )
    .unwrap();
    assert!(store.get::<RowCount>(&changed_size).is_none());
}

#[test]
fn metadata_kind_and_payload_type_are_isolated() {
    let store = MetadataStore::new();
    let key = key("part.parquet", "v1");
    let manifest = MetadataKey::from_storage(MANIFEST, &snapshot("part.parquet", Some("v1"), 64))
        .unwrap();
    store.insert(key.clone(), Arc::new(RowCount(7)));
    store.insert(manifest.clone(), Arc::new(String::from("manifest")));

    assert_eq!(*store.get::<RowCount>(&key).unwrap(), RowCount(7));
    assert!(store.get::<RowCount>(&manifest).is_none());
    assert_eq!(&*store.get::<String>(&manifest).unwrap(), "manifest");
}

#[test]
fn replacement_does_not_leave_a_stale_fifo_entry() {
    let store = MetadataStore::with_capacity(2);
    let first = key("first.parquet", "v1");
    let second = key("second.parquet", "v1");
    let replacement = MetadataKey::from_storage(
        FOOTER,
        &snapshot("first.parquet", Some("v2"), 64),
    )
    .unwrap();
    store.insert(first, Arc::new(RowCount(1)));
    store.insert(second.clone(), Arc::new(RowCount(2)));
    store.insert(replacement.clone(), Arc::new(RowCount(3)));
    store.insert(key("third.parquet", "v1"), Arc::new(RowCount(4)));

    assert_eq!(store.len(), 2);
    assert!(store.get::<RowCount>(&replacement).is_some());
    assert!(store.get::<RowCount>(&second).is_none(), "second is the true oldest entry");
}
