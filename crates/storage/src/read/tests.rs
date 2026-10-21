use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use bytes::Bytes;
use error::{Result, VeinError};
use futures::TryStreamExt;
use futures::future::BoxFuture;
use futures::stream::{self, BoxStream};

use super::{
    ReadStorage, StorageFileMetadata, StorageLocation, StorageObjectIdentity, StorageObjectPath,
    StorageObjectVersion, StorageTimestamp,
};

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
    let first = MemoryStorage {
        location: StorageLocation::uri("memory://warehouse"),
        objects: objects.clone(),
        version: observed.clone(),
    };
    let second = MemoryStorage {
        location: StorageLocation::uri("memory://warehouse"),
        objects,
        version: changed.clone(),
    };
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

#[tokio::test]
async fn backend_neutral_contract_supports_metadata_ranges_and_streaming_discovery() {
    let location = StorageLocation::uri("memory://warehouse");
    let path = StorageObjectPath::new("part/file.parquet");
    let source = Bytes::from_static(b"zero-copy-range");
    let mut objects = std::collections::HashMap::new();
    objects.insert(path.clone(), source.clone());
    let storage = MemoryStorage {
        location,
        objects,
        version: StorageObjectVersion::default(),
    };

    let metadata = storage.stat(&path).await.expect("memory object metadata");
    assert_eq!(metadata.identity.path(), &path);
    assert_eq!(metadata.size_bytes, source.len() as u64);
    assert_eq!(metadata.version, StorageObjectVersion::default());

    let range = storage.read_range(&path, 5..10).await.expect("memory range");
    assert_eq!(range.as_ref(), b"copy-");
    assert_eq!(range.as_ptr(), source.as_ptr().wrapping_add(5));

    let paths = storage
        .list(&StorageObjectPath::new("part/"))
        .try_collect::<Vec<_>>()
        .await
        .expect("memory listing");
    assert_eq!(paths, vec![path]);
}

struct MemoryStorage {
    location: StorageLocation,
    objects: std::collections::HashMap<StorageObjectPath, Bytes>,
    version: StorageObjectVersion,
}

impl ReadStorage for MemoryStorage {
    fn stat<'a>(&'a self, path: &'a StorageObjectPath) -> BoxFuture<'a, Result<StorageFileMetadata>> {
        Box::pin(async move {
            let bytes = self.objects.get(path).ok_or_else(|| {
                VeinError::not_found(format!("memory object {}", path.as_str()))
            })?;
            Ok(StorageFileMetadata {
                identity: StorageObjectIdentity::new(self.location.clone(), path.clone()),
                size_bytes: bytes.len() as u64,
                version: self.version.clone(),
            })
        })
    }

    fn read_range<'a>(
        &'a self,
        path: &'a StorageObjectPath,
        range: std::ops::Range<u64>,
    ) -> BoxFuture<'a, Result<Bytes>> {
        Box::pin(async move {
            let bytes = self.objects.get(path).ok_or_else(|| {
                VeinError::not_found(format!("memory object {}", path.as_str()))
            })?;
            Ok(bytes.slice(range.start as usize..range.end as usize))
        })
    }

    fn list<'a>(&'a self, prefix: &'a StorageObjectPath) -> BoxStream<'a, Result<StorageObjectPath>> {
        Box::pin(stream::iter(
            self.objects
                .keys()
                .filter(|path| path.as_str().starts_with(prefix.as_str()))
                .cloned()
                .map(Ok),
        ))
    }
}
