use bytes::Bytes;
use futures::TryStreamExt;

use crate::{
    ReadStorageFactory, StorageLocation, StorageObjectPath, memory::MemoryReadStorageFactory,
};

fn provider() -> MemoryReadStorageFactory {
    MemoryReadStorageFactory::new(
        StorageLocation::uri("memory://tpch"),
        [
            (
                StorageObjectPath::new("part/part.1.parquet"),
                Bytes::from_static(b"abcdefgh"),
            ),
            (
                StorageObjectPath::new("partsupp/partsupp.1.parquet"),
                Bytes::from_static(b"xyz"),
            ),
        ],
    )
    .expect("valid in-memory objects")
}

#[tokio::test]
async fn range_reads_share_the_original_byte_buffer() {
    let source = Bytes::from_static(b"abcdefgh");
    let factory = MemoryReadStorageFactory::new(
        StorageLocation::uri("memory://tpch"),
        [(StorageObjectPath::new("part/data.parquet"), source.clone())],
    )
    .unwrap();
    let storage = factory
        .open(&StorageLocation::uri("memory://tpch"))
        .unwrap();
    let result = storage
        .read_range(&StorageObjectPath::new("part/data.parquet"), 2..6)
        .await
        .unwrap();

    assert_eq!(result, Bytes::from_static(b"cdef"));
    assert_eq!(result.as_ptr(), source.as_ptr().wrapping_add(2));
}

#[tokio::test]
async fn metadata_and_listing_are_rooted_and_deterministic() {
    let factory = provider();
    let storage = factory
        .open(&StorageLocation::uri("memory://tpch"))
        .unwrap();
    let path = StorageObjectPath::new("part/part.1.parquet");
    let metadata = storage.stat(&path).await.unwrap();
    assert_eq!(metadata.identity.path(), &path);
    assert_eq!(metadata.size_bytes, 8);

    let paths = storage
        .list(&StorageObjectPath::new(""))
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(
        paths,
        vec![path, StorageObjectPath::new("partsupp/partsupp.1.parquet")]
    );
}

#[tokio::test]
async fn prefix_listing_observes_path_component_boundaries() {
    let storage = provider()
        .open(&StorageLocation::uri("memory://tpch"))
        .unwrap();
    let paths = storage
        .list(&StorageObjectPath::new("part"))
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(paths, vec![StorageObjectPath::new("part/part.1.parquet")]);
}

#[tokio::test]
async fn invalid_ranges_and_unknown_objects_return_typed_errors() {
    let storage = provider()
        .open(&StorageLocation::uri("memory://tpch"))
        .unwrap();
    let path = StorageObjectPath::new("part/part.1.parquet");
    assert!(storage.read_range(&path, 7..9).await.is_err());
    assert!(
        storage
            .stat(&StorageObjectPath::new("missing"))
            .await
            .is_err()
    );
}

#[test]
fn factory_rejects_duplicate_and_escaping_paths() {
    let location = StorageLocation::uri("memory://tpch");
    let duplicate = MemoryReadStorageFactory::new(
        location.clone(),
        [
            (StorageObjectPath::new("a"), Bytes::new()),
            (StorageObjectPath::new("a"), Bytes::new()),
        ],
    );
    assert!(duplicate.is_err());
    assert!(
        MemoryReadStorageFactory::new(
            location,
            [(StorageObjectPath::new("../outside"), Bytes::new())]
        )
        .is_err()
    );
}
