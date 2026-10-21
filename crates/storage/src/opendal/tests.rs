use std::io::Write;

use bytes::Bytes;
use futures::TryStreamExt;

use crate::{
    ReadStorageFactory, StorageLocation, StorageObjectPath,
    opendal::OpenDalReadStorageFactory,
};

#[tokio::test]
async fn filesystem_adapter_stats_reads_ranges_and_streams_objects() {
    let directory = tempfile::tempdir().expect("temporary storage root");
    let mut file = std::fs::File::create(directory.path().join("part.bin"))
        .expect("create storage object");
    file.write_all(b"zero-copy-range").expect("write storage object");
    std::fs::create_dir(directory.path().join("nested")).expect("create nested directory");
    std::fs::write(directory.path().join("nested/part-2.bin"), b"nested")
        .expect("write nested object");

    let location = StorageLocation::local(directory.path().to_path_buf());
    let storage = OpenDalReadStorageFactory
        .open(&location)
        .expect("open filesystem storage");
    let object = StorageObjectPath::new("part.bin");
    let metadata = storage.stat(&object).await.expect("stat object");

    assert_eq!(metadata.size_bytes, 15);
    assert_eq!(metadata.identity.path(), &object);
    assert_eq!(
        metadata.identity.location(),
        &StorageLocation::local(directory.path().canonicalize().unwrap())
    );
    assert_eq!(
        storage.read_range(&object, 5..10).await.unwrap(),
        Bytes::from_static(b"copy-")
    );

    let mut paths = storage
        .list(&StorageObjectPath::new(""))
        .try_collect::<Vec<_>>()
        .await
        .expect("stream filesystem listing");
    paths.sort_by(|left, right| left.as_str().cmp(right.as_str()));
    assert_eq!(
        paths,
        vec![StorageObjectPath::new("nested/part-2.bin"), object]
    );
}

#[tokio::test]
async fn listing_prefix_respects_path_component_boundaries() {
    let directory = tempfile::tempdir().expect("temporary storage root");
    std::fs::create_dir(directory.path().join("part")).expect("create part table");
    std::fs::create_dir(directory.path().join("partsupp")).expect("create partsupp table");
    std::fs::write(directory.path().join("part/part.1.parquet"), b"part")
        .expect("write part shard");
    std::fs::write(directory.path().join("partsupp/partsupp.1.parquet"), b"partsupp")
        .expect("write partsupp shard");

    let storage = OpenDalReadStorageFactory
        .open(&StorageLocation::local(directory.path().to_path_buf()))
        .expect("open filesystem storage");
    let paths = storage
        .list(&StorageObjectPath::new("part"))
        .try_collect::<Vec<_>>()
        .await
        .expect("list part table objects");

    assert_eq!(paths, vec![StorageObjectPath::new("part/part.1.parquet")]);
}

#[tokio::test]
async fn filesystem_locations_normalize_before_object_identity_is_reported() {
    let directory = tempfile::tempdir().expect("temporary storage root");
    std::fs::write(directory.path().join("object"), b"value").expect("write object");
    let factory = OpenDalReadStorageFactory;
    let first = factory
        .open(&StorageLocation::local(directory.path().to_path_buf()))
        .expect("open original root");
    let second = factory
        .open(&StorageLocation::local(directory.path().join(".")))
        .expect("open equivalent root");
    let path = StorageObjectPath::new("object");

    let first_identity = first.stat(&path).await.unwrap().identity;
    let second_identity = second.stat(&path).await.unwrap().identity;

    assert_eq!(first_identity, second_identity);
}

#[tokio::test]
async fn optional_object_read_distinguishes_missing_from_storage_failure() {
    let directory = tempfile::tempdir().expect("temporary storage root");
    std::fs::write(directory.path().join("manifest.json"), b"manifest")
        .expect("write manifest");
    let storage = OpenDalReadStorageFactory
        .open(&StorageLocation::local(directory.path().to_path_buf()))
        .expect("open filesystem storage");

    assert_eq!(
        storage
            .read_optional(&StorageObjectPath::new("manifest.json"))
            .await
            .expect("read present manifest"),
        Some(Bytes::from_static(b"manifest"))
    );
    assert_eq!(
        storage
            .read_optional(&StorageObjectPath::new("missing.json"))
            .await
            .expect("missing manifest is not an I/O failure"),
        None
    );
}
