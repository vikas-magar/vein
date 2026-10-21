use std::sync::Arc;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use catalog::MetadataStore;
use parquet::arrow::ArrowWriter;
use storage::opendal::OpenDalReadStorageFactory;
use storage::{ReadStorageFactory, StorageLocation, StorageObjectPath};

use super::load_footer_from_storage;

#[tokio::test]
async fn catalog_footer_loading_uses_shared_range_reader_and_cache() {
    let root = tempfile::tempdir().expect("temporary root");
    let path = root.path().join("values.parquet");
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![1_i64, 2]))],
    )
        .expect("valid test batch");
    let file = std::fs::File::create(&path).expect("create test file");
    let mut writer = ArrowWriter::try_new(file, Arc::clone(&schema), None)
        .expect("create Parquet writer");
    writer.write(&batch).expect("write test batch");
    writer.close().expect("finish Parquet file");

    let location = StorageLocation::local(root.path().to_path_buf());
    let storage = OpenDalReadStorageFactory
        .open(&location)
        .expect("open storage");
    let path = StorageObjectPath::new("values.parquet");
    let source = storage.stat(&path).await.expect("stat source");
    let store = MetadataStore::new();

    let (first_schema, first_footer, first_hit) =
        load_footer_from_storage(&storage, &path, &source, &store)
            .await
            .expect("load footer");
    let (_, second_footer, second_hit) =
        load_footer_from_storage(&storage, &path, &source, &store)
            .await
            .expect("reuse footer");

    assert_eq!(first_schema.as_ref(), schema.as_ref());
    assert!(!first_hit);
    assert!(second_hit);
    assert!(Arc::ptr_eq(&first_footer, &second_footer));
}
