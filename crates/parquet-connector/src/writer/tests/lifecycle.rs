use std::sync::Arc;

use arrow::array::Int32Array;
use arrow::datatypes::{DataType, Field, Schema};
use ::catalog::CatalogProvider;
use exec::operators::sink::WriteStorage;
use exec::pipeline::DataChunkView;

use super::support::{catalog, chunk, storage, target};

#[test]
fn invalid_chunk_aborts_staging_without_publishing_a_manifest() {
    let root = tempfile::tempdir().expect("temporary root");
    let storage = storage(root.path());
    let mut transaction = storage.begin_write(&target(None)).expect("begin create");
    transaction.write_view(&chunk(vec![1])).expect("write first chunk");
    let schema = Arc::new(Schema::new(vec![Field::new("other", DataType::Int32, false)]));
    let incompatible = DataChunkView::from_dense_arrays(vec![Arc::new(Int32Array::from(vec![2]))])
        .expect("valid chunk").with_schema(schema).expect("valid schema");
    assert!(transaction.write_view(&incompatible).is_err());
    drop(transaction);
    assert!(storage.read_manifest("events").expect("read catalog").is_none());
}

#[tokio::test]
async fn empty_write_publishes_a_valid_schema_only_parquet_file() {
    let root = tempfile::tempdir().expect("temporary root");
    let storage = storage(root.path());
    let receipt = storage.begin_write(&target(None)).expect("begin write")
        .commit().expect("commit empty table");
    assert_eq!(receipt.row_count, 0);
    let table = catalog(root.path()).get_table("events").await.expect("read empty table");
    assert_eq!(table.files.len(), 1);
    assert_eq!(table.schema.fields[0].name, "id");
}

#[test]
fn concurrent_stale_writer_cannot_overwrite_a_new_manifest() {
    let root = tempfile::tempdir().expect("temporary root");
    let first_storage = storage(root.path());
    let second_storage = storage(root.path());
    let mut first = first_storage.begin_write(&target(None)).expect("begin first");
    let mut second = second_storage.begin_write(&target(None)).expect("begin second");
    first.write_view(&chunk(vec![1])).expect("write first");
    second.write_view(&chunk(vec![2])).expect("write second");
    first.commit().expect("first commit wins");
    assert!(second.commit().is_err());
    let manifest = first_storage.read_manifest("events").expect("read manifest").expect("committed table");
    assert_eq!(manifest.revision, 1);
    assert_eq!(manifest.files.len(), 1);
}

#[test]
fn manifest_paths_cannot_escape_the_storage_root() {
    let root = tempfile::tempdir().expect("temporary root");
    assert!(super::super::filesystem::resolve_manifest_file(root.path(), "../outside.parquet").is_err());
    assert!(super::super::filesystem::resolve_manifest_file(root.path(), "/outside.parquet").is_err());
}
