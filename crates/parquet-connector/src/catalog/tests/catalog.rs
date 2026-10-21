use std::fs;

use plan::CatalogProvider;

use super::support::{catalog, write_parquet};

#[tokio::test]
async fn discovers_sorted_visible_files_and_populates_shared_footer_cache() {
    let root = tempfile::tempdir().expect("temporary root");
    let table = root.path().join("flights");
    fs::create_dir_all(&table).expect("create table directory");
    write_parquet(&table.join("b.parquet"), "flight_id");
    write_parquet(&table.join("a.parquet"), "flight_id");
    write_parquet(&table.join("_metadata.parquet"), "ignored");

    let catalog = catalog(root.path());
    let metadata = catalog.get_table("flights").await.expect("resolve table");
    assert_eq!(metadata.files.len(), 2);
    assert!(metadata.files[0].ends_with("a.parquet"));
    assert!(metadata.files[1].ends_with("b.parquet"));
    assert_eq!(metadata.schema.fields[0].name, "flight_id");
    assert_eq!(metadata.statistics.as_ref().unwrap().num_rows, Some(4));
    assert!(crate::ParquetMetadataService::new(catalog.metadata_store())
        .cached_footer(&metadata.storage_metadata[0])
        .is_some());
}

#[tokio::test]
async fn rejects_files_with_different_schemas() {
    let root = tempfile::tempdir().expect("temporary root");
    let table = root.path().join("flights");
    fs::create_dir_all(&table).expect("create table directory");
    write_parquet(&table.join("a.parquet"), "flight_id");
    write_parquet(&table.join("b.parquet"), "other_id");

    let error = catalog(root.path())
        .get_table("flights")
        .await
        .expect_err("mixed schemas must fail");
    assert!(error.to_string().contains("schema mismatch"));
}
