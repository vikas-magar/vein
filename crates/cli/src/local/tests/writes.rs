use super::*;

#[tokio::test]
async fn executes_pipe_write_and_exposes_only_committed_rows() {
    let root = tempfile::tempdir().expect("temporary data root");
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(vec![4, 5, 6]))],
    )
        .expect("valid source batch");
    write_parquet_table(root.path(), "source", "create", Arc::clone(&schema), &batch);

    let engine = test_engine(root.path());
    let error = match engine
        .prepare("FROM source |> SELECT id |> WRITE archived (compression='zstd')")
        .await
    {
        Ok(_) => panic!("unsupported writer options should fail preparation"),
        Err(error) => error,
    };
    assert!(
        error
            .to_string()
            .contains("does not support option 'compression'")
    );

    let query = engine
        .prepare("FROM source |> WHERE id > 4 |> SELECT id |> WRITE archived")
        .await
        .expect("prepare WRITE query");
    assert!(query.is_write());
    let receipt = query.execute_write().await.expect("commit WRITE query");
    assert_eq!(receipt.destination, "archived");
    assert_eq!(receipt.row_count, 2);

    let catalog = parquet_catalog(root.path());
    let metadata = CatalogProvider::get_table(&catalog, "archived")
        .await
        .expect("read committed output table");
    assert_eq!(metadata.files.len(), 1);
    assert_eq!(metadata.schema.fields[0].name, "id");
}
