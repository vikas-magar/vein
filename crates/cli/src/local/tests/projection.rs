use super::*;

#[tokio::test]
async fn parquet_projection_keeps_filter_columns_and_compacts_predicate_input() {
    let root = tempfile::tempdir().expect("temporary data root");
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("unused", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec!["discard-a", "discard-b"])),
            Arc::new(Int64Array::from(vec![10, 42])),
        ],
    )
        .expect("valid batch");
    write_parquet_batch(root.path(), "wide", "create", Arc::clone(&schema), &batch);

    let engine = test_engine(root.path());
    let query = engine
        .prepare("FROM wide |> WHERE id = 2 |> SELECT value")
        .await
        .expect("prepare projected scan");
    let scan = find_file_scan(&query.physical_plan(0).expect("first task").root)
        .expect("expected a Parquet file scan in the physical plan");
    assert_eq!(
        scan.projected
            .fields
            .iter()
            .map(|column| column.name.as_str())
            .collect::<Vec<_>>(),
        vec!["id", "value"],
    );

    let mut stream = query.into_stream();
    let batch = stream
        .next()
        .await
        .expect("query batch")
        .expect("successful scan");
    let values_array = materialized_result_column(&batch, 0);
    let values = values_array
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Int64 output");
    assert_eq!(values.values(), &[42]);
    assert!(stream.next().await.is_none());

    let write = engine
        .prepare("FROM wide |> WHERE id = 2 |> SELECT value |> WRITE narrowed")
        .await
        .expect("prepare projected WRITE");
    let receipt = write.execute_write().await.expect("commit projected WRITE");
    assert_eq!(receipt.row_count, 1);

    let catalog = parquet_catalog(root.path());
    let metadata = CatalogProvider::get_table(&catalog, "narrowed")
        .await
        .expect("read projected output metadata");
    assert_eq!(metadata.schema.fields.len(), 1);
    assert_eq!(metadata.schema.fields[0].name, "value");
}
