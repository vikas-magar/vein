use super::*;

#[tokio::test]
async fn runtime_synopsis_uses_complete_rows_with_projected_column_coverage() {
    let root = tempfile::tempdir().expect("temporary data root");
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int64, false),
        Field::new("payload", DataType::Utf8, false),
    ]));
    let input = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
            Arc::new(StringArray::from(vec!["a", "b", "c", "d"])),
        ],
    )
        .expect("valid input batch");
    write_parquet_table(root.path(), "events", "create", Arc::clone(&schema), &input);

    let engine = test_engine(root.path());
    let query = engine
        .prepare("FROM events |> SELECT key")
        .await
        .expect("prepare query");
    let scan = find_file_scan(&query.physical_plan(0).expect("first task").root)
        .expect("find file scan");
    let statistics_scan = scan
        .runtime_statistics
        .as_ref()
        .expect("statistics context");
    assert!(statistics_scan.row_coverage_candidate);
    let table_key = statistics_scan.table_key.clone();
    assert_eq!(
        scan.projected.fields.len(),
        1,
        "only the selected column is scanned"
    );

    let mut stream = query.into_stream();
    let mut row_count = 0;
    while let Some(batch) = stream.next().await {
        row_count += batch.expect("query batch").num_rows();
    }
    assert_eq!(row_count, 4);

    let statistics = engine
        .runtime_statistics()
        .complete_table_statistics(&table_key)
        .expect("background synopsis is visible to the next plan");
    assert_eq!(statistics.num_rows, Some(4));
    assert_eq!(statistics.column_statistics.len(), 1);
    assert_eq!(statistics.column_statistics["key"].distinct_count, Some(4));
}
