use super::*;

#[tokio::test]
async fn filtered_and_complete_scans_publish_only_valid_table_statistics() {
    let root = tempfile::tempdir().expect("temporary data root");
    let (table_dir, schema) = create_flights_table(root.path());
    let engine = LocalQueryEngine::new(root.path());
    let catalog = parquet_catalog_with_runtime(root.path(), engine.runtime_statistics.clone());
    let table = CatalogProvider::get_table(&catalog, "flights")
        .await
        .expect("read scan table metadata");
    let runtime_key = common::statistics::runtime::table_statistics_key(
        table
            .catalog_snapshot
            .as_ref()
            .expect("catalog pins source revision"),
    );

    let query = engine
        .prepare("FROM flights |> WHERE id > 1 |> SELECT id")
        .await
        .expect("prepare filtered scan");
    let mut stream = query.into_stream();
    let mut filtered_rows = 0;
    while let Some(batch) = stream.next().await {
        filtered_rows += batch.expect("filtered query batch").num_rows();
    }
    assert_eq!(filtered_rows, 4);
    assert!(
        engine
            .runtime_statistics
            .complete_table_statistics(&runtime_key)
            .is_none(),
        "filtered scans must not become full-table optimizer statistics"
    );

    let query = engine
        .prepare("FROM flights |> SELECT id")
        .await
        .expect("prepare complete table scan");
    let mut stream = query.into_stream();
    let mut scanned_rows = 0;
    while let Some(batch) = stream.next().await {
        scanned_rows += batch.expect("full scan batch").num_rows();
    }
    assert_eq!(scanned_rows, 5);

    let runtime = engine
        .runtime_statistics
        .complete_table_statistics(&runtime_key)
        .expect("complete scan publishes runtime synopsis");
    assert_eq!(runtime.num_rows, Some(5));
    assert_eq!(runtime.column_statistics["id"].distinct_count, Some(5));

    let replanned_table = CatalogProvider::get_table(&catalog, "flights")
        .await
        .expect("read table metadata after runtime collection");
    let planner_statistics = replanned_table
        .statistics
        .as_ref()
        .expect("runtime synopsis is visible to binder and optimizer");
    assert_eq!(planner_statistics.num_rows, Some(5));
    assert_eq!(planner_statistics.column_statistics["id"].distinct_count, Some(5));

    let replaced_path = table_dir.join("part-1.parquet");
    std::fs::remove_file(&replaced_path).expect("remove old source file");
    let replacement = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(vec![7, 7]))],
    )
        .expect("valid replacement batch");
    write_parquet_batch(&replaced_path, schema, &replacement);

    let changed_table = CatalogProvider::get_table(&catalog, "flights")
        .await
        .expect("read table after same-path replacement");
    let changed_snapshot = changed_table
        .catalog_snapshot
        .as_ref()
        .expect("catalog pins replacement source revision");
    assert_ne!(
        table.catalog_snapshot.as_ref(),
        Some(changed_snapshot),
        "source replacement must create a new runtime-statistics identity"
    );
    let changed_runtime_key =
        common::statistics::runtime::table_statistics_key(changed_snapshot);
    assert!(
        engine
            .runtime_statistics
            .complete_table_statistics(&changed_runtime_key)
            .is_none(),
        "a complete synopsis for the old source revision must not be reused"
    );
    assert_eq!(changed_table.statistics.as_ref().unwrap().num_rows, Some(5));
    assert_eq!(
        changed_table.statistics.as_ref().unwrap().column_statistics["id"].distinct_count,
        None,
        "the old five-distinct-value synopsis must not leak into the replacement"
    );
}
