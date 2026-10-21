use super::*;

#[tokio::test]
async fn executes_ordered_local_parquet_query_across_files() {
    let root = tempfile::tempdir().expect("temporary data root");
    create_flights_table(root.path());

    let engine = LocalQueryEngine::new(root.path());
    let query = engine
        .prepare("FROM flights |> WHERE id > 1 |> SELECT id |> ORDER id DESC")
        .await
        .expect("prepare ordered local query");
    assert_eq!(
        query.tasks.len(),
        1,
        "a global sort must read all files in one task"
    );
    assert!(contains_sort(&query.tasks[0].plan.root));
    let statistics_context = Arc::clone(query.statistics_context());
    assert_eq!(statistics_context.sources().len(), 1);
    assert_eq!(statistics_context.sources()[0].table_name, "flights");
    assert!(Arc::ptr_eq(
        &statistics_context,
        query.tasks[0]
            .statistics_context
            .as_ref()
            .expect("task shares query context"),
    ));

    let mut stream = query.into_stream();
    let mut rows = Vec::new();
    while let Some(batch) = stream.next().await {
        let batch = batch.expect("query batch");
        for row in 0..batch.num_rows() {
            let (array, index) = batch
                .resolve(0, row)
                .expect("resolve reference-backed value")
                .expect("selected value is not null-extended");
            let ids = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("Int64 output");
            rows.push(ids.value(index));
        }
    }
    assert_eq!(rows, vec![5, 4, 3, 2]);
    assert!(
        statistics_context
            .operator_statistics()
            .iter()
            .any(|operator| operator.output_rows > 0)
    );
}

#[tokio::test]
async fn computed_order_and_aggregate_queries_preserve_order() {
    let root = tempfile::tempdir().expect("temporary data root");
    create_flights_table(root.path());
    let engine = LocalQueryEngine::new(root.path());

    let query = engine
        .prepare("FROM flights |> SELECT id |> ORDER id + 1 DESC")
        .await
        .expect("prepare computed ORDER BY query");
    let mut stream = query.into_stream();
    let mut ordered_ids = Vec::new();
    while let Some(batch) = stream.next().await {
        let batch = batch.expect("computed ORDER BY batch");
        assert_eq!(batch.num_columns(), 1, "temporary sort key must not escape");
        let ids_array = materialized_result_column(&batch, 0);
        let ids = ids_array
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Int64 projected output");
        ordered_ids.extend(ids.values().iter().copied());
    }
    assert_eq!(ordered_ids, vec![5, 4, 3, 2, 1]);

    let query = engine
        .prepare(
            "FROM flights |> AGGREGATE count(id) AS flight_count GROUP BY id |> ORDER id DESC",
        )
        .await
        .expect("prepare ordered aggregate query");
    let mut stream = query.into_stream();
    let mut grouped_ids = Vec::new();
    while let Some(batch) = stream.next().await {
        let batch = batch.expect("aggregate query batch");
        let ids_array = materialized_result_column(&batch, 0);
        let ids = ids_array
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Int64 grouping key");
        grouped_ids.extend(ids.values().iter().copied());
    }
    assert_eq!(grouped_ids, vec![5, 4, 3, 2, 1]);
}
