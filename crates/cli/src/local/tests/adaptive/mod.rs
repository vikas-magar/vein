use super::*;

mod integration;

#[tokio::test]
async fn skips_redundant_checkpoint_when_join_already_builds_filtered_input() {
    let root = tempfile::tempdir().expect("temporary data root");
    let left_schema = Arc::new(Schema::new(vec![
        Field::new("left_key", DataType::Int64, false),
        Field::new("left_value", DataType::Utf8, false),
    ]));
    let left_batch = RecordBatch::try_new(
        Arc::clone(&left_schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec!["one", "two"])),
        ],
    )
        .expect("valid left batch");
    write_parquet_table(
        root.path(),
        "lefts",
        "create",
        Arc::clone(&left_schema),
        &left_batch,
    );

    let right_schema = Arc::new(Schema::new(vec![
        Field::new("right_key", DataType::Int64, false),
        Field::new("right_value", DataType::Utf8, false),
    ]));
    let right_batch = RecordBatch::try_new(
        Arc::clone(&right_schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["one", "two", "three"])),
        ],
    )
        .expect("valid right batch");
    write_parquet_table(
        root.path(),
        "rights",
        "create",
        Arc::clone(&right_schema),
        &right_batch,
    );

    let engine = test_engine(root.path());
    let query = engine
        .prepare(
            "FROM lefts |> WHERE left_key > 0 |> JOIN rights ON left_key = right_key |> SELECT left_value, right_value",
        )
        .await
        .expect("prepare join with adaptive checkpoint");
    assert!(
        !query.has_adaptive_checkpoint(),
        "the physical join already builds this filtered input before probing"
    );

    let mut stream = query.into_stream();
    let mut rows = Vec::new();
    while let Some(batch) = stream.next().await {
        let batch = batch.expect("continuation output batch");
        let left_array = materialized_result_column(&batch, 0);
        let left_values = left_array
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("left value output");
        let right_array = materialized_result_column(&batch, 1);
        let right_values = right_array
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("right value output");
        for row in 0..batch.num_rows() {
            rows.push((
                left_values.value(row).to_owned(),
                right_values.value(row).to_owned(),
            ));
        }
    }
    rows.sort();
    assert_eq!(
        rows,
        vec![("one".into(), "one".into()), ("two".into(), "two".into())]
    );
}
