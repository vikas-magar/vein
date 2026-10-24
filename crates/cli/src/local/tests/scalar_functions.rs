use super::*;

#[tokio::test]
async fn executes_lower_and_round_scalar_projections_preserving_nulls() {
    let root = tempfile::tempdir().expect("temporary data root");
    let schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, true),
        Field::new("amount", DataType::Float64, true),
        Field::new("order_date", DataType::Date32, true),
        Field::new(
            "left_vector",
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 2),
            true,
        ),
        Field::new(
            "right_vector",
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 2),
            true,
        ),
    ]));
    let vector_field = Arc::new(Field::new("item", DataType::Float32, true));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec![Some("MiXeD"), None, Some("VeIn")])),
            Arc::new(Float64Array::from(vec![Some(1.2), Some(1.5), Some(-1.5)])),
            Arc::new(arrow::array::Date32Array::from(vec![
                Some(0),
                Some(19_723),
                None,
            ])),
            Arc::new(
                FixedSizeListArray::try_new(
                    vector_field.clone(),
                    2,
                    Arc::new(Float32Array::from(vec![1.0, 0.0, 0.0, 1.0, 1.0, 0.0])),
                    Some(vec![true, true, false].into()),
                )
                    .expect("valid left vectors"),
            ),
            Arc::new(
                FixedSizeListArray::try_new(
                    vector_field,
                    2,
                    Arc::new(Float32Array::from(vec![0.0, 1.0, 0.0, 1.0, 1.0, 0.0])),
                    None,
                )
                    .expect("valid right vectors"),
            ),
        ],
    )
        .expect("valid batch");
    write_parquet_table(root.path(), "names", "create", Arc::clone(&schema), &batch);

    let engine = test_engine(root.path());
    let query = engine
        .prepare("FROM names |> SELECT LOWER(name) AS lowered")
        .await
        .expect("prepare LOWER projection");
    let mut stream = query.into_stream();
    let result = stream
        .next()
        .await
        .expect("query batch")
        .expect("successful LOWER projection");
    let values_array = materialized_result_column(&result, 0);
    let values = values_array
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("UTF-8 output");
    assert_eq!(values.value(0), "mixed");
    assert!(values.is_null(1));
    assert_eq!(values.value(2), "vein");
    assert!(stream.next().await.is_none());

    let query = engine
        .prepare("FROM names |> SELECT ROUND(amount) AS rounded")
        .await
        .expect("prepare ROUND projection");
    let mut stream = query.into_stream();
    let result = stream
        .next()
        .await
        .expect("query batch")
        .expect("successful ROUND projection");
    let values_array = materialized_result_column(&result, 0);
    let values = values_array
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("Float64 output");
    assert_eq!(values.value(0), 1.0);
    assert_eq!(values.value(1), 2.0);
    assert_eq!(values.value(2), -2.0);
    assert!(stream.next().await.is_none());

    let query = engine
        .prepare(
            "FROM names |> SELECT EXTRACT(YEAR FROM order_date) AS year, YEAR(order_date) AS year_alias",
        )
        .await
        .expect("prepare EXTRACT and YEAR projections");
    let mut stream = query.into_stream();
    let result = stream
        .next()
        .await
        .expect("query batch")
        .expect("successful temporal projections");
    let year_array = materialized_result_column(&result, 0);
    let years = year_array
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .expect("Int64 output");
    assert_eq!(years.value(0), 1970);
    assert_eq!(years.value(1), 2024);
    assert!(years.is_null(2));
    let year_alias_array = materialized_result_column(&result, 1);
    let year_alias = year_alias_array
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .expect("Int64 YEAR alias output");
    assert_eq!(year_alias, years);
    assert!(stream.next().await.is_none());

    let query = engine
        .prepare(
            "FROM names |> WHERE order_date = DATE '1970-01-01' |> EXTEND TRUNCATE(order_date, 'MONTH') AS month_start |> SELECT month_start, TRUNCATE(TIMESTAMP '2024-03-31 12:34:56', 'HOUR') AS hour_start",
        )
        .await
        .expect("prepare temporal EXTEND and timestamp literal");
    let mut stream = query.into_stream();
    let result = stream
        .next()
        .await
        .expect("query batch")
        .expect("successful temporal EXTEND and timestamp literal");
    let month_start_array = materialized_result_column(&result, 0);
    let month_start = month_start_array
        .as_any()
        .downcast_ref::<arrow::array::Date32Array>()
        .expect("Date32 TRUNCATE output");
    assert_eq!(month_start.value(0), 0);
    let hour_start_array = materialized_result_column(&result, 1);
    let hour_start = hour_start_array
        .as_any()
        .downcast_ref::<arrow::array::TimestampMicrosecondArray>()
        .expect("timestamp TRUNCATE output");
    assert_eq!(hour_start.value(0), 1_711_886_400_000_000);
    assert!(stream.next().await.is_none());

    let query = engine
        .prepare("FROM names |> SELECT VECTOR_DISTANCE(left_vector, right_vector) AS distance")
        .await
        .expect("prepare VECTOR_DISTANCE projection");
    let mut stream = query.into_stream();
    let result = stream
        .next()
        .await
        .expect("query batch")
        .expect("successful VECTOR_DISTANCE projection");
    let distance_array = materialized_result_column(&result, 0);
    let distances = distance_array
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("Float64 output");
    assert_eq!(distances.value(0), 1.0);
    assert_eq!(distances.value(1), 0.0);
    assert!(distances.is_null(2));
    assert!(stream.next().await.is_none());
}
