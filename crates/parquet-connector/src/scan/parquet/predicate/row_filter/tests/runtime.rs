use super::*;

#[test]
fn runtime_filters_bind_as_independent_narrow_parquet_predicates() {
    let arrow_schema = Schema::new(vec![
        Field::new("key_a", DataType::Int64, true),
        Field::new("key_b", DataType::Int64, true),
    ]);
    let parquet_schema = SchemaDescriptor::new(Arc::new(
        ParquetType::group_type_builder("schema")
            .with_fields(vec!["key_a", "key_b"].into_iter().map(|name| {
                Arc::new(
                    ParquetType::primitive_type_builder(name, parquet::basic::Type::INT64)
                        .build()
                        .expect("Parquet key field"),
                )
            }).collect())
            .build()
            .expect("Parquet schema"),
    ));
    let key_a = Arc::new(ResolvedColumn::new(
        ColumnId(10), "key_a", None, VeinDataType::Int64,
    ));
    let key_b = Arc::new(ResolvedColumn::new(
        ColumnId(11), "key_b", None, VeinDataType::Int64,
    ));
    let projected = VeinSchema::new(vec![key_a, key_b]);
    let runtime_filters = RuntimeFilterSet::new(vec![
        Arc::new(KeepColumnValue { column: ColumnId(10), expected: 2 }),
        Arc::new(KeepColumnValue { column: ColumnId(11), expected: 3 }),
    ]);
    let expression_kernels: Arc<dyn ExpressionKernelProvider> =
        Arc::new(ExpressionKernelRegistry::with_builtins());
    let pipeline = build_scan_predicates(ScanPredicateContext {
        arrow_schema: &arrow_schema,
        parquet_schema: &parquet_schema,
        path: "test.parquet",
        projected: &projected,
        pushdown: None,
        registry: &Arc::new(FunctionRegistry::new()),
        expression_kernels: &expression_kernels,
        detailed_metrics_enabled: true,
        predicate_metrics: &Arc::new(ScanPredicateMetrics::default()),
        runtime_filters: &runtime_filters,
        adaptive_order: None,
    })
        .expect("runtime filter predicates");
    assert_eq!(pipeline.stage_metrics.len(), 2);
    let first_metrics = Arc::clone(&pipeline.stage_metrics[0].3);
    let second_metrics = Arc::clone(&pipeline.stage_metrics[1].3);
    let mut predicates = pipeline.row_filter.expect("row filter").into_predicates();

    assert_eq!(predicates.len(), 2, "each runtime filter must have its own stage");
    let input_a = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("key_a", DataType::Int64, true)])),
        vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
    )
        .expect("first narrow filter batch");
    let input_b = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("key_b", DataType::Int64, true)])),
        vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
    )
        .expect("second narrow filter batch");

    assert_eq!(
        predicates[0].evaluate(input_a).expect("first runtime filter"),
        BooleanArray::from(vec![false, true, false])
    );
    assert_eq!(
        predicates[1].evaluate(input_b).expect("second runtime filter"),
        BooleanArray::from(vec![false, false, true])
    );
    let first = first_metrics.snapshot();
    let second = second_metrics.snapshot();
    assert_eq!((first.batches, first.rows_in, first.rows_kept), (1, 3, 1));
    assert_eq!((second.batches, second.rows_in, second.rows_kept), (1, 3, 1));
    assert!(first.input_array_bytes > 0 && second.input_array_bytes > 0);
}
