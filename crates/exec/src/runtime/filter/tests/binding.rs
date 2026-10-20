use super::*;

#[test]
fn binding_maps_filter_columns_by_id_and_preserves_stable_order() {
    let filters: Vec<Arc<dyn RuntimeFilter>> = vec![Arc::new(KeepValues {
        columns: vec![ColumnId(7)],
        expected: 2,
    })];
    let set = RuntimeFilterSet::new(filters);
    let binding = set.bind(&schema()).expect("filter columns bind");

    assert_eq!(binding.columns().len(), 1);
    assert_eq!(binding.columns()[0].id, ColumnId(7));
}

#[test]
fn independent_bindings_keep_each_filter_narrow_and_preserve_order() {
    let set = RuntimeFilterSet::new(vec![
        Arc::new(KeepValues { columns: vec![ColumnId(7)], expected: 2 }),
        Arc::new(KeepValues { columns: vec![ColumnId(7)], expected: 3 }),
    ]);
    let bindings = set.bind_stages(&schema()).expect("row filters bind");

    assert_eq!(bindings.len(), 1, "same-column filters share one decode stage");
    assert_eq!(
        bindings[0].filter_names(),
        vec!["test_keep_values", "test_keep_values"]
    );
    assert_eq!(bindings[0].columns().len(), 1);
    assert_eq!(bindings[0].columns()[0].id, ColumnId(7));

    let batch = RecordBatch::try_from_iter(vec![(
        "id",
        Arc::new(Int64Array::from(vec![1, 2, 3])) as Arc<dyn Array>,
    )])
        .expect("probe batch");
    assert_eq!(
        bindings[0].evaluate(&batch).expect("combined same-column stage"),
        BooleanArray::from(vec![false, false, false])
    );
}

#[test]
fn diagnostics_separate_row_filters_from_metadata_pruning_filters() {
    let set = RuntimeFilterSet::new(vec![
        Arc::new(KeepValues {
            columns: vec![ColumnId(7)],
            expected: 1,
        }),
        Arc::new(MetadataOnlyRange {
            columns: vec![ColumnId(7)],
        }),
    ]);

    let (predicates, diagnostics) = set.statistics_predicates_with_diagnostics(&schema());

    assert_eq!(predicates.len(), 1);
    assert_eq!(
        diagnostics,
        vec![
            RuntimeFilterPruningDiagnostic {
                name: "test_keep_values",
                row_evaluation: true,
                metadata_predicate_count: 0,
            },
            RuntimeFilterPruningDiagnostic {
                name: "test_metadata_range",
                row_evaluation: false,
                metadata_predicate_count: 1,
            },
        ]
    );
    assert_eq!(set.row_filter_names(), vec!["test_keep_values"]);
}

#[test]
fn binding_uses_scan_order_without_changing_filter_key_positions() {
    let set = RuntimeFilterSet::new(vec![
        Arc::new(KeepValues {
            columns: vec![ColumnId(8)],
            expected: 20,
        }),
        Arc::new(KeepValues {
            columns: vec![ColumnId(7)],
            expected: 2,
        }),
    ]);
    let binding = set.bind(&schema()).expect("filter columns bind");
    let bound_ids = binding
        .columns()
        .iter()
        .map(|column| column.id)
        .collect::<Vec<_>>();
    assert_eq!(bound_ids, vec![ColumnId(7), ColumnId(8)]);

    let batch = RecordBatch::try_from_iter(vec![
        (
            "id",
            Arc::new(Int64Array::from(vec![1, 2, 3])) as Arc<dyn Array>,
        ),
        (
            "label",
            Arc::new(Int64Array::from(vec![10, 20, 30])) as Arc<dyn Array>,
        ),
    ])
        .expect("test batch");
    let mask = binding.evaluate(&batch).expect("evaluate bound filters");

    assert_eq!(mask, BooleanArray::from(vec![false, true, false]));
}

#[test]
fn missing_filter_column_declines_pushdown() {
    let set = RuntimeFilterSet::new(vec![Arc::new(KeepValues {
        columns: vec![ColumnId(99)],
        expected: 2,
    })]);

    assert!(set.bind(&schema()).is_none());
}

#[test]
fn combined_runtime_filters_return_a_selection_and_account_rows() {
    let statistics_context = Arc::new(QueryStatisticsContext::new([]));
    let statistics = statistics_context.register_operator(
        OperatorStatisticsIdentity {
            task_id: 9,
            node_id: 3,
        },
        "join",
    );
    let set = RuntimeFilterSet::with_statistics(
        vec![
            Arc::new(KeepValues {
                columns: vec![ColumnId(7)],
                expected: 2,
            }),
            Arc::new(KeepValues {
                columns: vec![ColumnId(7)],
                expected: 2,
            }),
        ],
        Arc::clone(&statistics),
    );
    let batch = RecordBatch::try_from_iter(vec![(
        "id",
        Arc::new(Int64Array::from(vec![1, 2, 3])) as Arc<dyn Array>,
    )])
        .expect("test batch");
    let binding = set.bind(&schema()).expect("filter columns bind");
    let mask = binding.evaluate(&batch).expect("combine filters");

    assert_eq!(mask, BooleanArray::from(vec![false, true, false]));
    assert_eq!(set.rows_seen(), 3);
    assert_eq!(set.rows_kept(), 1);
    let snapshots = statistics_context.operator_statistics();
    let [operator] = snapshots.as_slice() else {
        panic!("runtime filter feedback belongs to its query operator");
    };
    assert_eq!(operator.metrics["runtime_filter_rows_seen"], 3);
    assert_eq!(operator.metrics["runtime_filter_rows_kept"], 1);
}

#[test]
fn empty_filter_set_does_not_bind_or_allocate_a_mask() {
    assert!(RuntimeFilterSet::empty().bind(&schema()).is_none());
}
