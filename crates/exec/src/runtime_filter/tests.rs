use super::*;
use arrow::array::Int64Array;
use common::statistics::{
    OperatorStatisticsIdentity, QuerySourceStatistics, QueryStatisticsContext,
    RuntimeStatisticsObservation,
};
use catalog::TableStatistics;
use types::{ResolvedColumn, VeinDataType};

#[derive(Debug)]
struct KeepValues {
    columns: Vec<ColumnId>,
    expected: i64,
}

#[derive(Debug)]
struct MetadataOnlyRange {
    columns: Vec<ColumnId>,
}

impl RuntimeFilter for MetadataOnlyRange {
    fn name(&self) -> &'static str {
        "test_metadata_range"
    }

    fn required_columns(&self) -> &[ColumnId] {
        &self.columns
    }

    fn memory_bytes(&self) -> usize {
        0
    }

    fn evaluate(
        &self,
        _batch: &RecordBatch,
        _column_positions: &[usize],
    ) -> Result<BooleanArray> {
        unreachable!("metadata-only filter is not evaluated on rows")
    }

    fn supports_row_evaluation(&self) -> bool {
        false
    }

    fn statistics_predicates(&self, _schema: &VeinSchema) -> Vec<catalog::StatisticsPredicate> {
        vec![catalog::StatisticsPredicate::Comparison {
            column: "id".into(),
            comparison: catalog::StatisticsComparison::GreaterOrEqual,
            literal: types::LogicalLiteral::Int64(1),
        }]
    }
}

impl RuntimeFilter for KeepValues {
    fn name(&self) -> &'static str {
        "test_keep_values"
    }

    fn required_columns(&self) -> &[ColumnId] {
        &self.columns
    }

    fn memory_bytes(&self) -> usize {
        0
    }

    fn evaluate(
        &self,
        batch: &RecordBatch,
        column_positions: &[usize],
    ) -> Result<BooleanArray> {
        let values = batch
            .column(column_positions[0])
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| VeinError::internal("test key must be Int64"))?;
        let mut output = BooleanBuilder::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            output.append_value(values.is_valid(row) && values.value(row) == self.expected);
        }
        Ok(output.finish())
    }
}

fn schema() -> VeinSchema {
    VeinSchema::new(vec![
        Arc::new(ResolvedColumn::new(
            ColumnId(7),
            "id",
            None,
            VeinDataType::Int64,
        )),
        Arc::new(ResolvedColumn::new(
            ColumnId(8),
            "label",
            None,
            VeinDataType::Utf8,
        )),
    ])
}

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
    let set = RuntimeFilterSet::with_statistics(vec![
        Arc::new(KeepValues {
            columns: vec![ColumnId(7)],
            expected: 2,
        }),
        Arc::new(KeepValues {
            columns: vec![ColumnId(7)],
            expected: 2,
        }),
    ], Arc::clone(&statistics));
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

#[test]
fn conservative_policy_uses_current_build_and_probe_measurements() {
    let policy = ConservativeRuntimeFilterPolicy;
    let candidates: Vec<Arc<dyn RuntimeFilter>> = vec![Arc::new(KeepValues {
        columns: vec![ColumnId(7)],
        expected: 2,
    })];
    let context = RuntimeFilterDecisionContext {
        build_rows: 4,
        build_keys: 3,
        probe_rows_estimate: None,
        probe_source_key: None,
        available_memory_bytes: 1024,
        query_statistics: QueryStatisticsContext::new([]).snapshot(),
    };

    assert!(policy.should_build_candidates(&context));
    assert_eq!(
        policy.select_filters(&context, &candidates).unwrap(),
        vec![0],
        "unknown probe cardinality must retain the exact safe-filter fallback"
    );

    let empty_probe = RuntimeFilterDecisionContext {
        probe_rows_estimate: Some(0),
        ..context
    };
    assert!(!policy.should_build_candidates(&empty_probe));
    assert!(policy
        .select_filters(&empty_probe, &candidates)
        .unwrap()
        .is_empty());

    let empty_build = RuntimeFilterDecisionContext {
        build_rows: 0,
        build_keys: 0,
        probe_rows_estimate: Some(10_000),
        probe_source_key: None,
        available_memory_bytes: 1024,
        query_statistics: QueryStatisticsContext::new([]).snapshot(),
    };
    assert!(!policy.should_build_candidates(&empty_build));
    assert!(policy
        .select_filters(&empty_build, &candidates)
        .unwrap()
        .is_empty());

    let null_key_only_build = RuntimeFilterDecisionContext {
        build_rows: 4,
        build_keys: 0,
        ..empty_build
    };
    assert!(!policy.should_build_candidates(&null_key_only_build));
    assert!(policy
        .select_filters(&null_key_only_build, &candidates)
        .unwrap()
        .is_empty());
}

#[test]
fn conservative_policy_uses_complete_runtime_probe_rows_but_not_partial_rows() {
    let policy = ConservativeRuntimeFilterPolicy;
    let candidates: Vec<Arc<dyn RuntimeFilter>> = vec![Arc::new(KeepValues {
        columns: vec![ColumnId(7)],
        expected: 2,
    })];
    let statistics = QueryStatisticsContext::new([QuerySourceStatistics {
        source_key: "probe@revision".into(),
        table_name: "probe".into(),
        statistics: Some(Arc::new(TableStatistics {
            num_rows: Some(100),
            ..TableStatistics::default()
        })),
    }]);
    statistics.record_scan(&RuntimeStatisticsObservation {
        table_key: "probe@revision".into(),
        statistics: Arc::new(TableStatistics {
            num_rows: Some(0),
            ..TableStatistics::default()
        }),
        complete_table_snapshot: false,
        expected_rows: Some(100),
    });
    let partial = RuntimeFilterDecisionContext {
        build_rows: 4,
        build_keys: 3,
        probe_rows_estimate: Some(100),
        probe_source_key: Some("probe@revision".into()),
        available_memory_bytes: 1024,
        query_statistics: statistics.snapshot(),
    };

    assert_eq!(partial.probe_rows(), (Some(100), "plan_estimate"));
    assert!(policy.should_build_candidates(&partial));

    statistics.record_scan(&RuntimeStatisticsObservation {
        table_key: "probe@revision".into(),
        statistics: Arc::new(TableStatistics {
            num_rows: Some(0),
            ..TableStatistics::default()
        }),
        complete_table_snapshot: true,
        expected_rows: Some(0),
    });
    let complete = RuntimeFilterDecisionContext {
        query_statistics: statistics.snapshot(),
        ..partial
    };

    assert_eq!(complete.probe_rows(), (Some(0), "complete_runtime_scan"));
    assert!(!policy.should_build_candidates(&complete));
    assert!(policy
        .select_filters(&complete, &candidates)
        .unwrap()
        .is_empty());
}

#[derive(Debug)]
struct ContextAwarePolicy;

impl RuntimeFilterDecisionPolicy for ContextAwarePolicy {
    fn should_build_candidates(&self, context: &RuntimeFilterDecisionContext) -> bool {
        context
            .query_statistics
            .scan_statistics("source@revision")
            .iter()
            .any(|observation| !observation.complete_table_snapshot)
    }

    fn select_filters(
        &self,
        context: &RuntimeFilterDecisionContext,
        candidates: &[Arc<dyn RuntimeFilter>],
    ) -> Result<Vec<usize>> {
        let Some(source) = context
            .query_statistics
            .source_statistics("source@revision")
        else {
            return Ok(Vec::new());
        };
        let observations = context
            .query_statistics
            .scan_statistics("source@revision");
        let has_partial_observation = observations.iter().any(|observation| {
            !observation.complete_table_snapshot && observation.statistics.num_rows == Some(10)
        });
        let has_operator_feedback = context
            .query_statistics
            .operators
            .iter()
            .any(|operator| operator.output_rows == 7 && operator.output_batches == 1);
        Ok(if source.num_rows == Some(100)
            && has_partial_observation
            && has_operator_feedback
            && !candidates.is_empty()
        {
            vec![0]
        } else {
            Vec::new()
        })
    }
}

#[test]
fn runtime_filter_policy_can_read_source_scan_and_operator_statistics() {
    let statistics = QueryStatisticsContext::new([QuerySourceStatistics {
        source_key: "source@revision".into(),
        table_name: "source".into(),
        statistics: Some(Arc::new(TableStatistics {
            num_rows: Some(100),
            ..TableStatistics::default()
        })),
    }]);
    statistics.record_scan(&RuntimeStatisticsObservation {
        table_key: "source@revision".into(),
        statistics: Arc::new(TableStatistics {
            num_rows: Some(10),
            ..TableStatistics::default()
        }),
        complete_table_snapshot: false,
        expected_rows: Some(100),
    });
    statistics
        .register_operator(OperatorStatisticsIdentity { task_id: 9, node_id: 3 }, "filter")
        .record_output(7);
    let query_statistics = statistics.snapshot();
    let context = RuntimeFilterDecisionContext {
        build_rows: 4,
        build_keys: 3,
        probe_rows_estimate: Some(100),
        probe_source_key: None,
        available_memory_bytes: 1024,
        query_statistics,
    };
    let candidates: Vec<Arc<dyn RuntimeFilter>> = vec![Arc::new(KeepValues {
        columns: vec![ColumnId(7)],
        expected: 2,
    })];
    let policy = ContextAwarePolicy;

    assert!(policy.should_build_candidates(&context));
    assert_eq!(
        policy.select_filters(&context, &candidates).unwrap(),
        vec![0],
        "policy must see query metadata and preserve the partial-scan provenance"
    );
    assert_eq!(context.query_statistics.sources.len(), 1);
    assert_eq!(context.query_statistics.operators.len(), 1);
    assert_eq!(context.query_statistics.operators[0].output_rows, 7);
}

#[test]
fn policy_selected_filter_set_changes_probe_selection_without_copying_filter_state() {
    let candidates: Vec<Arc<dyn RuntimeFilter>> = vec![
        Arc::new(KeepValues {
            columns: vec![ColumnId(7)],
            expected: 1,
        }),
        Arc::new(KeepValues {
            columns: vec![ColumnId(7)],
            expected: 2,
        }),
    ];
    let selected = RuntimeFilterSet::from_selection(&candidates, &[1])
        .expect("valid adaptive filter decision");
    let batch = RecordBatch::try_from_iter(vec![(
        "id",
        Arc::new(Int64Array::from(vec![1, 2, 3])) as Arc<dyn Array>,
    )])
        .expect("probe batch");
    let binding = selected.bind(&schema()).expect("selected filter binds");
    let mask = binding.evaluate(&batch).expect("evaluate selected filter");

    assert_eq!(selected.names(), vec!["test_keep_values"]);
    assert_eq!(mask, BooleanArray::from(vec![false, true, false]));
}

#[test]
fn policy_selection_rejects_duplicate_and_out_of_range_candidates() {
    let candidates: Vec<Arc<dyn RuntimeFilter>> = vec![Arc::new(KeepValues {
        columns: vec![ColumnId(7)],
        expected: 1,
    })];

    assert!(RuntimeFilterSet::from_selection(&candidates, &[0, 0]).is_err());
    assert!(RuntimeFilterSet::from_selection(&candidates, &[1]).is_err());
}
