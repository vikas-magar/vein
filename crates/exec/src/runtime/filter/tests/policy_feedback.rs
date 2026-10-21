use super::*;

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
