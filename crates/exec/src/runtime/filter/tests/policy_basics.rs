use super::*;

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
