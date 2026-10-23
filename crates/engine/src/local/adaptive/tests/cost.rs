use super::*;

#[test]
fn checkpoint_cost_rejects_missing_invalid_or_dominated_measurements() {
    let model = InputDominanceCost;
    let sibling = CheckpointWork { input_rows: Some(100), output_rows: Some(50.0) };
    for candidate in [
        CheckpointWork { input_rows: None, output_rows: Some(1.0) },
        CheckpointWork { input_rows: Some(1), output_rows: None },
        CheckpointWork { input_rows: Some(101), output_rows: Some(1.0) },
        CheckpointWork { input_rows: Some(1), output_rows: Some(51.0) },
        CheckpointWork { input_rows: Some(1), output_rows: Some(f64::NAN) },
    ] { assert!(!model.allows(candidate, sibling)); }
    assert!(model.allows(CheckpointWork { input_rows: Some(10), output_rows: Some(5.0) }, sibling));
    assert!(model.allows(CheckpointWork { input_rows: Some(0), output_rows: Some(0.0) }, sibling));
}

#[test]
fn expensive_parent_does_not_hide_an_inexpensive_feedback_candidate() {
    let subtree = join(scan("large", 1_000_000), scan("tiny", 25));
    let mut known = scan("known", 1);
    if let LogicalPlan::Scan { filters, .. } = &mut known { filters.clear(); }
    let plan = join(subtree, known);
    let boundary = StatisticsCheckpointPolicy.choose_for_test(&plan).unwrap();
    assert_eq!(boundary.table_name(), "tiny");
}

#[test]
fn checkpoint_cost_model_is_injectable_without_changing_safety_rules() {
    struct Decline;
    impl CheckpointCostModel for Decline {
        fn allows(&self, _: CheckpointWork, _: CheckpointWork) -> bool { false }
    }
    let policy = StatisticsCheckpointPolicy::with_cost_model(Decline);
    let statistics = Arc::new(QueryStatisticsContext::new([]));
    assert!(policy.choose_boundary(&join(scan("a", 10), scan("b", 100)), statistics).is_none());
}
