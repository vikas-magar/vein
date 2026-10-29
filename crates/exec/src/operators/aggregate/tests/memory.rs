//! memory regression tests for hash aggregation.

use super::*;

#[tokio::test]
async fn estimated_group_count_reserves_index_before_accumulation() {
    let plan = AggregatePlan {
        groups: 1,
        estimated_groups: Some(4),
        grouping_sets: None,
        emit_empty_group_on_empty_input: true,
        aggs: vec![AggOpCode::Count],
        aggregate_input_types: vec![VeinDataType::Int64],
        types: vec![VeinDataType::Int64, VeinDataType::Int64],
    };
    let mut sink = AggregateSink::new(plan).expect("create aggregate sink");
    sink.accumulate(crate::pipeline::test_support::dense_view(vec![
        Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
        Arc::new(Int64Array::from(vec![1, 1, 1, 1])),
    ]))
        .expect("accumulate groups");

    assert_eq!(sink.metrics().group_index_capacity_growth_events, 0);
    let output = sink.finish().await.expect("finish aggregate");
    assert_eq!(output[0].num_rows(), 4);
}

#[tokio::test]
async fn first_batch_candidate_bound_reserves_index_without_group_estimate() {
    let plan = AggregatePlan {
        groups: 1,
        estimated_groups: None,
        grouping_sets: None,
        emit_empty_group_on_empty_input: true,
        aggs: vec![AggOpCode::Count],
        aggregate_input_types: vec![VeinDataType::Int64],
        types: vec![VeinDataType::Int64, VeinDataType::Int64],
    };
    let mut sink = AggregateSink::new(plan).expect("create aggregate sink");
    sink.accumulate(crate::pipeline::test_support::dense_view(vec![
        Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
        Arc::new(Int64Array::from(vec![1, 1, 1, 1])),
    ]))
        .expect("accumulate first batch");

    let metrics = sink.metrics();
    assert!(metrics.group_index_capacity >= 4);
    assert!(metrics.group_index_reserve_ns > 0);
    assert_eq!(metrics.group_index_capacity_growth_events, 0);
    assert_eq!(metrics.new_groups, 4);

    let output = sink.finish().await.expect("finish aggregate");
    assert_eq!(output[0].num_rows(), 4);
}

#[test]
fn aggregate_group_growth_respects_budget_and_releases_on_error() {
    let plan = AggregatePlan {
        groups: 1,
        estimated_groups: None,
        grouping_sets: None,
        emit_empty_group_on_empty_input: false,
        aggs: vec![AggOpCode::Count],
        aggregate_input_types: vec![VeinDataType::Int64],
        types: vec![VeinDataType::Int64, VeinDataType::Int64],
    };
    let tracker = QueryMemoryTracker::new(0, common::cancellation::CancellationToken::new());
    let mut sink = AggregateSink::with_memory_tracker(plan, tracker.clone())
        .expect("empty aggregate state fits zero bytes");
    let error = sink
        .accumulate(crate::pipeline::test_support::dense_view(vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(Int64Array::from(vec![1])),
        ]))
        .expect_err("creating a group must reserve memory first");
    assert!(matches!(error, VeinError::ResourceExhausted { .. }));
    drop(sink);
    assert_eq!(
        tracker.used(),
        0,
        "failed aggregate state releases reservations"
    );
}

#[test]
fn failed_key_array_reservation_rolls_back_group_index_and_state() {
    let plan = AggregatePlan {
        groups: 1,
        estimated_groups: None,
        grouping_sets: None,
        emit_empty_group_on_empty_input: false,
        aggs: vec![AggOpCode::Count],
        aggregate_input_types: vec![VeinDataType::Int64],
        types: vec![VeinDataType::Int64, VeinDataType::Int64],
    };
    let key_array = Arc::new(Int64Array::from(vec![10]));
    let key_bytes = key_array.get_array_memory_size();
    let baseline_tracker =
        QueryMemoryTracker::unlimited(common::cancellation::CancellationToken::new());
    let mut baseline =
        AggregateSink::with_memory_tracker(plan.clone(), baseline_tracker.clone())
            .expect("create baseline sink");
    baseline
        .accumulate(crate::pipeline::test_support::dense_view(vec![
            Arc::clone(&key_array) as ArrayRef,
            Arc::new(Int64Array::from(vec![1])) as ArrayRef,
        ]))
        .expect("create baseline group");
    let group_state_bytes = baseline_tracker.used() - key_bytes;
    drop(baseline);

    let tracker = QueryMemoryTracker::new(
        group_state_bytes + key_bytes - 1,
        common::cancellation::CancellationToken::new(),
    );
    let mut sink = AggregateSink::with_memory_tracker(plan, tracker.clone())
        .expect("create budget-limited sink");
    let error = sink
        .accumulate(crate::pipeline::test_support::dense_view(vec![
            key_array as ArrayRef,
            Arc::new(Int64Array::from(vec![1])) as ArrayRef,
        ]))
        .expect_err("retaining the key array exceeds the remaining budget");

    assert!(matches!(error, VeinError::ResourceExhausted { .. }));
    assert_eq!(sink.group_count(), 0);
    assert_eq!(
        sink.index.len(),
        0,
        "failed batch leaves no dangling index entry"
    );
    assert!(sink.group_key_batches.is_empty());
    assert_eq!(
        tracker.used(),
        0,
        "failed batch releases its partial reservation"
    );
}

#[test]
fn later_group_budget_failure_rolls_back_earlier_groups_in_the_batch() {
    let plan = AggregatePlan {
        groups: 1,
        estimated_groups: None,
        grouping_sets: None,
        emit_empty_group_on_empty_input: false,
        aggs: vec![AggOpCode::Count],
        aggregate_input_types: vec![VeinDataType::Int64],
        types: vec![VeinDataType::Int64, VeinDataType::Int64],
    };
    let key_array = Arc::new(Int64Array::from(vec![10, 20]));
    let key_bytes = key_array.get_array_memory_size();
    let baseline_tracker =
        QueryMemoryTracker::unlimited(common::cancellation::CancellationToken::new());
    let mut baseline =
        AggregateSink::with_memory_tracker(plan.clone(), baseline_tracker.clone())
            .expect("create baseline sink");
    baseline
        .accumulate(crate::pipeline::test_support::selected_view(
            vec![
                Arc::clone(&key_array) as ArrayRef,
                Arc::new(Int64Array::from(vec![1, 1])) as ArrayRef,
            ],
            SelectionVector::new_sparse(vec![0]),
        ))
        .expect("create baseline group");
    let one_group_bytes = baseline_tracker.used() - key_bytes;
    drop(baseline);

    let tracker = QueryMemoryTracker::new(
        one_group_bytes,
        common::cancellation::CancellationToken::new(),
    );
    let mut sink = AggregateSink::with_memory_tracker(plan, tracker.clone())
        .expect("create budget-limited sink");
    let error = sink
        .accumulate(crate::pipeline::test_support::dense_view(vec![
            key_array,
            Arc::new(Int64Array::from(vec![1, 1])),
        ]))
        .expect_err("second distinct group exceeds the budget");

    assert!(matches!(error, VeinError::ResourceExhausted { .. }));
    assert_eq!(sink.group_count(), 0);
    assert_eq!(
        sink.index.len(),
        0,
        "earlier candidates from the batch are rolled back"
    );
    assert!(sink.group_key_batches.is_empty());
    assert_eq!(
        tracker.used(),
        0,
        "batch-local group state is released on failure"
    );
}
