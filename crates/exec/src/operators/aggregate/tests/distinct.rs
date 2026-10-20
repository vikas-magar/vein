//! COUNT(DISTINCT) correctness, fan-in, and bounded spill coverage.

use super::*;
use std::collections::BTreeMap;

fn distinct_plan(grouping_sets: Option<Vec<Vec<u32>>>) -> AggregatePlan {
    AggregatePlan {
        groups: 1,
        estimated_groups: None,
        grouping_sets,
        emit_empty_group_on_empty_input: true,
        aggs: vec![AggOpCode::CountDistinct],
        aggregate_input_types: vec![VeinDataType::Int64],
        types: vec![VeinDataType::Int64, VeinDataType::Int64],
    }
}

#[test]
fn count_distinct_handles_duplicates_nulls_and_multiple_groups() {
    let mut plan = distinct_plan(None);
    plan.aggs.insert(0, AggOpCode::Count);
    plan.aggregate_input_types.insert(0, VeinDataType::Int64);
    plan.types.insert(1, VeinDataType::Int64);
    let mut sink = AggregateSink::new(plan).expect("create mixed aggregate");
    sink.accumulate(crate::pipeline::test_support::dense_view(vec![
        Arc::new(Int64Array::from(vec![Some(1), Some(1), None, None, Some(2)])) as ArrayRef,
        Arc::new(Int64Array::from(vec![Some(1), Some(1), Some(1), Some(1), None])) as ArrayRef,
        Arc::new(Int64Array::from(vec![Some(7), Some(7), Some(8), Some(8), None])) as ArrayRef,
    ]))
        .expect("accumulate duplicate and NULL values");

    let mut actual = BTreeMap::new();
    for batch in sink.finish().expect("finish COUNT DISTINCT") {
        let keys = materialized_column::<Int64Array>(&batch, 0);
        let counts = materialized_column::<Int64Array>(&batch, 1);
        let distinct = materialized_column::<Int64Array>(&batch, 2);
        for row in 0..batch.num_rows() {
            actual.insert(
                keys.is_valid(row).then(|| keys.value(row)),
                (counts.value(row), distinct.value(row)),
            );
        }
    }
    assert_eq!(actual, BTreeMap::from([(None, (2, 1)), (Some(1), (2, 1)), (Some(2), (0, 0))]));
}

#[test]
fn multiple_count_distinct_expressions_keep_independent_states() {
    let plan = AggregatePlan {
        groups: 1,
        estimated_groups: None,
        grouping_sets: None,
        emit_empty_group_on_empty_input: true,
        aggs: vec![AggOpCode::CountDistinct, AggOpCode::CountDistinct],
        aggregate_input_types: vec![VeinDataType::Int64, VeinDataType::Int64],
        types: vec![VeinDataType::Int64, VeinDataType::Int64, VeinDataType::Int64],
    };
    let mut sink = AggregateSink::new(plan).expect("create multiple distinct aggregate");
    sink.accumulate(crate::pipeline::test_support::dense_view(vec![
        Arc::new(Int64Array::from(vec![1, 1, 1, 2, 2, 2])) as ArrayRef,
        Arc::new(Int64Array::from(vec![Some(7), Some(7), Some(8), Some(7), None, Some(9)]))
            as ArrayRef,
        Arc::new(Int64Array::from(vec![Some(3), Some(4), Some(3), Some(3), Some(3), Some(3)]))
            as ArrayRef,
    ]))
        .expect("accumulate independent distinct arguments");

    let mut actual = BTreeMap::new();
    for batch in sink.finish().expect("finish multiple distinct aggregate") {
        let groups = materialized_column::<Int64Array>(&batch, 0);
        let first = materialized_column::<Int64Array>(&batch, 1);
        let second = materialized_column::<Int64Array>(&batch, 2);
        for row in 0..batch.num_rows() {
            actual.insert(groups.value(row), (first.value(row), second.value(row)));
        }
    }
    assert_eq!(actual, BTreeMap::from([(1, (2, 2)), (2, (2, 1))]));
}

#[test]
fn count_distinct_preserves_grouping_set_identity_for_null_keys() {
    let mut sink = AggregateSink::new(distinct_plan(Some(vec![vec![0], vec![]])))
        .expect("create grouping-set distinct aggregate");
    sink.accumulate(crate::pipeline::test_support::dense_view(vec![
        Arc::new(Int64Array::from(vec![None, Some(4), None])) as ArrayRef,
        Arc::new(Int64Array::from(vec![Some(7), Some(8), Some(7)])) as ArrayRef,
    ]))
        .expect("accumulate grouping sets");

    let mut actual = Vec::new();
    for batch in sink.finish().expect("finish grouping sets") {
        let keys = materialized_column::<Int64Array>(&batch, 0);
        let counts = materialized_column::<Int64Array>(&batch, 1);
        for row in 0..batch.num_rows() {
            actual.push((keys.is_valid(row).then(|| keys.value(row)), counts.value(row)));
        }
    }
    actual.sort_unstable();
    assert_eq!(actual, vec![(None, 1), (None, 2), (Some(4), 1)]);
}

#[test]
fn count_distinct_spills_and_merges_exact_unique_pairs_within_budget() {
    let tracker = QueryMemoryTracker::new(
        32 * 1024,
        common::cancellation::CancellationToken::new(),
    );
    let mut sink = AggregateSink::with_memory_tracker(distinct_plan(None), tracker.clone())
        .expect("create memory-bounded COUNT DISTINCT");
    let rows = 2048_i64;
    let batch_rows = 128_i64;
    for start in (0..rows).step_by(batch_rows as usize) {
        let end = (start + batch_rows).min(rows);
        sink.accumulate(crate::pipeline::test_support::dense_view(vec![
            Arc::new(Int64Array::from_iter_values((start..end).map(|row| row % 67))) as ArrayRef,
            Arc::new(Int64Array::from_iter_values((start..end).map(|row| row % 13))) as ArrayRef,
        ]))
            .expect("spill unique pairs across bounded input batches");
    }
    assert!(
        sink.distinct_collectors
            .iter()
            .any(|collector| !collector.sink.spill_runs.is_empty()),
        "distinct key state must use the existing spill path"
    );
    assert!(tracker.peak() <= tracker.limit());

    let output = sink.finish().expect("merge exact distinct spill state");
    let mut counts = BTreeMap::new();
    for batch in output {
        let keys = materialized_column::<Int64Array>(&batch, 0);
        let values = materialized_column::<Int64Array>(&batch, 1);
        for row in 0..batch.num_rows() {
            counts.insert(keys.value(row), values.value(row));
        }
    }
    assert_eq!(counts.len(), 67);
    assert!(counts.values().all(|count| *count == 13));
}

#[test]
fn worker_local_distinct_states_deduplicate_again_during_fan_in() {
    let tracker = QueryMemoryTracker::unlimited(
        common::cancellation::CancellationToken::new(),
    );
    let (aggregate, mut canonical) =
        shared_with_memory_tracker(distinct_plan(None), tracker).expect("create aggregate");
    let workers = (0..2)
        .map(|_| canonical.fork_local().unwrap().unwrap())
        .collect::<Vec<_>>();
    for (mut worker, values) in workers.into_iter().zip([vec![7, 7], vec![7, 9]]) {
        worker
            .push(crate::pipeline::test_support::dense_view(vec![
                Arc::new(Int64Array::from(vec![1, 1])) as ArrayRef,
                Arc::new(Int64Array::from(values)) as ArrayRef,
            ]))
            .expect("accumulate worker-local distinct values");
        worker
            .combine_local(canonical.as_mut())
            .expect("combine worker-local distinct state");
    }

    let output = aggregate.lock().unwrap().finish().expect("finish fan-in");
    let count = materialized_column::<Int64Array>(&output[0], 1);
    assert_eq!(count.value(0), 2);
}
