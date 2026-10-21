use super::*;

#[test]
fn aggregate_spill_flushes_retries_and_merges_repeated_keys() {
    let plan = AggregatePlan {
        groups: 1,
        estimated_groups: None,
        grouping_sets: None,
        emit_empty_group_on_empty_input: false,
        aggs: vec![AggOpCode::Count],
        aggregate_input_types: vec![VeinDataType::Int64],
        types: vec![VeinDataType::Int64, VeinDataType::Int64],
    };
    let tracker = QueryMemoryTracker::new(
        32 * 1024,
        common::cancellation::CancellationToken::new(),
    );
    let mut sink = AggregateSink::with_memory_tracker(plan, tracker.clone())
        .expect("create budgeted aggregate");
    let input_rows = 2048_i64;
    let keys = (0..input_rows).map(|row| row % 257).collect::<Vec<_>>();
    let values = (0..input_rows).collect::<Vec<_>>();
    sink.accumulate(crate::pipeline::test_support::dense_view(vec![
        Arc::new(Int64Array::from(keys.clone())) as ArrayRef,
        Arc::new(Int64Array::from(values)) as ArrayRef,
    ]))
        .expect("large input selection is split and retried after spill");
    assert!(
        !sink.spill_runs.is_empty(),
        "the small tracker budget must externalize groups"
    );
    assert!(
        tracker.spill_bytes_used() > 0,
        "aggregate runs consume query spill budget"
    );

    let mut counts = std::collections::BTreeMap::new();
    sink.finish_with(AGGREGATE_BATCH_ROWS, |batch| {
        let keys = crate::pipeline::test_support::materialized_column::<Int64Array>(&batch, 0);
        let values = crate::pipeline::test_support::materialized_column::<Int64Array>(&batch, 1);
        for row in 0..batch.num_rows() {
            counts.insert(keys.value(row), values.value(row));
        }
        Ok(())
    })
        .expect("merge spill runs");
    assert_eq!(tracker.spill_bytes_used(), 0);
    let expected = keys.into_iter().fold(
        std::collections::BTreeMap::new(),
        |mut counts, key| {
            *counts.entry(key).or_insert(0_i64) += 1;
            counts
        },
    );
    assert_eq!(counts, expected);
    assert!(tracker.peak() <= tracker.limit());
}
