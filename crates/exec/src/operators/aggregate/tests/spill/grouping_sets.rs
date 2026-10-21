use super::*;

#[test]
fn aggregate_spill_preserves_null_keys_and_grouping_set_identity() {
    let plan = AggregatePlan {
        groups: 1,
        estimated_groups: None,
        grouping_sets: Some(vec![vec![0], vec![]]),
        emit_empty_group_on_empty_input: false,
        aggs: vec![AggOpCode::Count],
        aggregate_input_types: vec![VeinDataType::Int64],
        types: vec![VeinDataType::Int64, VeinDataType::Int64],
    };
    let tracker = QueryMemoryTracker::new(
        32 * 1024,
        common::cancellation::CancellationToken::new(),
    );
    let mut sink = AggregateSink::with_memory_tracker(plan, tracker)
        .expect("create budgeted grouping-set aggregate");
    let input_rows = 2048_i64;
    let keys = (0..input_rows)
        .map(|row| (row % 100 != 0).then_some(row % 257))
        .collect::<Vec<_>>();
    sink.accumulate(crate::pipeline::test_support::dense_view(vec![
        Arc::new(Int64Array::from(keys.clone())) as ArrayRef,
        Arc::new(Int64Array::from_iter_values(0..input_rows)) as ArrayRef,
    ]))
        .expect("spill grouping-set state");
    assert!(!sink.spill_runs.is_empty());

    let mut null_key_counts = Vec::new();
    sink.finish_with(AGGREGATE_BATCH_ROWS, |batch| {
        let keys = crate::pipeline::test_support::materialized_column::<Int64Array>(&batch, 0);
        let values = crate::pipeline::test_support::materialized_column::<Int64Array>(&batch, 1);
        for row in 0..batch.num_rows() {
            if keys.is_null(row) {
                null_key_counts.push(values.value(row));
            }
        }
        Ok(())
    })
        .expect("merge grouping-set spill runs");
    null_key_counts.sort_unstable();
    let null_rows = keys.iter().filter(|key| key.is_none()).count() as i64;
    assert_eq!(null_key_counts, [null_rows, input_rows]);
}
