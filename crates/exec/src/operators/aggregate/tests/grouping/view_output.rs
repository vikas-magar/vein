//! Aggregate output preserves source-backed grouping keys.

use std::collections::BTreeMap;
use std::num::NonZeroUsize;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array};
use common::cancellation::CancellationToken;
use super::*;

#[test]
fn aggregate_view_output_reuses_key_array_and_retains_its_memory_lease() {
    let keys: ArrayRef = Arc::new(Int64Array::from(vec![10, 20, 10, 30]));
    let values: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3, 4]));
    let tracker = QueryMemoryTracker::unlimited(CancellationToken::new());
    let plan = AggregatePlan {
        groups: 1,
        estimated_groups: None,
        grouping_sets: None,
        emit_empty_group_on_empty_input: false,
        aggs: vec![AggOpCode::Sum],
        aggregate_input_types: vec![VeinDataType::Int64],
        types: vec![VeinDataType::Int64, VeinDataType::Int64],
    };
    let input = crate::pipeline::test_support::dense_view(vec![
        Arc::clone(&keys),
        values,
    ]);
    let mut sink = AggregateSink::with_memory_tracker(plan, tracker.clone()).expect("sink");
    sink.accumulate(input).expect("accumulate view");

    let mut output = Vec::new();
    sink.finish_with(NonZeroUsize::new(8).unwrap(), |view| {
        output.push(view);
        Ok(())
    })
        .expect("finish as views");
    assert_eq!(output.len(), 1);
    assert!(output[0]
        .source_arrays_for_column(0)
        .unwrap()
        .iter()
        .any(|array| Arc::ptr_eq(array, &keys)));

    let rows = materialized_column::<Int64Array>(&output[0], 0);
    let sums = materialized_column::<Int64Array>(&output[0], 1);
    let actual = (0..output[0].num_rows())
        .map(|row| (rows.value(row), sums.value(row)))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(actual, BTreeMap::from([(10, 4), (20, 2), (30, 4)]));

    drop(sink);
    assert!(tracker.used() > 0, "the emitted view keeps the source lease alive");
    drop(output);
    assert_eq!(tracker.used(), 0, "the source lease releases with the output view");
}
