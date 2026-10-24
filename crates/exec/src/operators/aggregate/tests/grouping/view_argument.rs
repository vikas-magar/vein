//! Zero-copy aggregate argument tests for mapped pipeline views.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array};
use common::cancellation::CancellationToken;
use crate::pipeline::{DataChunkView, ViewColumnRef, ViewInputRows};
use crate::operators::aggregate::input_batch::AggregateColumn;

use super::*;

#[test]
fn aggregate_view_reads_mapped_arguments_from_original_arrow_buffers() {
    let keys: ArrayRef = Arc::new(Int64Array::from(vec![1, 1, 2, 2]));
    let values: ArrayRef = Arc::new(Int64Array::from(vec![10, 20, 30, 40]));
    let key_view = crate::pipeline::test_support::dense_view(vec![Arc::clone(&keys)]);
    let value_view = crate::pipeline::test_support::dense_view(vec![Arc::clone(&values)]);
    let view = DataChunkView::combine(
        &[
            ViewInputRows {
                view: &key_view,
                rows: &[Some(0), Some(1), Some(2), Some(3)],
            },
            ViewInputRows {
                view: &value_view,
                rows: &[Some(3), Some(2), Some(1), Some(0)],
            },
        ],
        &[
            ViewColumnRef {
                input: 0,
                column: 0,
            },
            ViewColumnRef {
                input: 1,
                column: 0,
            },
        ],
    )
    .expect("mapped aggregate view");

    let batch = AggregateBatch::from_view(&view, 1, 1, &[vec![0]])
        .expect("construct aggregate input");
    let AggregateColumn::View(argument) = &batch.aggregate_columns[0];
    let sources = argument.source_arrays_for_column(0).expect("argument sources");
    assert!(sources.iter().any(|array| Arc::ptr_eq(array, &values)));

    let plan = AggregatePlan {
        groups: 1,
        estimated_groups: None,
        grouping_sets: None,
        emit_empty_group_on_empty_input: false,
        aggs: vec![AggOpCode::Sum],
        aggregate_input_types: vec![VeinDataType::Int64],
        types: vec![VeinDataType::Int64, VeinDataType::Int64],
    };
    let tracker = QueryMemoryTracker::unlimited(CancellationToken::new());
    let mut sink = AggregateSink::with_memory_tracker(inferred_results(plan), tracker).expect("aggregate sink");
    sink.accumulate(view).expect("aggregate mapped input");

    let rows = sink
        .finish()
        .expect("finish")
        .into_iter()
        .flat_map(|batch| {
            let keys = materialized_column::<Int64Array>(&batch, 0);
            let sums = int128_column(&batch, 1);
            (0..batch.num_rows())
                .map(|row| (keys.value(row), sums.value(row)))
                .collect::<Vec<_>>()
        })
        .collect::<BTreeMap<_, _>>();
    assert_eq!(rows, BTreeMap::from([(1, 70), (2, 30)]));
}
