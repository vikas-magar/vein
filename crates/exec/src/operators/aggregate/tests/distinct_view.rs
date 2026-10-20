//! Reference-backed DISTINCT aggregate input regression.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array};
use crate::pipeline::{DataChunkView, ViewColumnRef, ViewInputRows};

use super::*;

#[test]
fn count_distinct_view_keeps_source_buffers_and_ignores_null_values() {
    let groups: ArrayRef = Arc::new(Int64Array::from(vec![1, 1, 1, 2, 2, 2]));
    let values: ArrayRef =
        Arc::new(Int64Array::from(vec![Some(10), Some(10), None, Some(20), Some(30), Some(30)]));
    let group_view = crate::pipeline::test_support::dense_view(vec![Arc::clone(&groups)]);
    let value_view = crate::pipeline::test_support::dense_view(vec![Arc::clone(&values)]);
    let view = DataChunkView::combine(
        &[
            ViewInputRows {
                view: &group_view,
                rows: &[Some(0), Some(1), Some(2), Some(3), Some(4), Some(5)],
            },
            ViewInputRows {
                view: &value_view,
                rows: &[Some(0), Some(1), Some(2), Some(3), Some(4), Some(5)],
            },
        ],
        &[
            ViewColumnRef { input: 0, column: 0 },
            ViewColumnRef { input: 1, column: 0 },
        ],
    )
    .expect("distinct input view");

    let plan = AggregatePlan {
        groups: 1,
        estimated_groups: None,
        grouping_sets: None,
        emit_empty_group_on_empty_input: false,
        aggs: vec![AggOpCode::CountDistinct],
        aggregate_input_types: vec![VeinDataType::Int64],
        types: vec![VeinDataType::Int64, VeinDataType::Int64],
    };
    let mut sink = AggregateSink::new(plan).expect("distinct aggregate sink");
    sink.accumulate(view).expect("accumulate reference-backed input");

    let retained_value_source = sink.distinct_collectors[0]
        .sink
        .group_key_batches
        .iter()
        .flatten()
        .filter_map(Option::as_ref)
        .any(|column| {
            matches!(column, GroupKeyColumn::View(view)
                if view.source_arrays_for_column(0).is_ok_and(|arrays| {
                    arrays.iter().any(|array| Arc::ptr_eq(array, &values))
                }))
        });
    assert!(retained_value_source, "distinct keys retain original value arrays");

    let results = sink
        .finish()
        .expect("finish distinct aggregate")
        .into_iter()
        .flat_map(|batch| {
            let keys = materialized_column::<Int64Array>(&batch, 0);
            let counts = materialized_column::<Int64Array>(&batch, 1);
            (0..batch.num_rows())
                .map(|row| (keys.value(row), counts.value(row)))
                .collect::<Vec<_>>()
        })
        .collect::<BTreeMap<_, _>>();
    assert_eq!(results, BTreeMap::from([(1, 1), (2, 2)]));
}
