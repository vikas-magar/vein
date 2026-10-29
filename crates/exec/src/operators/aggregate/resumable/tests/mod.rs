use super::super::{AggregateSink, AGGREGATE_BATCH_ROWS};
use crate::pipeline::resumable::OperatorStep;
use arrow::array::{ArrayRef, Int64Array};
use common::protocol::{AggOpCode, AggregatePlan};
use std::sync::Arc;
use types::VeinDataType;

#[test]
fn aggregate_output_cursor_yields_bounded_batches_and_preserves_results() {
    let plan = AggregatePlan {
        groups: 0,
        estimated_groups: None,
        grouping_sets: None,
        emit_empty_group_on_empty_input: true,
        aggs: vec![AggOpCode::Count],
        aggregate_input_types: vec![VeinDataType::Int64],
        types: vec![VeinDataType::Int64],
    };
    let mut sink = AggregateSink::new(plan).expect("create global count");
    let values: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3]));
    sink.accumulate(crate::pipeline::test_support::dense_view(vec![values]))
        .expect("accumulate rows");
    let mut cursor = sink.take_output_cursor(AGGREGATE_BATCH_ROWS).expect("create output cursor");

    let mut rows = 0;
    loop {
        match cursor.step().expect("advance bounded aggregate step") {
            OperatorStep::Output(batch) => {
                assert!(batch.num_rows() <= AGGREGATE_BATCH_ROWS.get());
                let count = crate::pipeline::test_support::materialized_column::<Int64Array>(&batch, 0);
                assert_eq!(count.value(0), 3);
                rows += batch.num_rows();
            }
            OperatorStep::Runnable => {}
            OperatorStep::Finished => break,
            _ => panic!("aggregate output cursor does not block or request input"),
        }
    }
    assert_eq!(rows, 1);
}
