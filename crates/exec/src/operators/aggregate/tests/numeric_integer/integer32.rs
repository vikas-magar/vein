//! Aggregate integer32 regression coverage.

use super::*;

#[test]
fn int32_aggregates_widen_sum_and_preserve_min_max_layout() {
    let values: ArrayRef = Arc::new(Int32Array::from(vec![Some(1), None, Some(3)]));
    let plan = AggregatePlan {
        groups: 0,
        estimated_groups: None,
        grouping_sets: None,
        emit_empty_group_on_empty_input: true,
        aggs: vec![
            AggOpCode::Sum,
            AggOpCode::Min,
            AggOpCode::Max,
            AggOpCode::Avg,
        ],
        aggregate_input_types: vec![VeinDataType::Int32; 4],
        types: vec![
            VeinDataType::Int64,
            VeinDataType::Int32,
            VeinDataType::Int32,
            VeinDataType::Float64,
        ],
    };
    let mut sink = AggregateSink::new(plan).unwrap();

    sink.accumulate(crate::pipeline::test_support::dense_view(vec![
        values.clone(),
        values.clone(),
        values.clone(),
        values,
    ]))
        .unwrap();
    let output = sink.finish().unwrap();

    assert_eq!(
        crate::pipeline::test_support::materialized_column::<Int64Array>(&output[0], 0)
            .value(0),
        4
    );
    assert_eq!(
        crate::pipeline::test_support::materialized_column::<Int32Array>(&output[0], 1)
            .value(0),
        1
    );
    assert_eq!(
        crate::pipeline::test_support::materialized_column::<Int32Array>(&output[0], 2)
            .value(0),
        3
    );
    assert_eq!(
        crate::pipeline::test_support::materialized_column::<Float64Array>(&output[0], 3)
            .value(0),
        2.0
    );
}
