//! Aggregate integer small regression coverage.

use super::*;

#[test]
fn int8_and_int16_aggregates_share_int64_state() {
    let int8_values: ArrayRef = Arc::new(Int8Array::from(vec![Some(100), None, Some(-10)]));
    let int16_values: ArrayRef = Arc::new(Int16Array::from(vec![Some(1_000), None, Some(-10)]));
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
            AggOpCode::Sum,
            AggOpCode::Min,
            AggOpCode::Max,
            AggOpCode::Avg,
        ],
        aggregate_input_types: vec![
            VeinDataType::Int8,
            VeinDataType::Int8,
            VeinDataType::Int8,
            VeinDataType::Int8,
            VeinDataType::Int16,
            VeinDataType::Int16,
            VeinDataType::Int16,
            VeinDataType::Int16,
        ],
        types: vec![
            VeinDataType::Int64,
            VeinDataType::Int8,
            VeinDataType::Int8,
            VeinDataType::Float64,
            VeinDataType::Int64,
            VeinDataType::Int16,
            VeinDataType::Int16,
            VeinDataType::Float64,
        ],
    };
    let mut sink = AggregateSink::new(inferred_results(plan)).unwrap();

    sink.accumulate(crate::pipeline::test_support::dense_view(vec![
        int8_values.clone(),
        int8_values.clone(),
        int8_values.clone(),
        int8_values,
        int16_values.clone(),
        int16_values.clone(),
        int16_values.clone(),
        int16_values,
    ]))
        .unwrap();
    let output = sink.finish().unwrap();

    assert_eq!(
        int128_column(&output[0], 0)
            .value(0),
        90
    );
    assert_eq!(
        crate::pipeline::test_support::materialized_column::<Int8Array>(&output[0], 1)
            .value(0),
        -10
    );
    assert_eq!(
        crate::pipeline::test_support::materialized_column::<Int8Array>(&output[0], 2)
            .value(0),
        100
    );
    assert_eq!(
        crate::pipeline::test_support::materialized_column::<Float64Array>(&output[0], 3)
            .value(0),
        45.0
    );
    assert_eq!(
        int128_column(&output[0], 4)
            .value(0),
        990
    );
    assert_eq!(
        crate::pipeline::test_support::materialized_column::<Int16Array>(&output[0], 5)
            .value(0),
        -10
    );
    assert_eq!(
        crate::pipeline::test_support::materialized_column::<Int16Array>(&output[0], 6)
            .value(0),
        1_000
    );
    assert_eq!(
        crate::pipeline::test_support::materialized_column::<Float64Array>(&output[0], 7)
            .value(0),
        495.0
    );
}
