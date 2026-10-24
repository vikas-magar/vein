//! Aggregate nulls regression coverage.

use super::*;

#[test]
fn null_inputs_are_ignored_and_count_only_counts_non_null_values() {
    let values: ArrayRef = Arc::new(Int64Array::from(vec![Some(3), None, Some(5)]));
    let plan = AggregatePlan {
        groups: 0,
        estimated_groups: None,
        grouping_sets: None,
        emit_empty_group_on_empty_input: true,
        aggs: vec![
            AggOpCode::Count,
            AggOpCode::Sum,
            AggOpCode::Min,
            AggOpCode::Max,
            AggOpCode::Avg,
        ],
        aggregate_input_types: vec![VeinDataType::Int64; 5],
        types: vec![
            VeinDataType::Int64,
            VeinDataType::Int64,
            VeinDataType::Int64,
            VeinDataType::Int64,
            VeinDataType::Float64,
        ],
    };
    let mut sink = AggregateSink::new(inferred_results(plan)).unwrap();

    sink.accumulate(crate::pipeline::test_support::dense_view(vec![
        Arc::new(Int64Array::from(vec![Some(1), None, Some(1)])),
        values.clone(),
        values.clone(),
        values.clone(),
        values,
    ]))
        .unwrap();
    let output = sink.finish().unwrap();
    let row = &output[0];

    assert_eq!(
        crate::pipeline::test_support::materialized_column::<Int64Array>(row, 0)
            .value(0),
        2
    );
    assert_eq!(
        int128_column(row, 1)
            .value(0),
        8
    );
    assert_eq!(
        crate::pipeline::test_support::materialized_column::<Int64Array>(row, 2)
            .value(0),
        3
    );
    assert_eq!(
        crate::pipeline::test_support::materialized_column::<Int64Array>(row, 3)
            .value(0),
        5
    );
    assert_eq!(
        crate::pipeline::test_support::materialized_column::<Float64Array>(row, 4)
            .value(0),
        4.0
    );
}
