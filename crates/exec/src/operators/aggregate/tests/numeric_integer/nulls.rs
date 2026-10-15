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
    let mut sink = AggregateSink::new(plan).unwrap();

    sink.accumulate(DataChunk::from_vectors(vec![
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
        row.column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        2
    );
    assert_eq!(
        row.column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        8
    );
    assert_eq!(
        row.column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        3
    );
    assert_eq!(
        row.column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        5
    );
    assert_eq!(
        row.column(4)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0),
        4.0
    );
}
