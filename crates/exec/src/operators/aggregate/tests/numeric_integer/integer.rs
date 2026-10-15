//! Aggregate integer regression coverage.

use super::*;

#[test]
fn integer_sum_preserves_values_above_f64_precision() {
    let plan = AggregatePlan {
        groups: 0,
        estimated_groups: None,
        grouping_sets: None,
        emit_empty_group_on_empty_input: true,
        aggs: vec![AggOpCode::Sum, AggOpCode::Min, AggOpCode::Max],
        aggregate_input_types: vec![VeinDataType::Int64; 3],
        types: vec![VeinDataType::Int64; 3],
    };
    let values: ArrayRef = Arc::new(Int64Array::from(vec![
        9_007_199_254_740_992,
        9_007_199_254_740_993,
    ]));
    let input = DataChunk::from_vectors(vec![values.clone(), values.clone(), values]);
    let mut sink = AggregateSink::new(plan).unwrap();

    sink.accumulate(input).unwrap();
    let output = sink.finish().unwrap();
    let sums = output[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let minimum = output[0]
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let maximum = output[0]
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();

    assert_eq!(sums.value(0), 18_014_398_509_481_985);
    assert_eq!(minimum.value(0), 9_007_199_254_740_992);
    assert_eq!(maximum.value(0), 9_007_199_254_740_993);
}

#[test]
fn int64_average_accumulates_exactly_across_batches() {
    let plan = AggregatePlan {
        groups: 0,
        estimated_groups: None,
        grouping_sets: None,
        emit_empty_group_on_empty_input: true,
        aggs: vec![AggOpCode::Avg],
        aggregate_input_types: vec![VeinDataType::Int64],
        types: vec![VeinDataType::Float64],
    };
    let mut sink = AggregateSink::new(plan).unwrap();

    sink.accumulate(DataChunk::from_vectors(vec![Arc::new(Int64Array::from(
        vec![9_007_199_254_740_993_i64],
    ))]))
        .unwrap();
    sink.accumulate(DataChunk::from_vectors(vec![Arc::new(Int64Array::from(
        vec![-9_007_199_254_740_992_i64],
    ))]))
        .unwrap();

    let output = sink.finish().unwrap();
    let average = output[0]
        .column(0)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(average.value(0), 0.5);
}

#[test]
fn integer_sum_reports_overflow() {
    let plan = AggregatePlan {
        groups: 0,
        estimated_groups: None,
        grouping_sets: None,
        emit_empty_group_on_empty_input: true,
        aggs: vec![AggOpCode::Sum],
        aggregate_input_types: vec![VeinDataType::Int64],
        types: vec![VeinDataType::Int64],
    };
    let input = DataChunk::from_vectors(vec![Arc::new(Int64Array::from(vec![i64::MAX, 1]))]);
    let mut sink = AggregateSink::new(plan).unwrap();

    let error = sink.accumulate(input).unwrap_err();
    assert!(error.to_string().contains("overflow while computing SUM"));
}
