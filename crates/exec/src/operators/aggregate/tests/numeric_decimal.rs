//! numeric decimal regression tests for hash aggregation.

use super::*;

#[test]
fn decimal256_sum_and_extrema_preserve_wide_values_and_nulls() {
    let large = i256::from(10_i8).checked_pow(40).unwrap();
    let input = Decimal256Array::from(vec![Some(large), None, Some(i256::ONE)])
        .with_precision_and_scale(76, 0)
        .unwrap();
    let plan = AggregatePlan {
        groups: 0,
        estimated_groups: None,
        grouping_sets: None,
        emit_empty_group_on_empty_input: true,
        aggs: vec![AggOpCode::Sum, AggOpCode::Min, AggOpCode::Max],
        aggregate_input_types: vec![VeinDataType::Decimal256(76, 0); 3],
        types: vec![VeinDataType::Decimal256(76, 0); 3],
    };
    let mut sink = AggregateSink::new(plan).unwrap();
    sink.accumulate(DataChunk::from_vectors(vec![
        Arc::new(input.clone()),
        Arc::new(input.clone()),
        Arc::new(input),
    ]))
        .unwrap();
    let output = sink.finish().unwrap();

    let sum = output[0]
        .column(0)
        .as_any()
        .downcast_ref::<Decimal256Array>()
        .unwrap();
    let minimum = output[0]
        .column(1)
        .as_any()
        .downcast_ref::<Decimal256Array>()
        .unwrap();
    let maximum = output[0]
        .column(2)
        .as_any()
        .downcast_ref::<Decimal256Array>()
        .unwrap();
    assert_eq!(sum.value(0), large.checked_add(i256::ONE).unwrap());
    assert_eq!(minimum.value(0), i256::ONE);
    assert_eq!(maximum.value(0), large);
}

#[test]
fn decimal256_average_accumulates_exactly_across_batches_and_applies_scale() {
    let large = i256::from(10_i8).checked_pow(40).unwrap();
    let negative_large = i256::ZERO.checked_sub(large).unwrap();
    let plan = AggregatePlan {
        groups: 0,
        estimated_groups: None,
        grouping_sets: None,
        emit_empty_group_on_empty_input: true,
        aggs: vec![AggOpCode::Avg],
        aggregate_input_types: vec![VeinDataType::Decimal256(76, 2)],
        types: vec![VeinDataType::Float64],
    };
    let mut sink = AggregateSink::new(plan).unwrap();
    for value in [large, i256::ONE, negative_large] {
        let input = Decimal256Array::from(vec![value])
            .with_precision_and_scale(76, 2)
            .unwrap();
        sink.accumulate(DataChunk::from_vectors(vec![Arc::new(input)]))
            .unwrap();
    }
    let output = sink.finish().unwrap();
    let average = output[0]
        .column(0)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(average.value(0), 1.0 / 300.0);
}

#[test]
fn decimal256_sum_enforces_76_digit_precision() {
    let max_decimal_76 = i256::from(10_i8)
        .checked_pow(76)
        .unwrap()
        .checked_sub(i256::ONE)
        .unwrap();
    let input = Decimal256Array::from(vec![max_decimal_76, i256::ONE])
        .with_precision_and_scale(76, 0)
        .unwrap();
    let plan = AggregatePlan {
        groups: 0,
        estimated_groups: None,
        grouping_sets: None,
        emit_empty_group_on_empty_input: true,
        aggs: vec![AggOpCode::Sum],
        aggregate_input_types: vec![VeinDataType::Decimal256(76, 0)],
        types: vec![VeinDataType::Decimal256(76, 0)],
    };
    let mut sink = AggregateSink::new(plan).unwrap();
    let error = sink
        .accumulate(DataChunk::from_vectors(vec![Arc::new(input)]))
        .unwrap_err();
    assert!(error.to_string().contains("overflow while computing SUM"));
}

#[test]
fn decimal_sum_preserves_unscaled_integer_and_scale() {
    let input = Decimal128Array::from(vec![123_456_789_012_345_678_i128, 1])
        .with_precision_and_scale(38, 2)
        .unwrap();
    let plan = AggregatePlan {
        groups: 0,
        estimated_groups: None,
        grouping_sets: None,
        emit_empty_group_on_empty_input: true,
        aggs: vec![AggOpCode::Sum],
        aggregate_input_types: vec![VeinDataType::Decimal128(38, 2)],
        types: vec![VeinDataType::Decimal128(38, 2)],
    };
    let mut sink = AggregateSink::new(plan).unwrap();

    sink.accumulate(DataChunk::from_vectors(vec![Arc::new(input)]))
        .unwrap();
    let output = sink.finish().unwrap();
    let sum = output[0]
        .column(0)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();

    assert_eq!(sum.value(0), 123_456_789_012_345_679_i128);
    assert_eq!(sum.precision(), 38);
    assert_eq!(sum.scale(), 2);
}

#[test]
fn decimal_average_applies_the_input_scale() {
    let input = Decimal128Array::from(vec![1_250_i128, 2_250])
        .with_precision_and_scale(15, 2)
        .unwrap();
    let plan = AggregatePlan {
        groups: 0,
        estimated_groups: None,
        grouping_sets: None,
        emit_empty_group_on_empty_input: true,
        aggs: vec![AggOpCode::Avg],
        aggregate_input_types: vec![VeinDataType::Decimal128(15, 2)],
        types: vec![VeinDataType::Float64],
    };
    let mut sink = AggregateSink::new(plan).unwrap();

    sink.accumulate(DataChunk::from_vectors(vec![Arc::new(input)]))
        .unwrap();
    let output = sink.finish().unwrap();
    let average = output[0]
        .column(0)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();

    assert_eq!(average.value(0), 17.5);
}

#[test]
fn decimal_average_accumulates_unscaled_values_exactly() {
    let input = Decimal128Array::from(vec![
        9_007_199_254_740_993_i128,
        -9_007_199_254_740_992_i128,
    ])
        .with_precision_and_scale(19, 0)
        .unwrap();
    let plan = AggregatePlan {
        groups: 0,
        estimated_groups: None,
        grouping_sets: None,
        emit_empty_group_on_empty_input: true,
        aggs: vec![AggOpCode::Avg],
        aggregate_input_types: vec![VeinDataType::Decimal128(19, 0)],
        types: vec![VeinDataType::Float64],
    };
    let mut sink = AggregateSink::new(plan).unwrap();

    sink.accumulate(DataChunk::from_vectors(vec![Arc::new(input)]))
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
fn decimal_sum_reports_precision_overflow() {
    let max_decimal_38 = 10_i128.pow(38) - 1;
    let input = Decimal128Array::from(vec![max_decimal_38, 1])
        .with_precision_and_scale(38, 0)
        .unwrap();
    let plan = AggregatePlan {
        groups: 0,
        estimated_groups: None,
        grouping_sets: None,
        emit_empty_group_on_empty_input: true,
        aggs: vec![AggOpCode::Sum],
        aggregate_input_types: vec![VeinDataType::Decimal128(38, 0)],
        types: vec![VeinDataType::Decimal128(38, 0)],
    };
    let mut sink = AggregateSink::new(plan).unwrap();

    let error = sink
        .accumulate(DataChunk::from_vectors(vec![Arc::new(input)]))
        .unwrap_err();
    assert!(error.to_string().contains("overflow while computing SUM"));
}
