//! numeric float regression tests for hash aggregation.

use super::*;

#[test]
fn float16_aggregates_use_wide_fold_and_preserve_min_max_layout() {
    let source: ArrayRef = Arc::new(Float32Array::from(vec![Some(1.5), None, Some(2.5)]));
    let input = arrow::compute::cast(&source, &VeinDataType::Float16.to_arrow()).unwrap();
    assert!(input.as_any().is::<Float16Array>());

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
        aggregate_input_types: vec![VeinDataType::Float16; 4],
        types: vec![
            VeinDataType::Float64,
            VeinDataType::Float16,
            VeinDataType::Float16,
            VeinDataType::Float64,
        ],
    };
    let mut sink = AggregateSink::new(plan).unwrap();
    sink.accumulate(DataChunk::from_vectors(vec![
        input.clone(),
        input.clone(),
        input.clone(),
        input,
    ]))
        .unwrap();
    let output = sink.finish().unwrap();

    assert_eq!(
        output[0]
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0),
        4.0
    );
    for (column, expected) in [(1, 1.5), (2, 2.5)] {
        let value =
            arrow::compute::cast(output[0].column(column), &VeinDataType::Float32.to_arrow())
                .unwrap();
        assert_eq!(
            value
                .as_any()
                .downcast_ref::<Float32Array>()
                .unwrap()
                .value(0),
            expected
        );
    }
    assert_eq!(
        output[0]
            .column(3)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0),
        2.0
    );
}

#[test]
fn float32_average_accumulates_in_float64_state() {
    let input = Float32Array::from(vec![100_000_000.0_f32, 1.0, -100_000_000.0]);
    let plan = AggregatePlan {
        groups: 0,
        estimated_groups: None,
        grouping_sets: None,
        emit_empty_group_on_empty_input: true,
        aggs: vec![AggOpCode::Avg],
        aggregate_input_types: vec![VeinDataType::Float32],
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
    assert_eq!(average.value(0), 1.0 / 3.0);
}

#[test]
fn float32_sum_reads_input_layout_and_emits_widened_result() {
    let plan = AggregatePlan {
        groups: 0,
        estimated_groups: None,
        grouping_sets: None,
        emit_empty_group_on_empty_input: true,
        aggs: vec![AggOpCode::Sum],
        aggregate_input_types: vec![VeinDataType::Float32],
        types: vec![VeinDataType::Float64],
    };
    let mut sink = AggregateSink::new(plan).unwrap();

    sink.accumulate(DataChunk::from_vectors(vec![Arc::new(Float32Array::from(
        vec![1.25, 2.75],
    ))]))
        .unwrap();
    let output = sink.finish().unwrap();
    let sum = output[0]
        .column(0)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();

    assert_eq!(sum.value(0), 4.0);
}

#[test]
fn floating_aggregates_share_the_generic_fold() {
    let values: ArrayRef = Arc::new(Float64Array::from(vec![Some(1.5), None, Some(2.5)]));
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
        aggregate_input_types: vec![VeinDataType::Float64; 4],
        types: vec![VeinDataType::Float64; 4],
    };
    let mut sink = AggregateSink::new(plan).unwrap();

    sink.accumulate(DataChunk::from_vectors(vec![
        values.clone(),
        values.clone(),
        values.clone(),
        values,
    ]))
        .unwrap();
    let output = sink.finish().unwrap();
    let result = (0..4)
        .map(|column| {
            output[0]
                .column(column)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(0)
        })
        .collect::<Vec<_>>();

    assert_eq!(result, vec![4.0, 1.5, 2.5, 2.0]);
}
