//! Aggregate selection regression coverage.

use super::*;

#[test]
fn accumulation_reads_only_selected_physical_rows() {
    let plan = AggregatePlan {
        groups: 1,
        estimated_groups: None,
        grouping_sets: None,
        emit_empty_group_on_empty_input: true,
        aggs: vec![AggOpCode::Sum],
        aggregate_input_types: vec![VeinDataType::Int64],
        types: vec![VeinDataType::Int64, VeinDataType::Int64],
    };
    let chunk = DataChunk::new(
        vec![
            Arc::new(Int64Array::from(vec![10, 20, 30, 40])),
            Arc::new(Int64Array::from(vec![100, 200, 300, 400])),
        ],
        SelectionVector::new_sparse(vec![1, 3]),
    );
    let mut sink = AggregateSink::new(plan).unwrap();

    sink.accumulate(chunk).unwrap();
    sink.accumulate(DataChunk::from_vectors(vec![
        Arc::new(Int64Array::from(vec![20, 50])),
        Arc::new(Int64Array::from(vec![3, 5])),
    ]))
        .unwrap();
    let output = sink.finish().unwrap();

    let keys = output[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let sums = output[0]
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(keys.values(), &[20, 40, 50]);
    assert_eq!(sums.values(), &[203, 400, 5]);
}

#[test]
fn empty_global_aggregate_emits_count_zero_and_null_sum() {
    let plan = AggregatePlan {
        groups: 0,
        estimated_groups: None,
        grouping_sets: None,
        emit_empty_group_on_empty_input: true,
        aggs: vec![AggOpCode::Count, AggOpCode::Sum],
        aggregate_input_types: vec![VeinDataType::Int64, VeinDataType::Float64],
        types: vec![VeinDataType::Int64, VeinDataType::Float64],
    };
    let chunk = DataChunk::from_vectors(vec![
        Arc::new(Int64Array::from(Vec::<i64>::new())),
        Arc::new(Float64Array::from(Vec::<f64>::new())),
    ]);
    let mut sink = AggregateSink::new(plan).unwrap();

    sink.accumulate(chunk).unwrap();
    let output = sink.finish().unwrap();

    assert_eq!(output.len(), 1);
    assert_eq!(output[0].num_rows(), 1);
    let count = output[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let sum = output[0]
        .column(1)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(count.values(), &[0]);
    assert!(sum.is_null(0));
}

#[test]
fn zero_column_distinct_emits_one_row_only_for_nonempty_input() {
    let distinct_plan = || AggregatePlan {
        groups: 0,
        estimated_groups: None,
        grouping_sets: Some(vec![vec![]]),
        emit_empty_group_on_empty_input: false,
        aggs: Vec::new(),
        aggregate_input_types: Vec::new(),
        types: Vec::new(),
    };

    let mut empty = AggregateSink::new(distinct_plan()).expect("create empty distinct sink");
    assert!(empty.finish().expect("finish empty distinct").is_empty());

    let mut nonempty = AggregateSink::new(distinct_plan()).expect("create distinct sink");
    nonempty
        .accumulate(DataChunk::new(Vec::new(), SelectionVector::new_dense(4)))
        .expect("accumulate zero-column input rows");
    let output = nonempty.finish().expect("finish distinct");

    assert_eq!(output.len(), 1);
    assert_eq!(output[0].num_columns(), 0);
    assert_eq!(output[0].num_rows(), 1);
}

#[test]
fn empty_global_average_uses_its_declared_input_type() {
    let plan = AggregatePlan {
        groups: 0,
        estimated_groups: None,
        grouping_sets: None,
        emit_empty_group_on_empty_input: true,
        aggs: vec![AggOpCode::Avg],
        aggregate_input_types: vec![VeinDataType::Float32],
        types: vec![VeinDataType::Float64],
    };
    let chunk = DataChunk::from_vectors(vec![Arc::new(Float32Array::from(Vec::<f32>::new()))]);
    let mut sink = AggregateSink::new(plan).unwrap();

    sink.accumulate(chunk).unwrap();
    let output = sink.finish().unwrap();
    let average = output[0]
        .column(0)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();

    assert!(average.is_null(0));
}
