//! Aggregate single value regression coverage.

use super::*;

fn single_value_plan() -> AggregatePlan {
    AggregatePlan {
        groups: 0,
        estimated_groups: None,
        grouping_sets: None,
        emit_empty_group_on_empty_input: true,
        aggs: vec![AggOpCode::SingleValue],
        aggregate_input_types: vec![VeinDataType::Int64],
        types: vec![VeinDataType::Int64],
    }
}

#[test]
fn single_value_retains_the_only_arrow_scalar_without_copying() {
    let input = Arc::new(Int64Array::from(vec![42]));
    let input_values = input.values().as_ptr();
    let mut sink = AggregateSink::new(single_value_plan()).expect("create scalar aggregate");
    sink.accumulate(crate::pipeline::test_support::dense_view(
        vec![Arc::clone(&input) as ArrayRef],
    ))
        .expect("accumulate scalar row");

    let output = sink.finish().expect("finish scalar aggregate");
    let values = crate::pipeline::test_support::materialized_column::<Int64Array>(&output[0], 0);
    assert_eq!(values.value(0), 42);
    assert_eq!(values.values().as_ptr(), input_values);
}

#[test]
fn single_value_returns_typed_null_for_empty_input() {
    let mut sink = AggregateSink::new(single_value_plan()).expect("create scalar aggregate");
    sink.accumulate(crate::pipeline::test_support::dense_view(vec![Arc::new(Int64Array::from(
        Vec::<i64>::new(),
    ))]))
        .expect("accumulate empty input");

    let output = sink.finish().expect("finish empty scalar aggregate");
    let values = crate::pipeline::test_support::materialized_column::<Int64Array>(&output[0], 0);
    assert_eq!(values.len(), 1);
    assert!(values.is_null(0));
}

#[test]
fn single_value_errors_when_input_has_multiple_rows_across_batches() {
    let mut sink = AggregateSink::new(single_value_plan()).expect("create scalar aggregate");
    sink.accumulate(crate::pipeline::test_support::dense_view(vec![Arc::new(Int64Array::from(
        vec![1],
    ))]))
        .expect("first row is valid");
    let error = sink
        .accumulate(crate::pipeline::test_support::dense_view(vec![Arc::new(Int64Array::from(
            vec![2],
        ))]))
        .expect_err("second row must violate scalar cardinality");
    assert_eq!(error.category(), error::ErrorCategory::Execution);
    assert!(
        error
            .to_string()
            .contains("scalar subquery returned more than one row")
    );
}
