use super::*;
use std::sync::Arc;

use crate::expression::value::SegmentedArray;
use arrow::array::{Array, Int64Array, StringArray, new_null_array};

fn materialize_case(value: &ExpressionValue) -> ArrayRef {
    match value {
        ExpressionValue::ViewColumn(column) => {
            column
                .materialize_with_report()
                .expect("CASE view should materialize for the assertion")
                .0
        }
        value => value
            .materialized_array()
            .expect("CASE result should materialize for the assertion"),
    }
}

#[test]
fn mapped_case_aligns_segment_boundaries_without_materializing_inputs() {
    let first_condition = SegmentedArray::new(vec![
        Arc::new(BooleanArray::from(vec![Some(true), Some(false)])) as ArrayRef,
        Arc::new(BooleanArray::from(vec![None, Some(true)])) as ArrayRef,
    ])
    .unwrap();
    let second_condition =
        ExpressionValue::Array(Arc::new(BooleanArray::from(vec![false, true, true, true])));
    let first_result = SegmentedArray::new(vec![
        Arc::new(StringArray::from(vec!["a"])) as ArrayRef,
        Arc::new(StringArray::from(vec!["b"])) as ArrayRef,
        Arc::new(StringArray::from(vec!["c", "d"])) as ArrayRef,
    ])
    .unwrap();
    let second_result =
        ExpressionValue::Array(Arc::new(StringArray::from(vec!["w", "x", "y", "z"])));
    let otherwise = ExpressionValue::Scalar(Arc::new(StringArray::from(vec!["else"])));
    let conditions = vec![
        ExpressionValue::Segments(first_condition.clone()),
        second_condition,
    ];
    let results = vec![
        ExpressionValue::Segments(first_result.clone()),
        second_result,
    ];

    let output = eval_case_value(&conditions, &results, Some(&otherwise), &DataType::Utf8, 4)
        .expect("mapped CASE should run over aligned segments");

    assert!(!first_condition.is_materialized());
    assert!(!first_result.is_materialized());
    let output = materialize_case(&output);
    assert_eq!(
        output.as_any().downcast_ref::<StringArray>().unwrap(),
        &StringArray::from(vec!["a", "x", "y", "d"])
    );
}

#[test]
fn mapped_case_without_else_emits_typed_nulls_for_unmatched_rows() {
    let condition = SegmentedArray::new(vec![
        Arc::new(BooleanArray::from(vec![Some(false), None])) as ArrayRef,
        Arc::new(BooleanArray::from(vec![Some(true)])) as ArrayRef,
    ])
    .unwrap();
    let result = ExpressionValue::Scalar(Arc::new(Int64Array::from(vec![7])));
    let conditions = vec![ExpressionValue::Segments(condition)];
    let results = vec![result];

    let output = eval_case_value(&conditions, &results, None, &DataType::Int64, 3)
        .expect("mapped CASE without ELSE should run");
    let output = materialize_case(&output);

    assert_eq!(
        output.as_any().downcast_ref::<Int64Array>().unwrap(),
        &Int64Array::from(vec![None, None, Some(7)])
    );
}

#[test]
fn mapped_case_reuses_branch_buffers_for_uniform_true_segments() {
    let condition = ExpressionValue::Segments(
        SegmentedArray::new(vec![
            Arc::new(BooleanArray::from(vec![true, true, true])) as ArrayRef
        ])
        .unwrap(),
    );
    let branch: ArrayRef = Arc::new(Int64Array::from(vec![10, 20, 30]));
    let output = eval_case_value(
        &[condition],
        &[ExpressionValue::Array(Arc::clone(&branch))],
        None,
        &DataType::Int64,
        3,
    )
    .expect("uniform mapped CASE should reuse its selected branch");
    let ExpressionValue::ViewColumn(_) = &output else {
        panic!("mapped CASE output should remain a view");
    };
    for row in 0..3 {
        let (source, source_row, _) = output.source_row(row, 3).unwrap().unwrap();
        assert!(Arc::ptr_eq(source, &branch));
        assert_eq!(source_row, row);
    }
    let output = materialize_case(&output);
    assert_eq!(
        output.as_any().downcast_ref::<Int64Array>().unwrap(),
        &Int64Array::from(vec![10, 20, 30])
    );
}

#[test]
fn mixed_mapped_case_rows_retain_each_selected_source_array() {
    let condition = ExpressionValue::Segments(
        SegmentedArray::new(vec![
            Arc::new(BooleanArray::from(vec![true, false])) as ArrayRef,
            Arc::new(BooleanArray::from(vec![false, true])) as ArrayRef,
        ])
        .unwrap(),
    );
    let first = SegmentedArray::new(vec![
        Arc::new(Int64Array::from(vec![10, 20])) as ArrayRef,
        Arc::new(Int64Array::from(vec![30, 40])) as ArrayRef,
    ])
    .unwrap();
    let otherwise: ArrayRef = Arc::new(Int64Array::from(vec![40, 50, 60, 70]));
    let output = eval_case_value(
        &[condition],
        &[ExpressionValue::Segments(first.clone())],
        Some(&ExpressionValue::Array(Arc::clone(&otherwise))),
        &DataType::Int64,
        4,
    )
    .expect("mixed CASE should produce a mapped result");

    let ExpressionValue::ViewColumn(_) = &output else {
        panic!("mixed CASE output should remain mapped");
    };
    let first_arrays = first.source_arrays();
    let (source, source_row, _) = output.source_row(0, 4).unwrap().unwrap();
    assert!(Arc::ptr_eq(source, &first_arrays[0]));
    assert_eq!(source_row, 0);
    let (source, source_row, _) = output.source_row(1, 4).unwrap().unwrap();
    assert!(Arc::ptr_eq(source, &otherwise));
    assert_eq!(source_row, 1);
    let (source, source_row, _) = output.source_row(2, 4).unwrap().unwrap();
    assert!(Arc::ptr_eq(source, &otherwise));
    assert_eq!(source_row, 2);
    let (source, source_row, _) = output.source_row(3, 4).unwrap().unwrap();
    assert!(Arc::ptr_eq(source, &first_arrays[1]));
    assert_eq!(source_row, 1);
    assert_eq!(
        materialize_case(&output)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap(),
        &Int64Array::from(vec![10, 50, 60, 40])
    );
}

#[test]
fn mapped_case_treats_untyped_null_branch_as_a_typed_null_output() {
    let condition = ExpressionValue::Segments(
        SegmentedArray::new(vec![
            Arc::new(BooleanArray::from(vec![true, false])) as ArrayRef
        ])
        .unwrap(),
    );
    let null_result = ExpressionValue::Array(new_null_array(&DataType::Null, 2));
    let otherwise = ExpressionValue::Array(Arc::new(Int64Array::from(vec![20, 30])));
    let output = eval_case_value(
        &[condition],
        &[null_result],
        Some(&otherwise),
        &DataType::Int64,
        2,
    )
    .expect("untyped NULL branch should be represented as a null mapping");

    assert_eq!(
        materialize_case(&output)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap(),
        &Int64Array::from(vec![None, Some(30)])
    );
}
