use std::sync::Arc;
use arrow::array::{Array, ArrayRef, BooleanArray, Int64Array};
use super::*;
use crate::expression::kernels::{eval_add, eval_cmp_gt, eval_neg};
use crate::pipeline::{DataChunkView, ViewColumnRef, ViewInputRows};

fn mapped(rows: &[Option<usize>]) -> ExpressionValue {
    let source = crate::pipeline::test_support::dense_view(vec![
        Arc::new(Int64Array::from(vec![10, 20, 30])) as ArrayRef,
    ]);
    let view = DataChunkView::combine(&[ViewInputRows { view: &source, rows }],
                                      &[ViewColumnRef { input: 0, column: 0 }]).unwrap();
    ExpressionValue::view_column(Arc::new(view), 0).unwrap()
}

fn integers(value: &ExpressionValue, rows: usize) -> Vec<Option<i64>> {
    (0..rows).map(|row| {
        value.source_row(row, rows).unwrap().and_then(|(array, index, _)| {
            let array = array.as_any().downcast_ref::<Int64Array>().unwrap();
            array.is_valid(index).then(|| array.value(index))
        })
    }).collect()
}

#[test]
fn repeated_arithmetic_survives_chained_unary_and_binary_kernels() {
    let input = mapped(&[Some(1), Some(1), Some(1), Some(0), None]);
    let scalar = ExpressionValue::Scalar(Arc::new(Int64Array::from(vec![2])));
    let sum = eval_mapped_binary_value(&input, &scalar, 5, eval_add).unwrap();
    assert_eq!(sum.physical_len(), 5);
    assert_eq!(integers(&sum, 5), vec![Some(22), Some(22), Some(22), Some(12), None]);
    let first = sum.segment(0, 5, 5).unwrap();
    assert!(first.is_scalar());
    assert_eq!(first.len(), 3);
    assert_eq!(first.datum().unwrap().get().0.len(), 1);
    let negated = eval_mapped_unary_value(&sum, 5,
                                          |rows| Ok(new_null_array(sum.data_type(), rows)), eval_neg).unwrap();
    assert_eq!(integers(&negated, 5), vec![Some(-22), Some(-22), Some(-22), Some(-12), None]);
    let compared = eval_mapped_binary_value(&sum, &scalar, 5, eval_cmp_gt).unwrap();
    for row in 0..5 {
        let (array, index, _) = compared.source_row(row, 5).unwrap().unwrap();
        let array = array.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert_eq!(array.is_valid(index), row != 4);
        if row != 4 { assert!(array.value(index)); }
    }
}

#[test]
fn differently_aligned_runs_and_dense_operands_preserve_rows() {
    let left = mapped(&[Some(1), Some(1), Some(1), Some(2), Some(2)]);
    let right = mapped(&[Some(0), Some(0), Some(1), Some(1), Some(1)]);
    let output = eval_mapped_binary_value(&left, &right, 5, eval_add).unwrap();
    assert_eq!(integers(&output, 5), vec![Some(30), Some(30), Some(40), Some(50), Some(50)]);
    let dense = ExpressionValue::Array(Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])));
    let output = eval_mapped_binary_value(&output, &dense, 5, eval_add).unwrap();
    assert_eq!(integers(&output, 5), vec![Some(31), Some(32), Some(43), Some(54), Some(55)]);
}

#[test]
fn scalar_run_identity_kernel_shares_the_source_buffer() {
    let input = mapped(&[Some(1), Some(1), Some(1)]);
    let source = input.segment(0, 3, 3).unwrap();
    let original = source.datum().unwrap().get().0.to_data().buffers()[0].as_ptr();
    let output = eval_mapped_unary_value(&input, 3,
                                         |rows| Ok(new_null_array(input.data_type(), rows)),
                                         |array| Ok(arrow::array::make_array(array.to_data()))).unwrap();
    let (array, row, _) = output.source_row(2, 3).unwrap().unwrap();
    assert_eq!(row, 0);
    assert_eq!(array.len(), 1);
    assert_eq!(original, array.to_data().buffers()[0].as_ptr());
}

#[test]
fn malformed_dense_kernel_results_are_not_silently_broadcast() {
    let input = mapped(&[Some(0), Some(1), Some(2)]);
    let scalar = ExpressionValue::Scalar(Arc::new(Int64Array::from(vec![2])));
    assert!(eval_mapped_binary_value(&input, &scalar, 3,
                                     |_, _| Ok(Arc::new(Int64Array::from(vec![1])))).is_err());
    assert!(eval_mapped_unary_value(&input, 3,
                                    |rows| Ok(new_null_array(input.data_type(), rows)),
                                    |_| Ok(Arc::new(Int64Array::from(vec![1])))).is_err());
}

#[test]
fn dense_scalar_results_retain_broadcast_semantics() {
    let scalar = ExpressionValue::Scalar(Arc::new(Int64Array::from(vec![2])));
    let sum = eval_mapped_binary_value(&scalar, &scalar, 4, eval_add).unwrap();
    assert!(sum.is_scalar());
    let negative = eval_mapped_unary_value(&sum, 4,
                                           |rows| Ok(new_null_array(sum.data_type(), rows)), eval_neg).unwrap();
    assert!(negative.is_scalar());
    assert_eq!(integers(&negative, 4), vec![Some(-4); 4]);
}

#[test]
fn repeated_physical_nulls_remain_one_null_value() {
    let source = crate::pipeline::test_support::dense_view(vec![
        Arc::new(Int64Array::from(vec![None::<i64>])) as ArrayRef,
    ]);
    let rows = [Some(0), Some(0), Some(0)];
    let view = DataChunkView::combine(&[ViewInputRows { view: &source, rows: &rows }],
        &[ViewColumnRef { input: 0, column: 0 }]).unwrap();
    let input = ExpressionValue::view_column(Arc::new(view), 0).unwrap();
    let scalar = ExpressionValue::Scalar(Arc::new(Int64Array::from(vec![2])));
    let result = eval_mapped_binary_value(&input, &scalar, 3, eval_add).unwrap();
    assert_eq!(integers(&result, 3), vec![None; 3]);
    let run = result.segment(0, 3, 3).unwrap();
    assert!(run.is_scalar());
    assert_eq!(run.datum().unwrap().get().0.len(), 1);
}
