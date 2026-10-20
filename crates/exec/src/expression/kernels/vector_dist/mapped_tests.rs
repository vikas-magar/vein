use super::*;
use crate::expression::value::ExpressionValue;
use crate::pipeline::DataChunkView;
use arrow::array::{ArrayRef, Float32Array, ListArray};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, Field};
use common::arrow::SelectionVector;

fn list(rows: Vec<Vec<f32>>) -> ArrayRef {
    let mut values = Vec::new();
    let mut lengths = Vec::new();
    for row in rows {
        lengths.push(row.len());
        values.extend(row);
    }
    Arc::new(ListArray::new(
        Arc::new(Field::new("item", DataType::Float32, false)),
        OffsetBuffer::from_lengths(lengths),
        Arc::new(Float32Array::from(values)),
        None,
    ))
}

fn nullable_list(values: Vec<Option<f32>>) -> ArrayRef {
    Arc::new(ListArray::new(
        Arc::new(Field::new("item", DataType::Float32, true)),
        OffsetBuffer::from_lengths([values.len()]),
        Arc::new(Float32Array::from(values)),
        None,
    ))
}

#[test]
fn mapped_cosine_distance_reads_sparse_rows_without_materializing_input_columns() {
    let left = list(vec![vec![1.0, 0.0], vec![3.0, 4.0], vec![2.0, 2.0]]);
    let right = list(vec![vec![0.0, 1.0], vec![3.0, 4.0], vec![1.0, 1.0]]);
    let view = Arc::new(
        crate::pipeline::test_support::dense_view(vec![left, right])
            .with_selection(SelectionVector::new_sparse(vec![1, 0]))
            .unwrap(),
    );
    let left_value = ExpressionValue::view_column(Arc::clone(&view), 0).unwrap();
    let right_value = ExpressionValue::view_column(Arc::clone(&view), 1).unwrap();

    let result = eval_mapped_cosine_distance(&left_value, &right_value, 2).unwrap();
    let values = result.as_any().downcast_ref::<Float64Array>().unwrap();
    assert_eq!(values.value(0), 0.0);
    assert_eq!(values.value(1), 1.0);
    assert!(matches!(left_value, ExpressionValue::ViewColumn(_)));
    assert!(matches!(right_value, ExpressionValue::ViewColumn(_)));
}

#[test]
fn mapped_distance_propagates_null_extended_rows_and_null_elements() {
    let left = list(vec![vec![1.0, 0.0], vec![1.0, 1.0]]);
    let right = list(vec![vec![0.0, 1.0], vec![1.0, 1.0]]);
    let source = crate::pipeline::test_support::dense_view(vec![left, right]);
    let rows = [Some(0), None];
    let view = DataChunkView::combine(
        &[crate::pipeline::ViewInputRows {
            view: &source,
            rows: &rows,
        }],
        &[
            crate::pipeline::ViewColumnRef {
                input: 0,
                column: 0,
            },
            crate::pipeline::ViewColumnRef {
                input: 0,
                column: 1,
            },
        ],
    )
    .unwrap();
    let view = Arc::new(view);
    let left_value = ExpressionValue::view_column(Arc::clone(&view), 0).unwrap();
    let right_value = ExpressionValue::view_column(Arc::clone(&view), 1).unwrap();
    let result = eval_mapped_cosine_distance(&left_value, &right_value, 2).unwrap();
    let values = result.as_any().downcast_ref::<Float64Array>().unwrap();
    assert_eq!(values.value(0), 1.0);
    assert!(values.is_null(1));

    let nullable = crate::pipeline::test_support::dense_view(vec![
        nullable_list(vec![Some(1.0), None]),
        nullable_list(vec![Some(0.0), Some(1.0)]),
    ]);
    let nullable = Arc::new(nullable);
    let left_value = ExpressionValue::view_column(Arc::clone(&nullable), 0).unwrap();
    let right_value = ExpressionValue::view_column(Arc::clone(&nullable), 1).unwrap();
    let result = eval_mapped_cosine_distance(&left_value, &right_value, 1).unwrap();
    assert!(
        result
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .is_null(0)
    );
}
