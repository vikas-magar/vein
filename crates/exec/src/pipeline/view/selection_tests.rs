use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array};
use common::arrow::SelectionVector;

use crate::pipeline::DataChunkView;

fn int_at(view: &DataChunkView, row: usize) -> i64 {
    let (array, index) = view.resolve(0, row).unwrap().unwrap();
    array
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(index)
}

#[test]
fn selected_rows_compose_without_copying_source_arrays() {
    let source: ArrayRef = Arc::new(Int64Array::from(vec![10, 20, 30, 40, 50]));
    let input = DataChunkView::from_selected_arrays(
        vec![Arc::clone(&source)],
        SelectionVector::new_sparse(vec![4, 1, 3]),
    )
    .unwrap();

    let selected = input.select_logical_rows(&[2, 0, 2]).unwrap();

    assert_eq!(
        (0..selected.num_rows())
            .map(|row| int_at(&selected, row))
            .collect::<Vec<_>>(),
        [40, 50, 40]
    );
    assert!(Arc::ptr_eq(
        &source,
        selected
            .source_arrays_for_column(0)
            .unwrap()
            .first()
            .unwrap()
    ));
}

#[test]
fn row_selection_accepts_empty_and_rejects_out_of_range_rows() {
    let source: ArrayRef = Arc::new(Int64Array::from(vec![1, 2]));
    let view = DataChunkView::from_dense_arrays(vec![source]).unwrap();

    assert_eq!(view.select_logical_rows(&[]).unwrap().num_rows(), 0);
    assert!(view.select_logical_rows(&[2]).is_err());
}
