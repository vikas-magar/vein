use super::*;
use arrow::array::Int64Array;

fn ints(values: &[i64]) -> ArrayRef {
    Arc::new(Int64Array::from(values.to_vec()))
}

fn int_at(view: &DataChunkView, row: usize) -> i64 {
    let (array, index) = view.resolve(0, row).unwrap().unwrap();
    array
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(index)
}

#[test]
fn concatenated_mapping_keeps_segment_arrays_and_resolves_selections() {
    let first = ints(&[10, 20]);
    let second = ints(&[30, 40, 50]);
    let mapping = Arc::new(RowMapping::concatenated(Arc::from(vec![2, 3])).unwrap());
    assert_eq!(
        mapping.contiguous_source_range(&SelectionVector::new_sparse(vec![2, 3])),
        Some((1, 0, 2))
    );
    assert_eq!(
        mapping.contiguous_source_range(&SelectionVector::new_sparse(vec![1, 2])),
        None
    );
    let column = ViewColumn::new(
        vec![Arc::clone(&first), Arc::clone(&second)],
        DataType::Int64,
        mapping,
    )
        .unwrap();
    let view = DataChunkView::new(
        5,
        vec![column],
        SelectionVector::new_sparse(vec![4, 1, 2]),
    )
        .unwrap();

    assert_eq!((0..3).map(|row| int_at(&view, row)).collect::<Vec<_>>(), [50, 20, 30]);
    let arrays = view.source_arrays_for_column(0).unwrap();
    assert!(Arc::ptr_eq(&arrays[0], &first));
    assert!(Arc::ptr_eq(&arrays[1], &second));
}

#[test]
fn repeated_mapping_broadcasts_a_scalar_without_copying_it() {
    let scalar = ints(&[7]);
    let mapping = Arc::new(RowMapping::repeated(Arc::from(vec![1]), 0, 0, 3).unwrap());
    let column = ViewColumn::new(
        vec![Arc::clone(&scalar)],
        DataType::Int64,
        mapping,
    )
        .unwrap();
    let view = DataChunkView::new(3, vec![column], SelectionVector::new_dense(3)).unwrap();

    assert_eq!((0..3).map(|row| int_at(&view, row)).collect::<Vec<_>>(), [7, 7, 7]);
    assert!(Arc::ptr_eq(
        view.source_arrays_for_column(0).unwrap().first().unwrap(),
        &scalar
    ));
}
