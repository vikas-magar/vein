use super::SelectionVector;

#[test]
fn selection_vector_dense() {
    let selection = SelectionVector::new_dense(5);
    assert_eq!(selection.size(), 5);
    assert_eq!(selection.get_index(0), 0);
    assert_eq!(selection.get_index(3), 3);
}

#[test]
fn selection_reports_contiguous_ranges() {
    assert_eq!(SelectionVector::new_dense(5).contiguous_range(), Some(0..5));
    assert_eq!(
        SelectionVector::new_sparse(vec![4, 5, 6]).contiguous_range(),
        Some(4..7)
    );
    assert_eq!(
        SelectionVector::new_sparse(vec![4, 6]).contiguous_range(),
        None
    );
}

#[test]
fn selection_vector_sparse() {
    let selection = SelectionVector::new_sparse(vec![2, 4, 7]);
    assert_eq!(selection.size(), 3);
    assert_eq!(selection.get_index(0), 2);
    assert_eq!(selection.get_index(1), 4);
    assert_eq!(selection.get_index(2), 7);
}

#[test]
fn selection_vector_filters_preserve_survivors() {
    let dense = SelectionVector::new_dense(10);
    let evens = dense.filter(|index| index % 2 == 0);
    assert_eq!(evens.size(), 5);
    assert_eq!(
        (0..evens.size()).map(|row| evens.get_index(row)).collect::<Vec<_>>(),
        vec![0, 2, 4, 6, 8]
    );

    let chained = evens.filter(|index| index > 3);
    assert_eq!(
        (0..chained.size()).map(|row| chained.get_index(row)).collect::<Vec<_>>(),
        vec![4, 6, 8]
    );
}

#[test]
fn selection_vector_slice_keeps_dense_ranges_compact() {
    let selection = SelectionVector::new_dense(10);
    let sliced = selection.slice(3, 4);
    assert_eq!(sliced.size(), 4);
    assert_eq!(sliced.contiguous_range(), Some(3..7));

    let empty = selection.slice(15, 5);
    assert!(empty.is_empty());
    assert_eq!(empty.contiguous_range(), Some(0..0));
}

#[test]
fn dense_filter_that_keeps_every_row_stays_contiguous() {
    let selection = SelectionVector::new_dense(5);
    assert_eq!(selection.filter(|_| true).contiguous_range(), Some(0..5));

    let windowed = selection.slice(2, 3);
    assert_eq!(windowed.filter(|_| true).contiguous_range(), Some(2..5));
    assert_eq!(selection.filter(|row| row != 2).contiguous_range(), None);
}
