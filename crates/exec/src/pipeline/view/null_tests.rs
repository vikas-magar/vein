use super::*;
use arrow::array::Int64Array;

#[test]
fn is_null_checks_arrow_nulls_and_null_extended_rows() {
    let values = Arc::new(Int64Array::from(vec![Some(7), None]));
    let source = DataChunkView::from_arrays(
        vec![values],
        2,
        SelectionVector::new_dense(2),
    )
    .expect("create Arrow-backed view");
    let rows = [None, Some(0), Some(1)];
    let view = DataChunkView::combine(
        &[ViewInputRows { view: &source, rows: &rows }],
        &[ViewColumnRef { input: 0, column: 0 }],
    )
    .expect("compose null-extended and source rows");

    assert!(view.is_null(0, 0).expect("check null-extended row"));
    assert!(!view.is_null(0, 1).expect("check non-null source value"));
    assert!(view.is_null(0, 2).expect("check Arrow NULL value"));
}
