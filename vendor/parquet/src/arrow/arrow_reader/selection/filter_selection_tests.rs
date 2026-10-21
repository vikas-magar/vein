use arrow_array::{Array, BooleanArray};
use arrow_buffer::BooleanBuffer;

use super::RowSelection;

#[test]
fn predicate_selection_keeps_bitmap_and_treats_null_as_false() {
    let dense = BooleanArray::from(vec![true, false, true, false]);
    let dense_selection = RowSelection::from_filter(&dense);
    assert!(dense_selection.as_mask().is_some());
    assert_eq!(dense_selection.as_mask(), Some(dense.values()));

    let nullable = BooleanArray::from(vec![Some(true), None, Some(false), Some(true)]);
    let nullable_selection = RowSelection::from_filter(&nullable);
    let expected = RowSelection::from_boolean_buffer(BooleanBuffer::from(vec![
        true, false, false, true,
    ]));
    assert_eq!(nullable_selection, expected);
}

#[test]
fn predicate_selection_respects_sliced_bitmap_offsets() {
    let input = BooleanArray::from(vec![true, false, true, false, true]);
    let sliced = input.slice(1, 3);
    let sliced = sliced
        .as_any()
        .downcast_ref::<BooleanArray>()
        .expect("sliced boolean array");

    let selection = RowSelection::from_filter(sliced);
    let expected = RowSelection::from_boolean_buffer(BooleanBuffer::from(vec![
        false, true, false,
    ]));
    assert_eq!(selection, expected);
}
