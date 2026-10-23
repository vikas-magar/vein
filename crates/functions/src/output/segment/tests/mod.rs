use std::sync::Arc;
use arrow::array::{ArrayRef, Int64Array};
use super::*;

#[test]
fn repeated_run_retains_the_original_buffer() {
    let array: ArrayRef = Arc::new(Int64Array::from(vec![7]));
    let segment = OutputSegment::new(Arc::clone(&array), 4096, true).unwrap();
    assert_eq!(segment.len(), 4096);
    assert!(segment.is_scalar());
    assert!(Arc::ptr_eq(segment.array(), &array));
}

#[test]
fn rejects_unproven_broadcast_and_invalid_scalar_shape() {
    let one: ArrayRef = Arc::new(Int64Array::from(vec![7]));
    let two: ArrayRef = Arc::new(Int64Array::from(vec![7, 8]));
    assert!(OutputSegment::new(Arc::clone(&one), 5, false).is_err());
    assert!(OutputSegment::new(two, 5, true).is_err());
    assert!(OutputSegment::new(one, 0, true).is_err());
}
