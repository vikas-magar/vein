use std::sync::Arc;

use arrow::array::{ArrayRef, Datum, Int64Array, StringArray};

use crate::expression::value::ExpressionValue;
use crate::pipeline::{DataChunkView, ViewColumnRef, ViewInputRows};

#[test]
fn repeated_view_rows_are_one_zero_copy_scalar_segment() {
    let source = crate::pipeline::test_support::dense_view(vec![
        Arc::new(Int64Array::from(vec![10, 20, 30])) as ArrayRef,
    ]);
    let repeated_rows = [Some(1), Some(1), Some(1), Some(2)];
    let view = DataChunkView::combine(
        &[ViewInputRows {
            view: &source,
            rows: &repeated_rows,
        }],
        &[ViewColumnRef {
            input: 0,
            column: 0,
        }],
    )
        .expect("compose repeated references");
    let value = ExpressionValue::view_column(Arc::new(view), 0)
        .expect("create expression column");

    let mut segment = value.segment(0, 4, 4).expect("resolve repeated run");
    assert_eq!(segment.len(), 3);
    assert!(segment.is_scalar());
    let datum = segment.datum().expect("scalar datum");
    let (array, scalar) = datum.get();
    assert!(scalar, "Arrow should broadcast the retained one-value slice");
    let values = array
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("int64 source");
    assert_eq!(values.len(), 1);
    assert_eq!(values.value(0), 20);

    let prefix = segment.take_prefix(2);
    assert!(prefix.is_scalar());
    assert_eq!(prefix.len(), 2);
    assert_eq!(segment.len(), 1);
}

#[test]
fn repeated_utf8_rows_retain_the_source_value_buffer() {
    let values = Arc::new(StringArray::from(vec!["first", "needle", "last"]));
    let source_buffer = values.value_data().as_ptr();
    let source = crate::pipeline::test_support::dense_view(vec![values.clone() as ArrayRef]);
    let repeated_rows = [Some(1), Some(1)];
    let view = DataChunkView::combine(
        &[ViewInputRows {
            view: &source,
            rows: &repeated_rows,
        }],
        &[ViewColumnRef {
            input: 0,
            column: 0,
        }],
    )
        .expect("compose repeated string references");
    let value = ExpressionValue::view_column(Arc::new(view), 0)
        .expect("create expression column");

    let segment = value.segment(0, 2, 2).expect("resolve repeated string run");
    let datum = segment.datum().expect("borrowed string datum");
    let (array, scalar) = datum.get();
    let sliced = array
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("utf8 source");

    assert!(scalar);
    assert_eq!(sliced.value(0), "needle");
    assert!(std::ptr::eq(sliced.value_data().as_ptr(), source_buffer));
}
