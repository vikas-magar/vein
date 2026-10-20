use super::*;
use arrow::array::Int64Array;
use common::arrow::SelectionVector;
use crate::pipeline::{DataChunk, DataChunkView, MaterializationReport};

#[path = "tests/access_tests.rs"]
mod access_tests;

#[test]
fn register_file_bounds_and_lifecycle() {
    let mut rf = RegisterFile::new(4);
    assert_eq!(rf.len(), 4);
    assert!(!rf.is_empty());
    assert_eq!(rf.get(0).unwrap().physical_len(), 0);
    assert!(rf.get(4).is_err());
    assert!(rf.set(4, Arc::new(Int64Array::from(vec![1, 2]))).is_err());

    let array: ArrayRef = Arc::new(Int64Array::from(vec![10, 20, 30]));
    rf.set(1, array).unwrap();
    assert_eq!(rf.get(1).unwrap().physical_len(), 3);
    let projected = rf.project(&[1, 1]).unwrap();
    assert_eq!(projected.len(), 2);
    assert_eq!(projected[0].len(), 3);
    assert_eq!(projected[1].len(), 3);
}

#[test]
fn mount_view_registers_without_gathering_until_materialization_is_requested() {
    let source: ArrayRef = Arc::new(Int64Array::from(vec![10, 20, 30, 40]));
    let view = Arc::new(
        DataChunkView::from_chunk(DataChunk::new(
            vec![Arc::clone(&source)],
            SelectionVector::new_sparse(vec![3, 1]),
        ))
            .unwrap(),
    );
    let mut registers = RegisterFile::new(1);
    registers
        .mount_view_columns(2, Arc::clone(&view), [0])
        .unwrap();

    assert_eq!(registers.get(0).unwrap().view_column_index(), Some(0));
    let report = registers.materialize_register(0).unwrap();
    assert_eq!(report.gathered_columns, 1);
    assert_eq!(registers.get(0).unwrap().view_column_index(), None);
    let values = registers.get(0).unwrap().array().unwrap()
        .as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(values.values(), &[40, 20]);
    assert!(Arc::ptr_eq(view.source_arrays_for_column(0).unwrap().first().unwrap(), &source));
}

#[test]
fn mapped_register_aliases_share_the_materialized_arrow_array() {
    let source: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3]));
    let view = Arc::new(DataChunkView::from_chunk(DataChunk::from_vectors(vec![Arc::clone(&source)])).unwrap());
    let mut registers = RegisterFile::new(2);
    registers.mount_view_columns(3, Arc::clone(&view), [0]).unwrap();
    let alias = registers.get(0).unwrap().clone();
    registers.set_value(1, alias).unwrap();

    let first = registers.materialize_register(0).unwrap();
    let second = registers.materialize_register(1).unwrap();

    assert_eq!(first.shared_columns, 1);
    assert_eq!(second, MaterializationReport::default());
    assert!(Arc::ptr_eq(
        registers.get(0).unwrap().array().unwrap(),
        registers.get(1).unwrap().array().unwrap()
    ));
}

#[test]
fn segmented_register_slices_share_source_buffers_and_materialize_once() {
    let first: ArrayRef = Arc::new(Int64Array::from(vec![10, 20]));
    let second: ArrayRef = Arc::new(Int64Array::from(vec![30, 40]));
    let first_values = first.as_any().downcast_ref::<Int64Array>().unwrap();
    let second_values = second.as_any().downcast_ref::<Int64Array>().unwrap();
    let segments = SegmentedArray::new(vec![Arc::clone(&first), Arc::clone(&second)]).unwrap();

    let (slice, len) = segments.segment(1, 3, 4).unwrap();
    assert_eq!(len, 1);
    let slice = slice.as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(slice.values().as_ptr(), first_values.values().as_ptr().wrapping_add(1));
    let (slice, len) = segments.segment(2, 2, 4).unwrap();
    assert_eq!(len, 2);
    let slice = slice.as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(slice.values().as_ptr(), second_values.values().as_ptr());
    assert!(!segments.is_materialized());

    let mut registers = RegisterFile::new(2);
    registers.set_value(0, RegValue::Segments(segments.clone())).unwrap();
    registers.set_value(1, RegValue::Segments(segments.clone())).unwrap();
    assert_eq!(registers.materialize_register(0).unwrap(), MaterializationReport::default());
    assert!(segments.is_materialized());
    assert_eq!(registers.materialize_register(1).unwrap(), MaterializationReport::default());
    assert!(Arc::ptr_eq(
        registers.get(0).unwrap().array().unwrap(),
        registers.get(1).unwrap().array().unwrap()
    ));
    let values = registers.get(0).unwrap().array().unwrap()
        .as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(values.values(), &[10, 20, 30, 40]);
}
