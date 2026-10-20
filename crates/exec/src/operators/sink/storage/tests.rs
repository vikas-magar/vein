use super::*;
use arrow::array::{Array, ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use common::arrow::SelectionVector;

fn write_sink(nullable: bool) -> WriteSink {
    WriteSink {
        expected_schema: Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            nullable,
        )])),
        transaction: None,
        rows_written: 0,
        batches_written: 0,
        conversion_us: 0,
        materialization: MaterializationReport::default(),
        receipt: None,
    }
}

fn view(source: &ArrayRef, selection: SelectionVector) -> DataChunkView {
    DataChunkView::from_arrays(vec![Arc::clone(source)], source.len(), selection)
        .expect("valid view over source arrays")
}

#[test]
fn write_boundary_slices_contiguous_view_rows_without_copying_payloads() {
    let source: ArrayRef = Arc::new(Int64Array::from(vec![10, 20, 30, 40]));
    let selected = view(&source, SelectionVector::new_sparse(vec![1, 2]));
    let (batch, report) = write_sink(false)
        .materialize(&selected)
        .expect("view conversion")
        .expect("non-empty batch");

    let output = batch.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    let input = source.as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(output, &Int64Array::from(vec![20, 30]));
    assert_eq!(
        output.values().as_ptr(),
        input.values().as_ptr().wrapping_add(1)
    );
    assert_eq!(report.shared_columns, 1);
    assert_eq!(report.gathered_columns, 0);
}

#[test]
fn write_boundary_gathers_only_sparse_reordered_rows() {
    let source: ArrayRef = Arc::new(Int64Array::from(vec![10, 20, 30, 40]));
    let selected = view(&source, SelectionVector::new_sparse(vec![3, 1]));
    let (batch, report) = write_sink(false)
        .materialize(&selected)
        .expect("view conversion")
        .expect("non-empty batch");

    let output = batch.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(output, &Int64Array::from(vec![40, 20]));
    assert_eq!(report.gathered_columns, 1);
    assert_eq!(report.shared_columns, 0);
}

#[test]
fn write_boundary_checks_nullability_on_selected_rows() {
    let source: ArrayRef = Arc::new(Int64Array::from(vec![Some(10), None]));
    let selected_null = view(&source, SelectionVector::new_sparse(vec![1]));
    assert!(write_sink(false).materialize(&selected_null).is_err());

    let selected_value = view(&source, SelectionVector::new_sparse(vec![0]));
    assert!(write_sink(false).materialize(&selected_value).is_ok());
}

#[test]
fn write_boundary_skips_empty_views() {
    let source: ArrayRef = Arc::new(Int64Array::from(vec![10, 20]));
    let empty = view(&source, SelectionVector::new_sparse(Vec::new()));
    assert!(write_sink(false).materialize(&empty).unwrap().is_none());
}
