use super::*;

use arrow::array::{ArrayRef, Decimal128Array, Float64Array, Int64Array};
use arrow::datatypes::DataType;
use common::arrow::SelectionVector;
use exec::pipeline::DataChunkView;

fn result_batch(
    schema: SchemaRef,
    columns: Vec<ArrayRef>,
    rows: Vec<u32>,
) -> DataChunkView {
    let view = DataChunkView::from_selected_arrays(
        columns,
        SelectionVector::new_sparse(rows),
    )
        .expect("valid view");
    view.with_schema(schema).expect("valid result view")
}

#[test]
fn json_lines_serialize_mapped_rows_and_preserve_typed_nulls() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("value", DataType::Decimal128(8, 2), true),
        Field::new("value", DataType::Float64, true),
    ]));
    let decimal: ArrayRef = Arc::new(
        Decimal128Array::from(vec![Some(1234), None, Some(-25)])
            .with_precision_and_scale(8, 2)
            .unwrap(),
    );
    let float: ArrayRef = Arc::new(Float64Array::from(vec![Some(1.25), None, Some(-0.5)]));
    let result = result_batch(schema, vec![decimal, float], vec![2, 1, 0]);

    let mut output = Vec::new();
    let mut writer = JsonRowsWriter::new(&mut output);
    writer.write_view_batch(&result).unwrap();
    writer.finish().unwrap();

    let lines = String::from_utf8(output).unwrap();
    let rows = lines.lines().collect::<Vec<_>>();
    assert_eq!(rows.len(), 4);
    let header: serde_json::Value = serde_json::from_str(rows[0]).unwrap();
    assert_eq!(header["fields"][0]["name"], "vein_col_0");
    assert_eq!(header["fields"][0]["data_type"], "Decimal128(8, 2)");
    assert_eq!(rows[1], r#"{"vein_col_0":-0.25,"vein_col_1":-0.5}"#);
    assert_eq!(rows[2], r#"{"vein_col_0":null,"vein_col_1":null}"#);
    assert_eq!(rows[3], r#"{"vein_col_0":12.34,"vein_col_1":1.25}"#);
}

#[test]
fn json_row_batch_slices_share_the_source_value_buffer() {
    let source: ArrayRef = Arc::new(Int64Array::from(vec![10, 20, 30]));
    let schema = Arc::new(Schema::new(vec![Field::new("value", DataType::Int64, false)]));
    let result = result_batch(Arc::clone(&schema), vec![Arc::clone(&source)], vec![2]);

    let (row, run_len) = contiguous_run_batch(&result, schema, 0).unwrap();
    assert_eq!(run_len, 1);
    let source_buffer = source.to_data().buffers()[0].as_ptr();
    let row_buffer = row.column(0).to_data().buffers()[0].as_ptr();
    assert_eq!(
        source_buffer.wrapping_add(2 * std::mem::size_of::<i64>()),
        row_buffer,
        "the row slice must point into the original Arrow values buffer",
    );
}

#[test]
fn json_writer_coalesces_contiguous_rows_into_one_shared_buffer_slice() {
    let source: ArrayRef = Arc::new(Int64Array::from(vec![10, 20, 30]));
    let schema = Arc::new(Schema::new(vec![Field::new("value", DataType::Int64, false)]));
    let result = result_batch(Arc::clone(&schema), vec![Arc::clone(&source)], vec![0, 1, 2]);

    let (batch, run_len) = contiguous_run_batch(&result, schema, 0).unwrap();

    assert_eq!(run_len, 3);
    let source_buffer = source.to_data().buffers()[0].as_ptr();
    let output_buffer = batch.column(0).to_data().buffers()[0].as_ptr();
    assert_eq!(output_buffer, source_buffer);
}
