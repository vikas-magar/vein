use super::*;

use arrow::array::{ArrayRef, Decimal128Array, Float64Array, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use common::arrow::SelectionVector;
use exec::pipeline::DataChunkView;

fn result_batch(schema: SchemaRef, columns: Vec<ArrayRef>, rows: Vec<u32>) -> DataChunkView {
    let view = DataChunkView::from_selected_arrays(columns, SelectionVector::new_sparse(rows))
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
fn json_encoder_reuses_one_source_for_discontiguous_rows() {
    let source: ArrayRef = Arc::new(Int64Array::from(vec![10, 20, 30]));
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]));
    let result = result_batch(
        Arc::clone(&schema),
        vec![Arc::clone(&source)],
        vec![2, 0, 2, 1],
    );
    let mut output = Vec::new();
    let encoders = encode::write_view(
        &result,
        &schema,
        &[b"\"n\":".to_vec()],
        &mut Vec::new(),
        &mut output,
    )
    .unwrap();
    assert_eq!(
        encoders.source_encoders, 1,
        "encoder creation must depend on sources, not row runs"
    );
    assert_eq!(encoders.bytes, output.len());
    assert_eq!(
        String::from_utf8(output).unwrap(),
        "{\"n\":30}\n{\"n\":10}\n{\"n\":30}\n{\"n\":20}\n"
    );
    assert!(Arc::ptr_eq(
        result.resolve(0, 0).unwrap().unwrap().0,
        &source
    ));
}

#[test]
fn json_encoder_reuses_one_source_for_contiguous_rows() {
    let source: ArrayRef = Arc::new(Int64Array::from(vec![10, 20, 30]));
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]));
    let result = result_batch(
        Arc::clone(&schema),
        vec![Arc::clone(&source)],
        vec![0, 1, 2],
    );

    let mut output = Vec::new();
    assert_eq!(
        encode::write_view(
            &result,
            &schema,
            &[b"\"n\":".to_vec()],
            &mut Vec::new(),
            &mut output
        )
        .unwrap()
        .source_encoders,
        1
    );
    assert_eq!(
        output
            .split(|byte| *byte == b'\n')
            .filter(|row| !row.is_empty())
            .count(),
        3
    );
}

mod sources;
