use super::*;
use arrow::array::{Date32Array, StringArray};
use exec::pipeline::{RowMapping, SourceRow, ViewColumn};

#[test]
fn encoders_follow_multiple_sources_and_outer_nulls() {
    let sources: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(vec![11, 12])),
        Arc::new(Int64Array::from(vec![21, 22])),
    ];
    let mapping = Arc::new(
        RowMapping::new(
            Arc::from(vec![2, 2]),
            vec![
                Some(SourceRow::new(1, 1).unwrap()),
                None,
                Some(SourceRow::new(0, 0).unwrap()),
                Some(SourceRow::new(1, 1).unwrap()),
            ],
        )
        .unwrap(),
    );
    let column = ViewColumn::new(sources.clone(), DataType::Int64, mapping).unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, true)]));
    let view = DataChunkView::new(4, vec![column], SelectionVector::new_dense(4))
        .unwrap()
        .with_schema(Arc::clone(&schema))
        .unwrap();
    let mut output = Vec::new();
    let count = encode::write_view(
        &view,
        &schema,
        &[b"\"n\":".to_vec()],
        &mut Vec::new(),
        &mut output,
    )
    .unwrap();
    assert_eq!(count.source_encoders, 2);
    assert_eq!(count.bytes, output.len());
    assert_eq!(
        String::from_utf8(output).unwrap(),
        "{\"n\":22}\n{\"n\":null}\n{\"n\":11}\n{\"n\":22}\n"
    );
    assert!(Arc::ptr_eq(
        view.resolve(0, 0).unwrap().unwrap().0,
        &sources[1]
    ));
}

#[test]
fn encoding_matches_arrow_for_escaping_dates_and_nonfinite_numbers() {
    let columns: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(vec![Some("a\"b\n"), Some("é"), None])),
        Arc::new(Date32Array::from(vec![Some(0), None, Some(1)])),
        Arc::new(Float64Array::from(vec![f64::NAN, f64::INFINITY, -0.0])),
    ];
    let schema = Arc::new(Schema::new(
        columns
            .iter()
            .enumerate()
            .map(|(i, a)| Field::new(format!("vein_col_{i}"), a.data_type().clone(), true))
            .collect::<Vec<_>>(),
    ));
    let batch =
        arrow::record_batch::RecordBatch::try_new(Arc::clone(&schema), columns.clone()).unwrap();
    let mut expected = Vec::new();
    let mut reference = arrow::json::writer::WriterBuilder::new()
        .with_explicit_nulls(true)
        .build::<_, arrow::json::writer::LineDelimited>(&mut expected);
    reference.write(&batch).unwrap();
    reference.finish().unwrap();
    let view = DataChunkView::from_dense_arrays(columns)
        .unwrap()
        .with_schema(schema)
        .unwrap();
    let mut actual = Vec::new();
    let mut writer = JsonRowsWriter::new(&mut actual);
    writer.write_view_batch(&view).unwrap();
    writer.finish().unwrap();
    let rows = actual.splitn(2, |byte| *byte == b'\n').nth(1).unwrap();
    assert_eq!(rows, expected);
}

#[test]
fn writer_preserves_empty_results_and_rejects_schema_changes() {
    let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, true)]));
    let view = result_batch(
        schema,
        vec![Arc::new(Int64Array::from(Vec::<i64>::new()))],
        vec![],
    );
    let mut output = Vec::new();
    let mut writer = JsonRowsWriter::new(&mut output);
    writer.write_view_batch(&view).unwrap();
    let changed = DataChunkView::nulls(&[DataType::Utf8], 1)
        .unwrap()
        .with_schema(Arc::new(Schema::new(vec![Field::new(
            "n",
            DataType::Utf8,
            true,
        )])))
        .unwrap();
    assert!(writer.write_view_batch(&changed).is_err());
    writer.finish().unwrap();
    assert_eq!(output.iter().filter(|byte| **byte == b'\n').count(), 1);
}

#[test]
fn writer_propagates_output_failure() {
    struct Fails;
    impl std::io::Write for Fails {
        fn write(&mut self, _: &[u8]) -> std::io::Result<usize> {
            Err(std::io::Error::other("output unavailable"))
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, true)]));
    let view = result_batch(schema, vec![Arc::new(Int64Array::from(vec![1]))], vec![0]);
    assert!(JsonRowsWriter::new(Fails).write_view_batch(&view).is_err());
}
