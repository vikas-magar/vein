use super::*;
use arrow::datatypes::DataType;

#[tokio::test]
async fn scan_preserves_dictionary_encoding_when_requested() {
    let dir = tempfile::tempdir().expect("temporary data root");
    let path = write_string_parquet(dir.path(), "part-0.parquet");
    let mut scan = scan_over(dir.path(), &[("part-0.parquet", path)]);
    scan.projected = Arc::new(types::VeinSchema::new(vec![Arc::new(
        types::ResolvedColumn::new(
            types::ColumnId(0),
            "status",
            None,
            types::VeinDataType::Utf8,
        ),
    )]));
    scan.dictionary_columns = vec![0];
    let mut sink = ArrayTypeSink::default();

    scan.execute(&mut sink).await.expect("scan dictionary column");

    assert_eq!(
        sink.0,
        Some(DataType::Dictionary(
            Box::new(DataType::Int32),
            Box::new(DataType::Utf8)
        ))
    );
}
