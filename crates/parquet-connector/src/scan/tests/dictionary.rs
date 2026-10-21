use std::sync::Arc;

use super::support::{ArrayTypeSink, scan_config, scan_with_config, write_string_parquet};
use exec::pipeline::Source;
use arrow::datatypes::DataType;

#[tokio::test]
async fn scan_preserves_dictionary_encoding_when_requested() {
    let dir = tempfile::tempdir().expect("temporary data root");
    let path = write_string_parquet(dir.path(), "part-0.parquet");
    let mut config = scan_config(dir.path(), &[("part-0.parquet", path)]);
    config.projected = Arc::new(types::VeinSchema::new(vec![Arc::new(
        types::ResolvedColumn::new(
            types::ColumnId(0),
            "status",
            None,
            types::VeinDataType::Utf8,
        ),
    )]));
    config.column_encoding_hints = vec![common::protocol::ColumnEncodingHint::dictionary(0)];
    let mut scan = scan_with_config(config);
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
