//! Exercises the actual vendored decoder through Vein's Parquet dependency.

use std::sync::Arc;
use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::{
    ParquetRecordBatchReaderBuilder, RowSelection, RowSelector,
    metrics::ArrowReaderMetrics,
};
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;

#[path = "reader_metrics_tests/cache_tests.rs"]
mod cache_tests;

fn fixture() -> Bytes {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("label", DataType::Utf8, true),
    ]));
    let ids = Int64Array::from((0..4096).map(|value| (value % 13 != 0).then_some(value)).collect::<Vec<_>>());
    let labels = StringArray::from((0..4096).map(|value| (value % 7 != 0).then(|| format!("item-{value}"))).collect::<Vec<_>>());
    let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(ids), Arc::new(labels)]).unwrap();
    let properties = WriterProperties::builder().set_compression(Compression::SNAPPY)
        .set_dictionary_enabled(false).set_data_page_row_count_limit(128)
        .set_write_batch_size(128).build();
    let mut bytes = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut bytes, schema, Some(properties)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    Bytes::from(bytes)
}

fn read(bytes: Bytes, metrics: ArrowReaderMetrics, selection: Option<RowSelection>) -> Vec<RecordBatch> {
    let mut builder = ParquetRecordBatchReaderBuilder::try_new(bytes).unwrap()
        .with_batch_size(37).with_metrics(metrics);
    if let Some(selection) = selection { builder = builder.with_row_selection(selection); }
    builder.build().unwrap().collect::<Result<Vec<_>, _>>().unwrap()
}

#[test]
fn decoder_metrics_preserve_values_nulls_and_disabled_fast_path() {
    let bytes = fixture();
    let disabled = ArrowReaderMetrics::disabled();
    let expected = read(bytes.clone(), disabled.clone(), None);
    assert!(disabled.column_metrics().is_empty());
    let enabled = ArrowReaderMetrics::enabled();
    let actual = read(bytes, enabled.clone(), None);
    assert_eq!(actual, expected);
    let columns = enabled.column_metrics();
    assert_eq!(columns.len(), 2);
    for (index, column) in columns.iter().enumerate() {
        assert_eq!(column.column_index, index);
        assert_eq!(column.rows_read, 4096);
        assert!(column.calls[0] > 0);
        assert!(column.calls[2] > 0);
        assert!(column.calls[3] > 0);
        assert!(column.compressed_bytes > 0);
        assert!(column.uncompressed_bytes > 0);
        assert!(column.decompress_ns <= column.read_ns + column.skip_ns + column.materialize_ns);
    }
}

#[test]
fn decoder_metrics_track_selected_and_skipped_records() {
    let bytes = fixture();
    let selection = || RowSelection::from(vec![
        RowSelector::skip(1000), RowSelector::select(37), RowSelector::skip(2000),
        RowSelector::select(1059),
    ]);
    let metrics = ArrowReaderMetrics::enabled();
    let actual = read(bytes.clone(), metrics.clone(), Some(selection()));
    let expected = read(bytes, ArrowReaderMetrics::disabled(), Some(selection()));
    assert_eq!(actual, expected);
    assert_eq!(actual.iter().map(RecordBatch::num_rows).sum::<usize>(), 1096);
    assert!(metrics.column_metrics().iter().all(|column| column.rows_skipped > 0));
}

#[test]
fn failed_decode_does_not_leak_attribution_into_an_unprofiled_reader() {
    let bytes = fixture();
    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes.clone()).unwrap();
    let chunk = builder.metadata().row_group(0).column(0);
    let (start, length) = chunk.byte_range();
    let mut corrupt = bytes.to_vec();
    corrupt[start as usize..(start + length) as usize].fill(0xff);
    let metrics = ArrowReaderMetrics::enabled();
    let mut reader = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(corrupt)).unwrap()
        .with_metrics(metrics.clone()).build().unwrap();
    assert!(reader.next().unwrap().is_err());
    let before = metrics.column_metrics();
    assert!(before.iter().any(|column| column.calls[0] > 0));
    let _ = read(bytes, ArrowReaderMetrics::disabled(), None);
    let after = metrics.column_metrics();
    assert_eq!(before, after);
}

#[test]
fn independent_threads_keep_reader_attribution_separate() {
    let bytes = fixture();
    let readers = (0..2).map(|_| {
        let bytes = bytes.clone();
        std::thread::spawn(move || {
            let metrics = ArrowReaderMetrics::enabled();
            let batches = read(bytes, metrics.clone(), None);
            assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 4096);
            metrics.column_metrics()
        })
    }).collect::<Vec<_>>();
    for reader in readers {
        assert!(reader.join().unwrap().iter().all(|column| column.rows_read == 4096));
    }
}
