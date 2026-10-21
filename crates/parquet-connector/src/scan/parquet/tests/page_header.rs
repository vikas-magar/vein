use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::file::metadata::{PageIndexPolicy, ParquetMetaDataReader};
use parquet::file::properties::{EnabledStatistics, WriterProperties};

use crate::scan::parquet::page_header_reader::{
    PageHeaderReadRequest, read_page_header_statistics,
};
use crate::scan::parquet::page_pruning::{
    attach_header_page_locations, page_payload_pruning_metrics,
};
use crate::scan::parquet::reader::{IoStats, ParquetIoBackend};
use storage::{
    ReadStorageFactory, StorageLocation, opendal::OpenDalReadStorageFactory,
};

#[path = "page_header/decoder.rs"]
mod decoder;

#[tokio::test]
async fn page_header_statistics_prune_flat_data_pages_without_dropping_matches() {
    let root = tempfile::tempdir().expect("temporary Parquet root");
    let relative_path = "page_headers.parquet";
    let path = root.path().join(relative_path);
    let schema = Arc::new(Schema::new(vec![
        Field::new("value", DataType::Int64, false),
        Field::new("payload", DataType::Int64, false),
    ]));
    let properties = WriterProperties::builder()
        .set_data_page_size_limit(256)
        .set_write_batch_size(32)
        .set_statistics_enabled(EnabledStatistics::Page)
        .set_write_page_header_statistics(true)
        .build();
    let file = std::fs::File::create(&path).expect("create Parquet file");
    let mut writer = ArrowWriter::try_new(file, Arc::clone(&schema), Some(properties))
        .expect("create Parquet writer");
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from_iter_values(0_i64..8192)),
            Arc::new(Int64Array::from_iter_values((0_i64..8192).map(|value| value * 2))),
        ],
    )
        .expect("valid sorted values");
    writer.write(&batch).expect("write values");
    writer.close().expect("finish Parquet file");

    let file_size = std::fs::metadata(&path)
        .expect("read Parquet file size")
        .len();
    let metadata = ParquetMetaDataReader::new()
        .with_page_index_policy(PageIndexPolicy::Skip)
        .parse_and_finish(&std::fs::File::open(&path).expect("open Parquet footer"))
        .expect("parse footer without page indexes");
    assert_eq!(metadata.num_row_groups(), 1);
    assert!(metadata.page_index().is_none());

    let storage = OpenDalReadStorageFactory
        .open(&StorageLocation::local(root.path().to_path_buf()))
        .expect("create local filesystem storage");
    let bytes_read = Arc::new(AtomicU64::new(0));
    let io = Arc::new(IoStats::default());
    let mut plan = crate::parquet_statistics::PageHeaderReadPlan::default();
    plan.request(0, "value");
    let page_headers = read_page_header_statistics(PageHeaderReadRequest {
        storage: &storage,
        path: relative_path,
        absolute_path: &path,
        backend: ParquetIoBackend::Storage,
        file_size_bytes: file_size,
        bytes_read: &bytes_read,
        io: &io,
        arrow_schema: schema.as_ref(),
        metadata: &metadata,
        plan: &plan,
    })
        .await;

    assert_eq!(page_headers.metrics.failed_chunks, 0);
    assert_eq!(page_headers.metrics.unsupported_row_mapping_chunks, 0);
    assert!(page_headers.metrics.pages_scanned > 1);
    assert_eq!(
        page_headers.metrics.pages_with_statistics,
        page_headers.metrics.pages_scanned,
        "the fixture explicitly writes statistics into every page header"
    );
    assert_eq!(
        page_headers.metrics.pages_with_complete_bounds,
        page_headers.metrics.pages_scanned,
        "the fixture uses exact Int64 min/max bounds"
    );
    assert!(page_headers.metrics.header_bytes > 0);
    assert!(page_headers.metrics.probe_bytes >= page_headers.metrics.header_bytes);
    assert!(bytes_read.load(Ordering::Relaxed) >= page_headers.metrics.probe_bytes);

    let pages = page_headers
        .pages
        .get(&0)
        .and_then(|columns| columns.get("value"))
        .expect("complete flat column page mapping");
    assert_eq!(pages.len(), page_headers.metrics.pages_scanned as usize);
    assert_eq!(pages.first().expect("first data page").first_row, 0);
    let page_locations = page_headers
        .page_locations
        .get(&0)
        .and_then(|columns| columns.get(&0))
        .expect("header scan returns byte locations for the flat column");
    assert_eq!(page_locations.len(), pages.len());
    assert_eq!(page_locations.first().expect("first data page").first_row_index, 0);
    assert_eq!(
        pages.iter().map(|page| page.row_count).sum::<usize>(),
        8192,
        "page row spans must cover the row group exactly"
    );

    let target = 4097_i64;
    let predicate = catalog::StatisticsPredicate::Comparison {
        column: "value".to_string(),
        comparison: catalog::StatisticsComparison::Equal,
        literal: types::LogicalLiteral::Int64(target),
    };
    let without_header_statistics =
        crate::parquet_statistics::prune_data_pages_with_headers(
            schema.as_ref(),
            &metadata,
            std::slice::from_ref(&predicate),
            &[0],
            &crate::parquet_statistics::PageHeaderStatistics::new(),
        );
    assert_eq!(without_header_statistics.rows_skipped, 0);
    assert!(without_header_statistics.selections[0].selection().is_none());

    let pruning = crate::parquet_statistics::prune_data_pages_with_headers(
        schema.as_ref(),
        &metadata,
        std::slice::from_ref(&predicate),
        &[0],
        &page_headers.pages,
    );
    assert!(pruning.row_ranges_skipped > 0);
    assert!(pruning.rows_skipped > 0);
    let row_group_selection = attach_header_page_locations(
        pruning.selections,
        &page_headers.page_locations,
    )
        .into_iter()
        .next()
        .expect("one row group survives the page-header pruning");
    let payload_metrics = page_payload_pruning_metrics(
        std::slice::from_ref(&row_group_selection),
        &page_headers.page_locations,
    );
    assert!(payload_metrics.data_pages_skipped > 0);
    assert!(payload_metrics.skipped_payload_bytes > 0);
    let selection = decoder::verify_selected_page_reads(
        &path,
        &metadata,
        row_group_selection,
        &page_headers.page_locations,
        payload_metrics.selected_payload_bytes,
        payload_metrics.skipped_payload_bytes,
        target,
    );

    let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
        std::fs::File::open(&path).expect("reopen Parquet file"),
    )
        .expect("build selected reader")
        .with_row_selection(selection)
        .build()
        .expect("apply page-derived row selection");
    let matches = reader
        .flat_map(|batch| {
            let batch = batch.expect("read selected rows");
            let values = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("Int64 output");
            (0..values.len())
                .map(|row| values.value(row))
                .filter(|value| *value == target)
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    assert_eq!(matches, vec![target]);
}
