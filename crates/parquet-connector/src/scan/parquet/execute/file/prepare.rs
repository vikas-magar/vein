//! Builds a projected Parquet stream for one file range.

use std::sync::Arc;

use common::Result;
use error::VeinError;
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::metrics::ArrowReaderMetrics;
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
use parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder;

use exec::operators::source::scan::{ScanFile, ScanRequest};

use crate::scan::parquet::batch_size::{RowGroupBatchSizePolicy, ScanBatchSizePolicy};
use crate::scan::parquet::file::prepare_parquet_file;
use crate::scan::parquet::projection::{
    ScanProjection, dictionary_column_indices, dictionary_schema_hint, scan_projection,
};
use crate::scan::parquet::reader::{IoStats, ParquetIoBackend};

use super::pruning::ParquetScanBuilder;

/// File-local state retained between preparation and stream execution.
pub(super) struct PreparedScan {
    pub(super) builder: ParquetScanBuilder,
    pub(super) reader_metrics: ArrowReaderMetrics,
    pub(super) projection: ScanProjection,
    pub(super) io: Arc<IoStats>,
    pub(super) bytes_before_file: u64,
    pub(super) file_size_bytes: u64,
    pub(super) stat_us: u64,
    pub(super) metadata_started: common::time::Instant,
    pub(super) footer_hit: bool,
    pub(super) detailed_metrics_enabled: bool,
}

/// Opens footer metadata once, configures projection, and selects the batch size.
pub(super) async fn prepare(
    request: &ScanRequest<'_>,
    file: &ScanFile,
    io_backend: ParquetIoBackend,
    batch_size_policy: &RowGroupBatchSizePolicy,
    batch_rows_override: Option<&str>,
    detailed_metrics_enabled: bool,
) -> Result<PreparedScan> {
    let path = file.relative.as_str();
    let bytes_before_file = request.bytes_read.load(std::sync::atomic::Ordering::Relaxed);
    let io = Arc::new(IoStats::default());
    let prepared_file = prepare_parquet_file(
        request.storage,
        file,
        io_backend,
        request.bytes_read,
        Arc::clone(&io),
        request.metadata_store,
    )
        .await?;
    let dictionary_columns = dictionary_column_indices(request.column_encoding_hints);
    let options = if dictionary_columns.is_empty() {
        ArrowReaderOptions::new()
    } else {
        let metadata = ArrowReaderMetadata::try_new(
            Arc::clone(&prepared_file.metadata),
            ArrowReaderOptions::new(),
        )
            .map_err(|error| {
                VeinError::internal(format!(
                    "Failed to infer Parquet schema for '{path}': {error}"
                ))
            })?;
        let schema = dictionary_schema_hint(
            metadata.schema(),
            request.projected,
            &dictionary_columns,
        )?;
        ArrowReaderOptions::new().with_schema(schema)
    };
    let arrow_metadata = ArrowReaderMetadata::try_new(prepared_file.metadata, options)
        .map_err(|error| {
            VeinError::internal(format!(
                "Failed to configure Parquet reader for '{path}': {error}"
            ))
        })?;
    let mut builder = ParquetRecordBatchStreamBuilder::new_with_metadata(
        prepared_file.reader,
        arrow_metadata,
    );
    let reader_metrics = if detailed_metrics_enabled {
        ArrowReaderMetrics::enabled()
    } else {
        ArrowReaderMetrics::disabled()
    };
    builder = builder.with_metrics(reader_metrics.clone());
    let projection = scan_projection(
        builder.schema(),
        request.projected,
        request.pushdown,
        request.runtime_filters,
        path,
    )?;
    let output_mask =
        ProjectionMask::roots(builder.parquet_schema(), projection.output_roots.clone());
    builder = builder.with_projection(output_mask);

    let row_groups_total = builder.metadata().num_row_groups();
    let assigned_range = file
        .scan_unit_range
        .clone()
        .unwrap_or(0..row_groups_total);
    let assigned_count = assigned_range
        .end
        .min(row_groups_total)
        .saturating_sub(assigned_range.start.min(row_groups_total));
    let assigned_rows = (assigned_range.start.min(row_groups_total)
        ..assigned_range.end.min(row_groups_total))
        .map(|index| builder.metadata().row_group(index).num_rows());
    let batch_size = batch_size_policy.select(assigned_rows, batch_rows_override);
    builder = builder.with_batch_size(batch_size.rows);
    if file
        .scan_unit_range
        .as_ref()
        .is_none_or(|range| range.start == 0)
    {
        tracing::debug!(
            target: "vein.scan_batch_size",
            event = "parquet_batch_size_selected",
            source_id = %common::logging::LogLocation::from(path),
            batch_rows = batch_size.rows,
            source = batch_size.source.as_str(),
            assigned_row_groups = assigned_count,
            largest_assigned_row_group_rows = batch_size.largest_assigned_row_group.unwrap_or(0),
            invalid_override = batch_size.invalid_override,
            "selected Parquet scan batch size for first range in file"
        );
    }

    Ok(PreparedScan {
        builder,
        reader_metrics,
        projection,
        io,
        bytes_before_file,
        file_size_bytes: prepared_file.file_size_bytes,
        stat_us: prepared_file.stat_us,
        metadata_started: prepared_file.metadata_started,
        footer_hit: prepared_file.footer_hit,
        detailed_metrics_enabled,
    })
}
