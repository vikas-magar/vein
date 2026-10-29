//! Coordinates one file through Parquet preparation, pruning, and streaming.

mod audit;
mod predicates;
mod prepare;
mod pruning;

use std::sync::Arc;
use common::time::Instant;

use exec::operators::source::scan::{ScanFile, ScanRequest};
use exec::operators::source::scan::ScanCursor;
use futures::StreamExt;

use crate::scan::parquet::batch_size::RowGroupBatchSizePolicy;
use crate::scan::parquet::metrics::ScanPredicateMetrics;
use crate::scan::parquet::predicate::AdaptiveFilterOrder;
use crate::scan::parquet::reader::ParquetIoBackend;
use crate::scan::parquet::stream::{ScanStreamContext, consume_batches};

pub(super) struct FileExecution<'a, 'q> {
    pub(super) request: &'a ScanRequest<'q>,
    pub(super) file: &'q ScanFile,
    pub(super) adaptive_order: &'a mut Option<Arc<AdaptiveFilterOrder>>,
    pub(super) io_backend: ParquetIoBackend,
    pub(super) batch_size_policy: &'a RowGroupBatchSizePolicy,
    pub(super) batch_rows_override: Option<&'a str>,
}

pub(super) fn execute<'a, 'q: 'a>(context: FileExecution<'a, 'q>) -> ScanCursor<'a> {
    Box::pin(async_stream::try_stream! {
    let FileExecution {
        request,
        file,
        adaptive_order,
        io_backend,
        batch_size_policy,
        batch_rows_override,
    } = context;
    let path = file.relative.as_str();
    let scan_started = Instant::now();
    let detailed_metrics_enabled =
        tracing::enabled!(target: "vein.audit", tracing::Level::DEBUG);
    let predicate_metrics = Arc::new(ScanPredicateMetrics::default());
    let pushed_filter_columns = pushed_filter_columns(request);
    let runtime_filter_names = request.runtime_filters.names();
    audit::log_started(
        path,
        io_backend,
        request.pushdown,
        &pushed_filter_columns,
        &runtime_filter_names,
        detailed_metrics_enabled,
    );

    let prepared = prepare::prepare(
        request,
        file,
        io_backend,
        batch_size_policy,
        batch_rows_override,
        detailed_metrics_enabled,
    )
        .await?;
    let prepare::PreparedScan {
        mut builder,
        reader_metrics,
        projection,
        io,
        bytes_before_file,
        file_size_bytes,
        stat_us,
        metadata_started,
        footer_hit,
        detailed_metrics_enabled,
    } = prepared;

    let pruning = pruning::apply(
        &mut builder,
        pruning::PruningContext {
            path,
            file,
            storage: request.storage,
            projected: request.projected,
            pushdown: request.pushdown,
            runtime_filters: request.runtime_filters,
            bytes_read: request.bytes_read,
            io: &io,
            io_backend,
            file_size_bytes,
        },
    )
        .await?;
    if let Some(selections) = pruning.selections {
        builder = if selections.is_empty() {
            builder.with_row_groups(Vec::new())
        } else {
            builder.with_row_group_selections(selections)
        };
    }

    let (builder, predicate_setup) = predicates::install(
        builder,
        request,
        path,
        detailed_metrics_enabled,
        &predicate_metrics,
        adaptive_order,
    )?;
    let metadata_us = metadata_started.elapsed().as_micros() as u64;
    let file_rows = builder.metadata().file_metadata().num_rows();
    let build_started = Instant::now();
    let stream = builder
        .build()
        .map_err(|error| error::VeinError::internal(format!(
            "Failed to build parquet stream: {error}"
        )))?;
    let build_us = build_started.elapsed().as_micros() as u64;

    audit::log_ready(audit::ReadyScan {
        path,
        projection: &projection,
        summary: &pruning.summary,
        pushdown: request.pushdown,
        pushed_filter_columns: &pushed_filter_columns,
        runtime_filters: request.runtime_filters,
        runtime_filter_names: &runtime_filter_names,
        footer_hit,
        file_size_bytes,
        file_rows,
        detailed_metrics_enabled,
        stat_us,
        metadata_us,
        build_us,
    });
    let mut stream_metrics = crate::scan::parquet::stream::ScanStreamMetrics::default();
    let mut cursor = consume_batches(
        stream,
        ScanStreamContext {
            path,
            output_positions: &projection.output_positions,
            scan_started,
            bytes_read: request.bytes_read.as_ref(),
            bytes_before_file,
            file_size_bytes,
            io: io.as_ref(),
            detailed_metrics_enabled,
            has_row_filter: predicate_setup.has_row_filter,
            predicate_metrics: predicate_metrics.as_ref(),
            statistics_scan: request.statistics_scan,
            runtime_statistics: request.runtime_statistics,
        },
        &mut stream_metrics,
    );
    while let Some(view) = cursor.next().await { yield view?; }
    drop(cursor);
    audit::log_completed(audit::CompletedScan {
        path,
        scan_started,
        bytes_before_file,
        bytes_read: request.bytes_read.as_ref(),
        file_size_bytes,
        io: io.as_ref(),
        stream: &stream_metrics,
        predicate_metrics: predicate_metrics.as_ref(),
        reader_metrics: &reader_metrics,
        stage_metrics: &predicate_setup.stage_metrics,
        adaptive_order: adaptive_order.as_ref(),
        summary: &pruning.summary,
        pushdown: request.pushdown,
        pushed_filter_columns: &pushed_filter_columns,
        runtime_statistics_enabled: request.statistics_scan.is_some(),
        detailed_metrics_enabled,
        footer_hit,
        stat_us,
        metadata_us,
        build_us,
        file_rows,
    });
    })
}

fn pushed_filter_columns<'query>(request: &ScanRequest<'query>) -> Vec<&'query str> {
    let mut columns = Vec::new();
    if let Some(predicate) = request.pushdown {
        for column in predicate.stages.iter().flat_map(|stage| &stage.input.fields) {
            if !columns.contains(&column.name.as_str()) {
                columns.push(column.name.as_str());
            }
        }
    }
    columns
}
