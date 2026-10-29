//! Parquet scan execution over provider-assigned files and row-group ranges.

mod file;
mod worker;

use exec::operators::source::scan::ScanRequest;
use exec::operators::source::scan::ScanCursor;
use futures::StreamExt;

/// Executes each assigned file range and publishes scan completion to statistics.
pub(in crate::scan) fn execute_parquet_scan<'a>(request: ScanRequest<'a>) -> ScanCursor<'a> {
    Box::pin(async_stream::try_stream! {
    let io_backend = crate::scan::parquet::reader::ParquetIoBackend::configured();
    let batch_size_policy = crate::scan::parquet::batch_size::RowGroupBatchSizePolicy::default();
    let batch_rows_override = std::env::var("VEIN_SCAN_BATCH_ROWS").ok();
    let mut adaptive_order = request.worker_state.with::<worker::AdaptiveWorkerState, _>(
        |state| state.order.clone(),
    )?;
    tracing::debug!(target: "vein.parquet_adaptive_filter",
        event = "parquet_adaptive_filter_work_unit_started",
        worker_id = request.worker_state.diagnostic_id(),
        scan_id = ?request.statistics_scan.map(|scan| scan.scan_id),
        prior_observations = adaptive_order.as_ref().map_or(0, |order| order.snapshot().observations),
        "scan work unit reuses only its owning worker's predicate observations");
    for file in request.files {
        let mut cursor = file::execute(file::FileExecution {
            request: &request,
            file,
            adaptive_order: &mut adaptive_order,
            io_backend,
            batch_size_policy: &batch_size_policy,
            batch_rows_override: batch_rows_override.as_deref(),
        });
        while let Some(view) = cursor.next().await { yield view?; }
        drop(cursor);
        request.worker_state.with::<worker::AdaptiveWorkerState, _>(|state| {
            state.order.clone_from(&adaptive_order);
        })?;
    }
    if let Some(scan) = request.statistics_scan {
        let queued = request.runtime_statistics.enqueue_partition_completion(
            common::statistics::runtime::RuntimeStatisticsPartitionCompletion {
                scan_id: scan.scan_id,
                table_key: &scan.table_key,
                expected_rows: scan.expected_rows,
                expected_partitions: scan.partition_count,
                table_scan_candidate: scan.row_coverage_candidate,
                partition_index: scan.partition_index,
                query_context: request.query_statistics.cloned(),
            },
        );
        if !queued {
            tracing::debug!(
                target: "vein.audit",
                event = "runtime_statistics_completion_unavailable",
                scan_id = scan.scan_id,
                "runtime statistics completion notification was unavailable"
            );
        }
    }
    })
}
