//! Parquet scan execution over provider-assigned files and row-group ranges.

mod file;

use common::Result;
use exec::operators::source::scan::ScanRequest;
use exec::pipeline::PipelineNode;

/// Executes each assigned file range and publishes scan completion to statistics.
pub(in crate::scan) async fn execute_parquet_scan(
    request: ScanRequest<'_>,
    next: &mut dyn PipelineNode,
) -> Result<()> {
    let io_backend = crate::scan::parquet::reader::ParquetIoBackend::configured();
    let batch_size_policy = crate::scan::parquet::batch_size::RowGroupBatchSizePolicy::default();
    let batch_rows_override = std::env::var("VEIN_SCAN_BATCH_ROWS").ok();
    let mut adaptive_order = None;
    for file in request.files {
        file::execute(file::FileExecution {
            request: &request,
            file,
            next,
            adaptive_order: &mut adaptive_order,
            io_backend,
            batch_size_policy: &batch_size_policy,
            batch_rows_override: batch_rows_override.as_deref(),
        })
            .await?;
        if next.finished() {
            break;
        }
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
    Ok(())
}
