use crate::scan::parquet::reader::ParquetIoBackend;
use common::protocol::ScanPredicate;

pub(in crate::scan::parquet::execute::file) fn log_started(
    path: &str,
    backend: ParquetIoBackend,
    pushdown: Option<&ScanPredicate>,
    pushed_filter_columns: &[&str],
    runtime_filter_names: &[&str],
    detailed_metrics_enabled: bool,
) {
    tracing::debug!(
        target: "vein.audit",
        event = "scan_started",
        path = %path,
        format = "parquet",
        io_backend = backend.name(),
        pushed_filter_count = pushdown.map_or(0, |predicate| predicate.predicate_count),
        pushed_filter_stage_count = pushdown.map_or(0, |predicate| predicate.stages.len()),
        pushed_filter_columns = ?pushed_filter_columns,
        runtime_filter_count = runtime_filter_names.len(),
        runtime_filter_names = ?runtime_filter_names,
        detailed_metrics_enabled,
        "scan_started"
    );
}
