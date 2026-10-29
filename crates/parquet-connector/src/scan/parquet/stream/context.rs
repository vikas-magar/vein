use std::sync::atomic::AtomicU64;
use common::time::Instant;

use common::protocol::RuntimeStatisticsScan;
use common::statistics::RuntimeStatisticsStore;

use crate::scan::parquet::metrics::ScanPredicateMetrics;
use crate::scan::parquet::reader::IoStats;

pub(in crate::scan::parquet) struct ScanStreamContext<'a> {
    pub(in crate::scan::parquet) path: &'a str,
    pub(in crate::scan::parquet) output_positions: &'a [usize],
    pub(in crate::scan::parquet) scan_started: Instant,
    pub(in crate::scan::parquet) bytes_read: &'a AtomicU64,
    pub(in crate::scan::parquet) bytes_before_file: u64,
    pub(in crate::scan::parquet) file_size_bytes: u64,
    pub(in crate::scan::parquet) io: &'a IoStats,
    pub(in crate::scan::parquet) detailed_metrics_enabled: bool,
    pub(in crate::scan::parquet) has_row_filter: bool,
    pub(in crate::scan::parquet) predicate_metrics: &'a ScanPredicateMetrics,
    pub(in crate::scan::parquet) statistics_scan: Option<&'a RuntimeStatisticsScan>,
    pub(in crate::scan::parquet) runtime_statistics: &'a RuntimeStatisticsStore,
}
