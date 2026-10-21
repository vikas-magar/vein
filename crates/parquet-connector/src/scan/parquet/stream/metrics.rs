use std::time::Duration;

#[derive(Default)]
pub(crate) struct ScanStreamMetrics {
    pub(crate) total_rows: usize,
    pub(crate) physical_rows_read: usize,
    pub(crate) batches: u64,
    pub(crate) output_source_array_count: u64,
    pub(crate) output_source_array_bytes: u64,
    pub(crate) parquet_read_time: Duration,
    pub(crate) first_poll_time: Duration,
    pub(crate) rest_poll_time: Duration,
    pub(crate) downstream_time: Duration,
    pub(crate) first_batch_us: u64,
    pub(crate) poll_thread_cpu: Duration,
    pub(crate) poll_count: u64,
    pub(crate) first_poll_io_us: u64,
    pub(crate) first_poll_thread_cpu: Duration,
    pub(crate) first_poll_polls: u64,
    pub(crate) statistics_enqueue_us: u64,
    pub(crate) statistics_batches_queued: u64,
    pub(crate) statistics_batches_queue_full: u64,
    pub(crate) statistics_batches_too_large: u64,
    pub(crate) statistics_batches_worker_unavailable: u64,
}
