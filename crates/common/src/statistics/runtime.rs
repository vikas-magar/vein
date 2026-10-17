//! Thread-safe runtime synopsis collection over batches already read by a source.
//!
//! Scan operators enqueue shallow Arrow batch handles into a bounded worker
//! queue; enqueueing never copies column buffers or waits for synopsis work.
//! Completed observations are published only when row coverage is proven against
//! the pinned source snapshot. Column coverage remains explicit in the synopsis.

use std::collections::{HashMap, HashSet, hash_map::Entry};
use std::fmt;
use std::sync::{Arc, Mutex, mpsc};
use std::sync::mpsc::{SyncSender, TrySendError};
use std::thread;
use std::time::Duration;

use arrow::record_batch::RecordBatch;
use catalog::{CatalogSnapshotToken, TableStatistics};

use super::{ArrowScalarEncoder, HyperLogLog, StatisticsCollector, StatisticsConfig, TableStatisticsLog};
use super::QueryStatisticsContext;
use crate::{Result, VeinError};

/// Runtime synopsis fields tracked in operator and planner diagnostics.
pub const RUNTIME_STATISTIC_METRICS: [&str; 11] = [
    "null_count",
    "distinct_count",
    "distinct_sketch",
    "distinct_count_relative_error",
    "nan_count",
    "min_max",
    "min_max_complete",
    "numeric_moments",
    "byte_length",
    "most_common_values",
    "histogram",
];

/// Reports which standard synopsis fields are populated for a column.
pub fn runtime_metric_availability(column: &catalog::ColumnStatistics) -> [bool; 11] {
    [
        column.null_count.is_some(),
        column.distinct_count.is_some(),
        column.distinct_sketch.is_some(),
        column.distinct_count_relative_error.is_some(),
        column.nan_count.is_some(),
        column.min_value.is_some() && column.max_value.is_some(),
        column.min_max_complete.is_some(),
        column.numeric.is_some(),
        column.byte_length.is_some(),
        column.most_common_values.is_some(),
        column.histogram.is_some(),
    ]
}

/// Build a process-stable key for runtime statistics tied to a pinned source snapshot.
pub fn table_statistics_key(snapshot: &CatalogSnapshotToken) -> String {
    let mut identity = Vec::new();
    for value in [
        snapshot.table_id.as_str(),
        snapshot.catalog_revision.as_str(),
        snapshot.snapshot_id.as_str(),
    ] {
        identity.extend_from_slice(&(value.len() as u64).to_le_bytes());
        identity.extend_from_slice(value.as_bytes());
    }
    identity.extend_from_slice(&snapshot.schema_version.to_le_bytes());
    format!("{:016x}", super::stable_hash(0xfe, &identity))
}

/// Shared destination for complete runtime statistics and in-flight scans.
#[derive(Clone)]
pub struct RuntimeStatisticsStore {
    state: Arc<Mutex<StoreState>>,
    worker: Option<Arc<RuntimeStatisticsWorker>>,
}

/// Bounds background synopsis work. Oversized batches and a full queue are
/// skipped so statistics can never apply backpressure to query execution.
#[derive(Debug, Clone, Copy)]
pub struct RuntimeStatisticsConfig {
    /// Maximum number of Arrow batch handles waiting for synopsis processing.
    pub queue_capacity: usize,
    /// Maximum retained array bytes for any one queued batch.
    pub max_batch_bytes: usize,
}

impl Default for RuntimeStatisticsConfig {
    fn default() -> Self {
        Self {
            queue_capacity: 2,
            max_batch_bytes: 8 * 1024 * 1024,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Outcome of attempting to schedule best-effort runtime statistics work.
pub enum RuntimeStatisticsEnqueue {
    /// Batch handle was queued for the worker.
    Queued,
    /// The bounded worker queue was full; query execution continues.
    QueueFull,
    /// Batch exceeded the configured retained-byte limit.
    BatchTooLarge,
    /// The worker is absent or has stopped; query execution continues.
    WorkerUnavailable,
}

struct RuntimeStatisticsWorker {
    batches: SyncSender<StatisticsWork>,
    controls: mpsc::Sender<StatisticsControl>,
    config: RuntimeStatisticsConfig,
}

enum StatisticsWork {
    Batch {
        scan_id: u128,
        table_key: String,
        expected_rows: Option<u64>,
        expected_partitions: usize,
        table_scan_candidate: bool,
        batch: RecordBatch,
    },
}

enum StatisticsControl {
    Complete {
        scan_id: u128,
        table_key: String,
        expected_rows: Option<u64>,
        expected_partitions: usize,
        table_scan_candidate: bool,
        partition_index: usize,
        query_context: Option<Arc<QueryStatisticsContext>>,
    },
    Flush(mpsc::Sender<()>),
}

impl Default for RuntimeStatisticsStore {
    fn default() -> Self {
        Self::with_config(RuntimeStatisticsConfig::default())
            .expect("default runtime statistics worker configuration is valid")
    }
}

impl fmt::Debug for RuntimeStatisticsStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.state.lock() {
            Ok(state) => formatter
                .debug_struct("RuntimeStatisticsStore")
                .field("in_flight_scans", &state.scans.len())
                .field("complete_tables", &state.complete_tables.len())
                .finish(),
            Err(_) => formatter
                .debug_struct("RuntimeStatisticsStore")
                .field("state", &"poisoned")
                .finish(),
        }
    }
}

#[derive(Default)]
struct StoreState {
    scans: HashMap<u128, Arc<Mutex<ScanObservation>>>,
    complete_tables: HashMap<String, Arc<TableStatistics>>,
}

struct ScanObservation {
    table_key: String,
    expected_rows: Option<u64>,
    expected_partitions: usize,
    table_scan_candidate: bool,
    completed_partitions: HashSet<usize>,
    collector: Option<StatisticsCollector<HyperLogLog, ArrowScalarEncoder>>,
    finalized: bool,
}

impl ScanObservation {
    fn new(
        table_key: String,
        expected_rows: Option<u64>,
        expected_partitions: usize,
        table_scan_candidate: bool,
    ) -> Result<Self> {
        if expected_partitions == 0 {
            return Err(VeinError::internal(
                "runtime statistics scan must contain at least one partition",
            ));
        }
        Ok(Self {
            table_key,
            expected_rows,
            expected_partitions,
            table_scan_candidate,
            completed_partitions: HashSet::with_capacity(expected_partitions),
            collector: None,
            finalized: false,
        })
    }

    fn validate_context(
        &self,
        table_key: &str,
        expected_rows: Option<u64>,
        expected_partitions: usize,
        table_scan_candidate: bool,
    ) -> Result<()> {
        if self.table_key != table_key
            || self.expected_rows != expected_rows
            || self.expected_partitions != expected_partitions
            || self.table_scan_candidate != table_scan_candidate
        {
            return Err(VeinError::internal(
                "runtime statistics scan context changed between partitions",
            ));
        }
        Ok(())
    }
}

/// Final synopsis and coverage classification for one executed scan.
#[derive(Debug, Clone)]
pub struct RuntimeStatisticsObservation {
    /// Stable source identity selected by the planner.
    pub table_key: String,
    /// Statistics computed from batches emitted by this scan.
    pub statistics: Arc<TableStatistics>,
    /// Whether all source rows were observed and the synopsis may guide CBO.
    /// `statistics.column_statistics` can still contain only projected columns.
    pub complete_table_snapshot: bool,
    /// Expected source rows, when catalog metadata supplied an exact count.
    pub expected_rows: Option<u64>,
}

impl RuntimeStatisticsStore {
    /// Create a query-scoped store with bounded, best-effort background
    /// collection. Batch handles are shallow Arrow clones; the configured byte
    /// and item limits bound how much source memory the worker can retain.
    pub fn with_config(config: RuntimeStatisticsConfig) -> Result<Self> {
        if config.queue_capacity == 0 || config.max_batch_bytes == 0 {
            return Err(VeinError::internal(
                "runtime statistics queue capacity and batch limit must be nonzero",
            ));
        }
        let state = Arc::new(Mutex::new(StoreState::default()));
        let (batch_tx, batch_rx) = mpsc::sync_channel(config.queue_capacity);
        let (control_tx, control_rx) = mpsc::channel();
        let worker_state = Arc::clone(&state);
        thread::Builder::new()
            .name("vein-runtime-statistics".to_owned())
            .spawn(move || {
                let store = RuntimeStatisticsStore {
                    state: worker_state,
                    worker: None,
                };
                loop {
                    match control_rx.try_recv() {
                        Ok(control) => {
                            drain_statistics_batches(&store, &batch_rx);
                            if handle_statistics_control(&store, control).is_err() {
                                tracing::warn!(target: "vein.audit", event = "runtime_statistics_worker_error", "runtime statistics control processing failed");
                            }
                            continue;
                        }
                        Err(mpsc::TryRecvError::Disconnected) => {
                            drain_statistics_batches(&store, &batch_rx);
                            break;
                        }
                        Err(mpsc::TryRecvError::Empty) => {}
                    }
                    match batch_rx.recv_timeout(Duration::from_millis(10)) {
                        Ok(work) => process_statistics_batch(&store, work),
                        Err(mpsc::RecvTimeoutError::Timeout) => {}
                        Err(mpsc::RecvTimeoutError::Disconnected) => {
                            if control_rx.try_recv().is_err() {
                                break;
                            }
                        }
                    }
                }
            })
            .map_err(|error| VeinError::internal(format!("failed to start runtime statistics worker: {error}")))?;

        Ok(Self {
            state,
            worker: Some(Arc::new(RuntimeStatisticsWorker {
                batches: batch_tx,
                controls: control_tx,
                config,
            })),
        })
    }

    /// Queue an already-read batch for asynchronous statistics collection.
    /// This method never waits for the worker and never copies Arrow buffers.
    pub fn enqueue_batch(
        &self,
        scan_id: u128,
        table_key: &str,
        expected_rows: Option<u64>,
        expected_partitions: usize,
        table_scan_candidate: bool,
        batch: &RecordBatch,
    ) -> RuntimeStatisticsEnqueue {
        let Some(worker) = &self.worker else {
            return RuntimeStatisticsEnqueue::WorkerUnavailable;
        };
        if batch.get_array_memory_size() > worker.config.max_batch_bytes {
            return RuntimeStatisticsEnqueue::BatchTooLarge;
        }
        let work = StatisticsWork::Batch {
            scan_id,
            table_key: table_key.to_owned(),
            expected_rows,
            expected_partitions,
            table_scan_candidate,
            batch: batch.clone(),
        };
        match worker.batches.try_send(work) {
            Ok(()) => RuntimeStatisticsEnqueue::Queued,
            Err(TrySendError::Full(_)) => RuntimeStatisticsEnqueue::QueueFull,
            Err(TrySendError::Disconnected(_)) => RuntimeStatisticsEnqueue::WorkerUnavailable,
        }
    }

    /// Queue partition completion without waiting for synopsis work.
    pub fn enqueue_partition_completion(
        &self,
        scan_id: u128,
        table_key: &str,
        expected_rows: Option<u64>,
        expected_partitions: usize,
        table_scan_candidate: bool,
        partition_index: usize,
        query_context: Option<Arc<QueryStatisticsContext>>,
    ) -> bool {
        self.worker.as_ref().is_some_and(|worker| {
            worker.controls.send(StatisticsControl::Complete {
                scan_id,
                table_key: table_key.to_owned(),
                expected_rows,
                expected_partitions,
                table_scan_candidate,
                partition_index,
                query_context,
            }).is_ok()
        })
    }

    fn flush_pending(&self) {
        let Some(worker) = &self.worker else { return };
        let (ack_tx, ack_rx) = mpsc::channel();
        if worker.controls.send(StatisticsControl::Flush(ack_tx)).is_ok() {
            let _ = ack_rx.recv();
        }
    }

    /// Observe a batch without retaining or copying its Arrow arrays.
    pub fn observe_batch(
        &self,
        scan_id: u128,
        table_key: &str,
        expected_rows: Option<u64>,
        expected_partitions: usize,
        table_scan_candidate: bool,
        batch: &RecordBatch,
    ) -> Result<()> {
        let entry = {
            let mut state = self
                .state
                .lock()
                .map_err(|_| VeinError::internal("runtime statistics store mutex poisoned"))?;
            match state.scans.entry(scan_id) {
                Entry::Occupied(entry) => Arc::clone(entry.get()),
                Entry::Vacant(entry) => {
                    let observation = ScanObservation::new(
                        table_key.to_owned(),
                        expected_rows,
                        expected_partitions,
                        table_scan_candidate,
                    )?;
                    Arc::clone(entry.insert(Arc::new(Mutex::new(observation))))
                }
            }
        };

        let mut observation = entry
            .lock()
            .map_err(|_| VeinError::internal("runtime scan statistics mutex poisoned"))?;
        observation.validate_context(
            table_key,
            expected_rows,
            expected_partitions,
            table_scan_candidate,
        )?;
        if observation.collector.is_none() {
            observation.collector = Some(StatisticsCollector::new(
                batch.schema(),
                StatisticsConfig::default(),
            )?);
        }
        observation
            .collector
            .as_mut()
            .ok_or_else(|| VeinError::internal("runtime statistics collector was not initialized"))?
            .update(batch)
    }

    /// Mark one scan partition complete and publish a synopsis after all
    /// partitions finish. A filtered, partial, or row-count-mismatched scan is
    /// retained as an observation but never promoted to full-table statistics.
    pub fn complete_partition(
        &self,
        scan_id: u128,
        table_key: &str,
        expected_rows: Option<u64>,
        expected_partitions: usize,
        table_scan_candidate: bool,
        partition_index: usize,
    ) -> Result<Option<RuntimeStatisticsObservation>> {
        let entry = {
            let state = self
                .state
                .lock()
                .map_err(|_| VeinError::internal("runtime statistics store mutex poisoned"))?;
            state.scans.get(&scan_id).cloned()
        };
        let Some(entry) = entry else {
            return Ok(None);
        };

        let mut observation = entry
            .lock()
            .map_err(|_| VeinError::internal("runtime scan statistics mutex poisoned"))?;
        observation.validate_context(
            table_key,
            expected_rows,
            expected_partitions,
            table_scan_candidate,
        )?;
        if observation.finalized {
            return Ok(None);
        }
        if partition_index >= expected_partitions {
            return Err(VeinError::internal(
                "runtime statistics partition index is outside the scan",
            ));
        }
        observation.completed_partitions.insert(partition_index);
        if observation.completed_partitions.len() != expected_partitions {
            return Ok(None);
        }
        observation.finalized = true;

        let statistics = Arc::new(
            observation
                .collector
                .take()
                .map(StatisticsCollector::finish)
                .unwrap_or_default(),
        );
        let complete_table_snapshot = observation.table_scan_candidate
            && observation.expected_rows.is_some()
            && statistics.num_rows == observation.expected_rows;
        let result = RuntimeStatisticsObservation {
            table_key: observation.table_key.clone(),
            statistics: Arc::clone(&statistics),
            complete_table_snapshot,
            expected_rows: observation.expected_rows,
        };
        let mut state = self
            .state
            .lock()
            .map_err(|_| VeinError::internal("runtime statistics store mutex poisoned"))?;
        state.scans.remove(&scan_id);
        if complete_table_snapshot {
            state
                .complete_tables
                .insert(observation.table_key.clone(), statistics);
        }
        Ok(Some(result))
    }

    /// Read a zero-copy handle to a complete runtime table synopsis, if present.
    pub fn complete_table_statistics(&self, table_key: &str) -> Option<Arc<TableStatistics>> {
        // Catalog planning happens after a prior query has returned. A barrier
        // here makes completed observations available to that next plan without
        // making the producing query wait for synopsis computation.
        self.flush_pending();
        self.state
            .lock()
            .ok()?
            .complete_tables
            .get(table_key)
            .cloned()
    }
}

fn process_statistics_batch(store: &RuntimeStatisticsStore, work: StatisticsWork) {
    let StatisticsWork::Batch { scan_id, table_key, expected_rows, expected_partitions, table_scan_candidate, batch } = work;
    if let Err(error) = store.observe_batch(scan_id, &table_key, expected_rows, expected_partitions, table_scan_candidate, &batch) {
        tracing::warn!(target: "vein.audit", event = "runtime_statistics_batch_failed", scan_id, error = %error, "runtime statistics batch was discarded");
    }
}

fn drain_statistics_batches(store: &RuntimeStatisticsStore, receiver: &mpsc::Receiver<StatisticsWork>) {
    while let Ok(work) = receiver.try_recv() {
        process_statistics_batch(store, work);
    }
}

fn handle_statistics_control(store: &RuntimeStatisticsStore, control: StatisticsControl) -> Result<()> {
    match control {
        StatisticsControl::Complete { scan_id, table_key, expected_rows, expected_partitions, table_scan_candidate, partition_index, query_context } => {
            if let Some(observation) = store.complete_partition(scan_id, &table_key, expected_rows, expected_partitions, table_scan_candidate, partition_index)? {
                if let Some(query_context) = query_context {
                    query_context.record_scan(&observation);
                }
                let stats = observation.statistics.as_ref();
                let mut metric_columns = [0usize; RUNTIME_STATISTIC_METRICS.len()];
                for column in stats.column_statistics.values() {
                    let available = runtime_metric_availability(column);
                    for (count, present) in metric_columns.iter_mut().zip(available) {
                        *count += usize::from(present);
                    }
                }
                let missing = RUNTIME_STATISTIC_METRICS
                    .iter()
                    .zip(metric_columns)
                    .filter_map(|(name, count)| (count == 0).then_some(*name))
                    .collect::<Vec<_>>();
                let metric_column_counts = RUNTIME_STATISTIC_METRICS
                    .iter()
                    .copied()
                    .zip(metric_columns)
                    .collect::<Vec<_>>();
                tracing::info!(
                    target: "vein.statistics",
                    event = "runtime_scan_statistics_completed",
                    table_key = %observation.table_key,
                    observed_rows = ?stats.num_rows,
                    expected_rows = ?observation.expected_rows,
                    column_count = stats.column_statistics.len(),
                    metric_column_counts = ?metric_column_counts,
                    missing_metrics = ?missing,
                    complete_table_snapshot = observation.complete_table_snapshot,
                    computed_statistics = %TableStatisticsLog::new(&stats),
                    "runtime scan statistics completed asynchronously"
                );
            }
        }
        StatisticsControl::Flush(ack) => { let _ = ack.send(()); }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};

    use super::*;

    fn batch(values: &[i64]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("key", DataType::Int64, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(values.to_vec()))])
            .expect("valid test batch")
    }

    #[test]
    fn complete_unfiltered_scan_publishes_merged_partition_statistics() {
        let store = RuntimeStatisticsStore::default();
        store
            .observe_batch(7, "table@revision", Some(4), 2, true, &batch(&[1, 2]))
            .expect("observe first partition");
        store
            .observe_batch(7, "table@revision", Some(4), 2, true, &batch(&[3, 4]))
            .expect("observe second partition");

        assert!(store
            .complete_partition(7, "table@revision", Some(4), 2, true, 0)
            .expect("complete first partition")
            .is_none());
        let completed = store
            .complete_partition(7, "table@revision", Some(4), 2, true, 1)
            .expect("complete second partition")
            .expect("final partition publishes summary");

        assert!(completed.complete_table_snapshot);
        assert_eq!(completed.statistics.num_rows, Some(4));
        assert_eq!(
            store
                .complete_table_statistics("table@revision")
                .expect("complete snapshot")
                .num_rows,
            Some(4)
        );
    }

    #[test]
    fn filtered_or_incomplete_scan_never_replaces_table_statistics() {
        let store = RuntimeStatisticsStore::default();
        store
            .observe_batch(8, "table@revision", Some(4), 1, false, &batch(&[1]))
            .expect("observe filtered output");
        let filtered = store
            .complete_partition(8, "table@revision", Some(4), 1, false, 0)
            .expect("finish filtered scan")
            .expect("scan summary");
        assert!(!filtered.complete_table_snapshot);
        assert!(store.complete_table_statistics("table@revision").is_none());

        store
            .observe_batch(9, "table@revision", Some(4), 1, true, &batch(&[1, 2]))
            .expect("observe truncated output");
        let truncated = store
            .complete_partition(9, "table@revision", Some(4), 1, true, 0)
            .expect("finish row-count mismatch")
            .expect("scan summary");
        assert!(!truncated.complete_table_snapshot);
        assert!(store.complete_table_statistics("table@revision").is_none());
    }

    #[test]
    fn deferred_scan_statistics_are_collected_without_blocking_batch_delivery() {
        let store = RuntimeStatisticsStore::default();
        let query_context = Arc::new(QueryStatisticsContext::new([]));
        let input = batch(&[1, 2, 3, 4]);
        assert_eq!(
            store.enqueue_batch(17, "table@revision", Some(4), 1, true, &input),
            RuntimeStatisticsEnqueue::Queued,
        );
        assert!(store.enqueue_partition_completion(
            17,
            "table@revision",
            Some(4),
            1,
            true,
            0,
            Some(Arc::clone(&query_context)),
        ));

        // Reading the synopsis is a planning-time barrier. The producing scan
        // itself never waits for the background collector.
        let statistics = store
            .complete_table_statistics("table@revision")
            .expect("background worker publishes the complete observation");
        assert_eq!(statistics.num_rows, Some(4));
        assert!(statistics.column_statistics["key"].distinct_count.is_some());
        let observations = query_context.scan_statistics("table@revision");
        let [observation] = observations.as_slice() else {
            panic!("completed scan feedback should be published to the query context");
        };
        assert!(observation.complete_table_snapshot);
        assert_eq!(observation.statistics.num_rows, Some(4));
    }

    #[test]
    fn oversized_runtime_statistics_batches_are_skipped_without_promotion() {
        let store = RuntimeStatisticsStore::with_config(RuntimeStatisticsConfig {
            queue_capacity: 1,
            max_batch_bytes: 1,
        })
            .expect("valid worker config");
        let input = batch(&[1, 2, 3, 4]);
        assert_eq!(
            store.enqueue_batch(18, "table@revision", Some(4), 1, true, &input),
            RuntimeStatisticsEnqueue::BatchTooLarge,
        );
        assert!(store.enqueue_partition_completion(18, "table@revision", Some(4), 1, true, 0, None));
        assert!(store.complete_table_statistics("table@revision").is_none());
    }

    #[test]
    fn runtime_statistics_key_changes_with_source_or_schema_revision() {
        let snapshot = CatalogSnapshotToken::new("table", "catalog-v1", "source-v1", 1);
        let same_snapshot = CatalogSnapshotToken::new("table", "catalog-v1", "source-v1", 1);
        let changed_source = CatalogSnapshotToken::new("table", "catalog-v1", "source-v2", 1);
        let changed_schema = CatalogSnapshotToken::new("table", "catalog-v1", "source-v1", 2);

        assert_eq!(table_statistics_key(&snapshot), table_statistics_key(&same_snapshot));
        assert_ne!(table_statistics_key(&snapshot), table_statistics_key(&changed_source));
        assert_ne!(table_statistics_key(&snapshot), table_statistics_key(&changed_schema));
    }
}
