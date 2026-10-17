//! Query-scoped statistics shared by planning and execution.
//!
//! Source statistics are immutable handles pinned to the catalog snapshot used
//! to bind the query. Runtime feedback is accumulated in this context as the
//! query runs. The context stores observations and references only; synopsis
//! algorithms remain in [`super::StatisticsCollector`].

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use catalog::TableStatistics;

use super::RuntimeStatisticsObservation;

/// Statistics for one table input, keyed by its pinned source revision.
#[derive(Debug, Clone)]
pub struct QuerySourceStatistics {
    /// Stable key for the catalog snapshot that produced these statistics.
    pub source_key: String,
    /// Logical table name, used for diagnostics.
    pub table_name: String,
    /// Shared table statistics; cloning this value clones only an `Arc`.
    pub statistics: Option<Arc<TableStatistics>>,
}

/// Identifies one physical operator instance in one task of a query.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct OperatorStatisticsIdentity {
    /// Task-local physical operator id.
    pub node_id: u32,
    /// Task identity, preventing counters from different parallel tasks merging
    /// into one operator observation accidentally.
    pub task_id: u128,
}

/// Atomic counters updated by an execution trace without locking per batch.
#[derive(Debug)]
pub struct OperatorStatisticsHandle {
    operator: &'static str,
    output_rows: AtomicU64,
    output_batches: AtomicU64,
    source_bytes_read: AtomicU64,
    elapsed_ns: AtomicU64,
    metrics: Mutex<HashMap<&'static str, Arc<AtomicU64>>>,
}

impl OperatorStatisticsHandle {
    fn new(operator: &'static str) -> Self {
        Self {
            operator,
            output_rows: AtomicU64::new(0),
            output_batches: AtomicU64::new(0),
            source_bytes_read: AtomicU64::new(0),
            elapsed_ns: AtomicU64::new(0),
            metrics: Mutex::new(HashMap::new()),
        }
    }

    /// Records one output batch using lock-free saturating atomic counters.
    pub fn record_output(&self, rows: u64) {
        saturating_add(&self.output_rows, rows);
        saturating_add(&self.output_batches, 1);
    }

    /// Records bytes read from the operator's source.
    pub fn record_source_bytes(&self, bytes: u64) {
        saturating_add(&self.source_bytes_read, bytes);
    }

    /// Records elapsed wall-clock time for this operator instance.
    pub fn record_elapsed_ns(&self, elapsed_ns: u64) {
        saturating_add(&self.elapsed_ns, elapsed_ns);
    }

    /// Registers or returns an operator-local counter. Registration occurs when
    /// an operator is constructed; updates through the returned handle are
    /// atomic and do not lock the query context.
    pub fn register_counter(&self, name: &'static str) -> OperatorCounter {
        let mut metrics = self.metrics.lock().unwrap_or_else(|poison| poison.into_inner());
        OperatorCounter(Arc::clone(
            metrics.entry(name).or_insert_with(|| Arc::new(AtomicU64::new(0))),
        ))
    }

    /// Rows emitted by this operator across all observed batches.
    pub fn output_rows(&self) -> u64 {
        self.output_rows.load(Ordering::Relaxed)
    }

    /// Number of batches emitted by this operator.
    pub fn output_batches(&self) -> u64 {
        self.output_batches.load(Ordering::Relaxed)
    }

    /// Source bytes read by this operator.
    pub fn source_bytes_read(&self) -> u64 {
        self.source_bytes_read.load(Ordering::Relaxed)
    }

    /// Accumulated inclusive elapsed time in nanoseconds.
    pub fn elapsed_ns(&self) -> u64 {
        self.elapsed_ns.load(Ordering::Relaxed)
    }

    /// Takes a point-in-time snapshot for diagnostics and adaptive consumers.
    pub fn snapshot(&self, identity: OperatorStatisticsIdentity) -> OperatorStatisticsSnapshot {
        let metrics = self.metrics.lock().unwrap_or_else(|poison| poison.into_inner());
        OperatorStatisticsSnapshot {
            identity,
            operator: self.operator,
            output_rows: self.output_rows.load(Ordering::Relaxed),
            output_batches: self.output_batches.load(Ordering::Relaxed),
            source_bytes_read: self.source_bytes_read.load(Ordering::Relaxed),
            elapsed_ns: self.elapsed_ns.load(Ordering::Relaxed),
            metrics: metrics
                .iter()
                .map(|(name, value)| (*name, value.load(Ordering::Relaxed)))
                .collect(),
        }
    }
}

/// Lock-free update handle for a named counter owned by an operator statistic.
#[derive(Debug, Clone)]
pub struct OperatorCounter(Arc<AtomicU64>);

impl OperatorCounter {
    /// Adds to the counter with saturating arithmetic.
    pub fn add(&self, amount: u64) {
        saturating_add(self.0.as_ref(), amount);
    }

    /// Reads the current counter value.
    pub fn value(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }
}

fn saturating_add(counter: &AtomicU64, amount: u64) {
    let mut current = counter.load(Ordering::Relaxed);
    loop {
        let updated = current.saturating_add(amount);
        match counter.compare_exchange_weak(current, updated, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => return,
            Err(actual) => current = actual,
        }
    }
}

/// Immutable point-in-time view of one physical operator's runtime counters.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperatorStatisticsSnapshot {
    /// Operator instance identity.
    pub identity: OperatorStatisticsIdentity,
    /// Generic physical operator name.
    pub operator: &'static str,
    /// Rows emitted by this operator.
    pub output_rows: u64,
    /// Batches emitted by this operator.
    pub output_batches: u64,
    /// Bytes read from storage when this is a source operator.
    pub source_bytes_read: u64,
    /// Accumulated elapsed time attributed to this operator.
    pub elapsed_ns: u64,
    /// Additional operator-specific counters registered by name.
    pub metrics: HashMap<&'static str, u64>,
}

/// Immutable, best-effort point-in-time view of every statistics source owned by a query.
///
/// Table synopses and scan observations remain behind `Arc`; taking a snapshot
/// clones metadata keys and `Arc` handles, never column buffers or synopsis
/// payloads. Operator atomics are sampled while the snapshot is built, so a
/// concurrently running operator can advance between individual counter reads.
/// `complete_table_snapshot` on each scan observation remains authoritative for
/// whether it can be used as source-wide statistics.
#[derive(Debug, Clone)]
pub struct QueryStatisticsSnapshot {
    /// Revision-pinned catalog or source metadata used by this query.
    pub sources: Arc<[QuerySourceStatistics]>,
    /// Runtime scan observations grouped by their revision-pinned source key.
    pub scans: HashMap<String, Vec<Arc<RuntimeStatisticsObservation>>>,
    /// Operator feedback observed when this snapshot was taken.
    pub operators: Vec<OperatorStatisticsSnapshot>,
}

impl QueryStatisticsSnapshot {
    /// Looks up source statistics by revision-pinned key.
    pub fn source_statistics(&self, source_key: &str) -> Option<&TableStatistics> {
        self.sources
            .iter()
            .find(|source| source.source_key == source_key)
            .and_then(|source| source.statistics.as_deref())
    }

    /// Returns scan observations for the matching source revision.
    pub fn scan_statistics(
        &self,
        source_key: &str,
    ) -> &[Arc<RuntimeStatisticsObservation>] {
        self.scans.get(source_key).map_or(&[], Vec::as_slice)
    }

    /// Returns a runtime row count only when completed scan observations prove
    /// complete coverage of the pinned source revision.
    ///
    /// Partial scans are deliberately excluded: their row counts describe the
    /// rows emitted by that scan's predicates and selected work units, not the
    /// source table. If multiple complete scans are present, they must agree on
    /// the row count or the result is considered ambiguous.
    pub fn complete_scan_row_count(&self, source_key: &str) -> Option<u64> {
        let mut complete = self
            .scan_statistics(source_key)
            .iter()
            .filter(|observation| observation.complete_table_snapshot)
            .map(|observation| observation.statistics.num_rows);
        let rows = complete.next()??;
        complete.all(|observed| observed == Some(rows)).then_some(rows)
    }

    /// Returns a merged synopsis only when completed observations prove full
    /// source coverage and agree on the source row count. Columns observed by
    /// multiple scans are retained only when their synopses agree exactly.
    ///
    /// This is deliberately separate from partial scan feedback: a filtered
    /// scan may describe a useful intermediate relation, but without matching
    /// predicate lineage it cannot safely replace table-level statistics.
    pub fn complete_scan_statistics(&self, source_key: &str) -> Option<Arc<TableStatistics>> {
        let mut complete = self
            .scan_statistics(source_key)
            .iter()
            .filter(|observation| observation.complete_table_snapshot);
        let first = complete.next()?;
        let rows = first.statistics.num_rows?;
        let observations = std::iter::once(first)
            .chain(complete)
            .collect::<Vec<_>>();
        if observations
            .iter()
            .any(|observation| observation.statistics.num_rows != Some(rows))
        {
            return None;
        }

        let mut column_names = Vec::new();
        for observation in &observations {
            for name in observation.statistics.column_statistics.keys() {
                if !column_names.contains(name) {
                    column_names.push(name.clone());
                }
            }
        }
        let column_statistics = column_names
            .into_iter()
            .filter_map(|name| {
                let mut values = observations
                    .iter()
                    .filter_map(|observation| observation.statistics.column_statistics.get(&name));
                let first = values.next()?;
                values
                    .all(|statistics| statistics == first)
                    .then(|| (name.clone(), first.clone()))
            })
            .collect();
        let column_groups = observations
            .first()
            .map(|observation| observation.statistics.column_groups.clone())
            .filter(|groups| {
                observations
                    .iter()
                    .all(|observation| observation.statistics.column_groups == *groups)
            })
            .unwrap_or_default();

        Some(Arc::new(TableStatistics {
            num_rows: Some(rows),
            column_statistics,
            column_groups,
            ..TableStatistics::default()
        }))
    }
}

#[derive(Default)]
struct RuntimeState {
    operators: HashMap<OperatorStatisticsIdentity, Arc<OperatorStatisticsHandle>>,
    scans: HashMap<String, Vec<Arc<RuntimeStatisticsObservation>>>,
}

/// The shared statistics context for one prepared query.
///
/// Clones share the same immutable metadata handles and runtime state. The
/// metadata lookup path is read-only; batch feedback uses per-operator atomics,
/// while bounded scan synopsis results are published by the existing async
/// statistics collector.
#[derive(Clone)]
pub struct QueryStatisticsContext {
    sources: Arc<[QuerySourceStatistics]>,
    runtime: Arc<Mutex<RuntimeState>>,
}

impl std::fmt::Debug for QueryStatisticsContext {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("QueryStatisticsContext")
            .field("source_count", &self.sources.len())
            .field(
                "runtime_state",
                &self.runtime.lock().map(|state| (state.operators.len(), state.scans.len())),
            )
            .finish()
    }
}

impl QueryStatisticsContext {
    /// Creates a query context from source snapshots bound during planning.
    pub fn new(sources: impl IntoIterator<Item=QuerySourceStatistics>) -> Self {
        Self {
            sources: Arc::from(sources.into_iter().collect::<Vec<_>>()),
            runtime: Arc::new(Mutex::new(RuntimeState::default())),
        }
    }

    /// Looks up source statistics by the revision-pinned key.
    pub fn source_statistics(&self, source_key: &str) -> Option<&TableStatistics> {
        self.sources
            .iter()
            .find(|source| source.source_key == source_key)
            .and_then(|source| source.statistics.as_deref())
    }

    /// Looks up a source by logical table name when the source has no revision
    /// token. A revision-aware lookup should be preferred whenever available.
    pub fn source_statistics_for_table(&self, table_name: &str) -> Option<&TableStatistics> {
        self.sources
            .iter()
            .find(|source| source.table_name == table_name)
            .and_then(|source| source.statistics.as_deref())
    }

    /// Iterates the immutable source snapshots used to prepare this query.
    pub fn sources(&self) -> &[QuerySourceStatistics] {
        &self.sources
    }

    /// Returns a shared counter handle for one operator instance.
    ///
    /// This is called once while constructing an execution trace. Updating the
    /// returned handle on each batch performs no map lookup and takes no mutex.
    pub fn register_operator(
        &self,
        identity: OperatorStatisticsIdentity,
        operator: &'static str,
    ) -> Arc<OperatorStatisticsHandle> {
        let mut runtime = self.runtime.lock().unwrap_or_else(|poison| poison.into_inner());
        Arc::clone(
            runtime
                .operators
                .entry(identity)
                .or_insert_with(|| Arc::new(OperatorStatisticsHandle::new(operator))),
        )
    }

    /// Publishes a completed scan synopsis to this query, whether or not it
    /// covered the whole table. Incomplete observations remain query-local and
    /// are never mistaken for source-wide catalog statistics.
    pub fn record_scan(&self, observation: &RuntimeStatisticsObservation) {
        let mut runtime = self.runtime.lock().unwrap_or_else(|poison| poison.into_inner());
        runtime
            .scans
            .entry(observation.table_key.clone())
            .or_default()
            .push(Arc::new(observation.clone()));
    }

    /// Returns observed scan synopses for this source revision.
    pub fn scan_statistics(&self, source_key: &str) -> Vec<Arc<RuntimeStatisticsObservation>> {
        self.runtime
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .scans
            .get(source_key)
            .cloned()
            .unwrap_or_default()
    }

    /// Takes an immutable snapshot of operator feedback collected so far.
    pub fn operator_statistics(&self) -> Vec<OperatorStatisticsSnapshot> {
        self.runtime
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .operators
            .iter()
            .map(|(identity, metrics)| metrics.snapshot(*identity))
            .collect()
    }

    /// Takes one immutable view of source metadata and runtime feedback.
    ///
    /// Consumers at optimizer and execution decision points should prefer this
    /// API over independently sampling the source, scan, and operator stores.
    /// The snapshot is point-in-time and best-effort: background scan work that
    /// has not published yet is absent, and partial observations retain their
    /// explicit coverage marker.
    pub fn snapshot(&self) -> QueryStatisticsSnapshot {
        let runtime = self.runtime.lock().unwrap_or_else(|poison| poison.into_inner());
        QueryStatisticsSnapshot {
            sources: Arc::clone(&self.sources),
            scans: runtime.scans.clone(),
            operators: runtime
                .operators
                .iter()
                .map(|(identity, metrics)| metrics.snapshot(*identity))
                .collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cloned_contexts_share_runtime_feedback_and_keep_source_snapshots() {
        let statistics = Arc::new(TableStatistics {
            num_rows: Some(12),
            ..TableStatistics::default()
        });
        let context = QueryStatisticsContext::new([QuerySourceStatistics {
            source_key: "source@revision".into(),
            table_name: "source".into(),
            statistics: Some(Arc::clone(&statistics)),
        }]);
        let executor_view = context.clone();

        assert_eq!(executor_view.source_statistics("source@revision").unwrap().num_rows, Some(12));
        let handle = executor_view.register_operator(
            OperatorStatisticsIdentity { task_id: 7, node_id: 2 },
            "filter",
        );
        handle.record_output(3);
        handle.record_output(4);
        handle.record_source_bytes(128);

        let snapshots = context.operator_statistics();
        let [snapshot] = snapshots.as_slice() else {
            panic!("one shared operator observation is expected");
        };
        assert_eq!(snapshot.output_rows, 7);
        assert_eq!(snapshot.output_batches, 2);
        assert_eq!(snapshot.source_bytes_read, 128);

        handle.register_counter("runtime_filter_rows_seen").add(11);
        handle.register_counter("runtime_filter_rows_kept").add(5);
        let snapshot = context.operator_statistics();
        assert_eq!(snapshot[0].metrics["runtime_filter_rows_seen"], 11);
        assert_eq!(snapshot[0].metrics["runtime_filter_rows_kept"], 5);
        let direct_snapshot = handle.snapshot(OperatorStatisticsIdentity {
            task_id: 7,
            node_id: 2,
        });
        assert_eq!(direct_snapshot.metrics["runtime_filter_rows_seen"], 11);
        assert_eq!(direct_snapshot.metrics["runtime_filter_rows_kept"], 5);
    }

    #[test]
    fn contexts_keep_runtime_feedback_isolated_between_queries() {
        let first = QueryStatisticsContext::new([]);
        let second = QueryStatisticsContext::new([]);
        first
            .register_operator(OperatorStatisticsIdentity { task_id: 1, node_id: 0 }, "scan")
            .record_output(5);

        assert_eq!(first.operator_statistics()[0].output_rows, 5);
        assert!(second.operator_statistics().is_empty());
    }

    #[test]
    fn scan_feedback_keeps_coverage_provenance_in_the_query_context() {
        let context = QueryStatisticsContext::new([]);
        let observation = RuntimeStatisticsObservation {
            table_key: "orders@revision".into(),
            statistics: Arc::new(TableStatistics {
                num_rows: Some(20),
                ..TableStatistics::default()
            }),
            complete_table_snapshot: false,
            expected_rows: Some(100),
        };

        context.record_scan(&observation);

        let observations = context.scan_statistics("orders@revision");
        let [observed] = observations.as_slice() else {
            panic!("one scan observation should be retained");
        };
        assert!(!observed.complete_table_snapshot);
        assert_eq!(observed.expected_rows, Some(100));
        assert_eq!(observed.statistics.num_rows, Some(20));
    }

    #[test]
    fn query_snapshot_unifies_source_scan_and_operator_statistics_without_copying_synopses() {
        let source_statistics = Arc::new(TableStatistics {
            num_rows: Some(100),
            ..TableStatistics::default()
        });
        let context = QueryStatisticsContext::new([QuerySourceStatistics {
            source_key: "orders@revision".into(),
            table_name: "orders".into(),
            statistics: Some(Arc::clone(&source_statistics)),
        }]);
        let runtime_statistics = Arc::new(TableStatistics {
            num_rows: Some(10),
            ..TableStatistics::default()
        });
        context.record_scan(&RuntimeStatisticsObservation {
            table_key: "orders@revision".into(),
            statistics: Arc::clone(&runtime_statistics),
            complete_table_snapshot: false,
            expected_rows: Some(100),
        });
        let operator = context.register_operator(
            OperatorStatisticsIdentity { task_id: 7, node_id: 3 },
            "filter",
        );
        operator.record_output(10);

        let snapshot = context.snapshot();
        assert!(Arc::ptr_eq(
            snapshot.sources[0].statistics.as_ref().unwrap(),
            &source_statistics,
        ));
        let [scan] = snapshot.scan_statistics("orders@revision") else {
            panic!("one scan synopsis should be visible in the query snapshot");
        };
        assert!(Arc::ptr_eq(&scan.statistics, &runtime_statistics));
        assert!(!scan.complete_table_snapshot);
        assert_eq!(snapshot.operators[0].output_rows, 10);
    }

    #[test]
    fn complete_runtime_scan_row_count_is_revision_scoped_and_ignores_partial_scans() {
        let context = QueryStatisticsContext::new([]);
        context.record_scan(&RuntimeStatisticsObservation {
            table_key: "orders@revision".into(),
            statistics: Arc::new(TableStatistics {
                num_rows: Some(7),
                ..TableStatistics::default()
            }),
            complete_table_snapshot: false,
            expected_rows: Some(100),
        });
        assert_eq!(context.snapshot().complete_scan_row_count("orders@revision"), None);

        context.record_scan(&RuntimeStatisticsObservation {
            table_key: "orders@revision".into(),
            statistics: Arc::new(TableStatistics {
                num_rows: Some(100),
                ..TableStatistics::default()
            }),
            complete_table_snapshot: true,
            expected_rows: Some(100),
        });
        assert_eq!(context.snapshot().complete_scan_row_count("orders@revision"), Some(100));
        assert_eq!(context.snapshot().complete_scan_row_count("orders@other-revision"), None);
        assert!(context
            .snapshot()
            .complete_scan_statistics("orders@revision")
            .is_some());
        assert!(context
            .snapshot()
            .complete_scan_statistics("orders@other-revision")
            .is_none());
    }

    #[test]
    fn complete_scan_synopses_merge_projected_columns_and_drop_conflicts() {
        let context = QueryStatisticsContext::new([]);
        for (key_ndv, column, column_ndv) in [(10, "first", 2), (10, "second", 3), (11, "third", 4)] {
            let statistics = TableStatistics {
                num_rows: Some(100),
                column_statistics: HashMap::from([
                    (
                        "key".to_owned(),
                        catalog::ColumnStatistics {
                            distinct_count: Some(key_ndv),
                            ..catalog::ColumnStatistics::default()
                        },
                    ),
                    (
                        column.to_owned(),
                        catalog::ColumnStatistics {
                            distinct_count: Some(column_ndv),
                            ..catalog::ColumnStatistics::default()
                        },
                    ),
                ]),
                ..TableStatistics::default()
            };
            context.record_scan(&RuntimeStatisticsObservation {
                table_key: "orders@revision".into(),
                statistics: Arc::new(statistics),
                complete_table_snapshot: true,
                expected_rows: Some(100),
            });
        }

        let statistics = context
            .snapshot()
            .complete_scan_statistics("orders@revision")
            .expect("matching complete scans provide a source synopsis");
        assert_eq!(statistics.num_rows, Some(100));
        assert!(!statistics.column_statistics.contains_key("key"));
        assert_eq!(statistics.column_statistics["first"].distinct_count, Some(2));
        assert_eq!(statistics.column_statistics["second"].distinct_count, Some(3));
        assert_eq!(statistics.column_statistics["third"].distinct_count, Some(4));
    }

    #[test]
    fn conflicting_complete_scan_counts_are_not_exposed_as_authoritative() {
        let context = QueryStatisticsContext::new([]);
        for rows in [100, 101] {
            context.record_scan(&RuntimeStatisticsObservation {
                table_key: "orders@revision".into(),
                statistics: Arc::new(TableStatistics {
                    num_rows: Some(rows),
                    ..TableStatistics::default()
                }),
                complete_table_snapshot: true,
                expected_rows: Some(rows),
            });
        }

        assert_eq!(context.snapshot().complete_scan_row_count("orders@revision"), None);
        assert!(context
            .snapshot()
            .complete_scan_statistics("orders@revision")
            .is_none());
    }
}
