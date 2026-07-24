use std::sync::Arc;

use iceberg::scan::FileScanTask;
use moka::future::Cache;



/// Cache key: (table namespace + name, snapshot_id, predicate_fingerprint)
/// predicate_fingerprint = 0 means "no predicate" (full scan).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ScanPlanKey {
    pub namespace: Vec<String>,
    pub table_name: String,
    pub snapshot_id: i64,
    /// A stable hash of the serialized predicate expression.
    /// We use 0 when there is no predicate.
    pub predicate_fingerprint: u64,
    /// Sorted list of projected field IDs. Part of the key because
    /// different projections may yield different task lists.
    pub projected_fields: Vec<i32>,
}

/// A computed, pre-pruned list of scan tasks for one (table, snapshot, predicate).
pub type ScanPlan = Arc<Vec<FileScanTask>>;

/// Scan plan cache — eliminates redundant manifest IO for concurrent queries.
///
/// If 100 Executors all query the same table with the same predicate and snapshot,
/// only the FIRST query pays the manifest IO cost. All others get the pre-computed
/// Vec<FileScanTask> from this cache.
#[derive(Clone, Debug)]
pub struct ScanPlanCache {
    inner: Cache<ScanPlanKey, ScanPlan>,
}

impl ScanPlanCache {
    pub fn new() -> Self {
        // Cache up to 256 active scan plans.
        // Each plan is typically a Vec<FileScanTask> with a few hundred entries.
        let inner = Cache::builder().max_capacity(256).build();
        Self { inner }
    }

    pub async fn get(&self, key: &ScanPlanKey) -> Option<ScanPlan> {
        self.inner.get(key).await
    }

    pub async fn insert(&self, key: ScanPlanKey, plan: ScanPlan) {
        self.inner.insert(key, plan).await;
    }

    /// Invalidate all scan plans for a given table+snapshot combination.
    /// Called when VCS receives a new snapshot commit.
    pub async fn invalidate_table(&self, namespace: &[String], table_name: &str) {
        // moka doesn't support predicate-based eviction directly;
        // we invalidate_all() as a safe conservative strategy.
        // In practice, VCS handles one table at a time per invalidation.
        self.inner.invalidate_all();
        tracing::info!(
            namespace = ?namespace,
            table = table_name,
            "scan plan cache cleared after snapshot change"
        );
    }
}

impl Default for ScanPlanCache {
    fn default() -> Self {
        Self::new()
    }
}
