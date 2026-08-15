use std::hash::Hash;
use std::sync::Arc;

use futures::TryStreamExt;
use iceberg::expr::Predicate;
use iceberg::scan::FileScanTask;
use iceberg::table::Table;

use crate::plan_cache::{ScanPlan, ScanPlanCache, ScanPlanKey};
use error::{Result, VeinError};

/// Compute a stable fingerprint for a predicate expression.
/// We use std::hash on the Debug representation — not cryptographic,
/// but stable within a single process run, which is all we need.
fn predicate_fingerprint(pred: &Option<Predicate>) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    format!("{:?}", pred).hash(&mut hasher);
    std::hash::Hasher::finish(&hasher)
}

/// Run a table scan plan centrally in VCS, returning a pre-computed, pre-pruned
/// list of FileScanTasks ready to be dispatched to Executors.
///
/// Results are cached by (table_ident, snapshot_id, predicate, projection).
/// Subsequent calls with the same arguments return instantly from the cache.
pub async fn plan_scan(
    table: &Table,
    columns: Option<Vec<String>>,
    predicate: Option<Predicate>,
    plan_cache: &ScanPlanCache,
) -> Result<ScanPlan> {
    let snapshot_id = table
        .metadata()
        .current_snapshot()
        .map(|s| s.snapshot_id())
        .unwrap_or(0);

    // Build the cache key
    let fingerprint = predicate_fingerprint(&predicate);
    let schema = table.metadata().current_schema();
    let projected_fields: Vec<i32> = match &columns {
        Some(cols) => cols
            .iter()
            .filter_map(|name| schema.field_id_by_name(name))
            .collect(),
        None => schema.as_struct().fields().iter().map(|f| f.id).collect(),
    };

    let key = ScanPlanKey {
        namespace: table.identifier().namespace().as_ref().to_vec(),
        table_name: table.identifier().name().to_string(),
        snapshot_id,
        predicate_fingerprint: fingerprint,
        projected_fields: projected_fields.clone(),
    };

    // Cache hit — return immediately, zero IO
    if let Some(cached_plan) = plan_cache.get(&key).await {
        tracing::debug!(
            table = ?table.identifier(),
            snapshot_id,
            "scan plan cache hit"
        );
        return Ok(cached_plan);
    }

    // Cache miss — build the scan and run plan_files()
    tracing::info!(
        table = ?table.identifier(),
        snapshot_id,
        ?predicate,
        "building scan plan (manifest IO)"
    );

    let mut scan_builder = table.scan();

    // Apply column projection if specified
    if let Some(ref cols) = columns {
        scan_builder = scan_builder.select(cols.iter().map(|s| s.as_str()));
    }

    // Apply predicate pushdown if provided
    if let Some(pred) = predicate {
        scan_builder = scan_builder.with_filter(pred);
    }

    let scan = scan_builder.build().map_err(VeinError::Iceberg)?;

    // plan_files() runs the 3-phase manifest scan:
    //   1. Reads manifest list (1 IO)
    //   2. Reads manifest files in parallel (N IO, concurrency = num_cpus)
    //   3. Evaluates partition filter + column statistics in RAM (0 IO)
    // Returns a stream of FileScanTask for surviving data files only.
    let task_stream = scan.plan_files().await.map_err(VeinError::Iceberg)?;

    let tasks: Vec<FileScanTask> = task_stream
        .try_collect()
        .await
        .map_err(VeinError::Iceberg)?;

    tracing::info!(
        table = ?table.identifier(),
        task_count = tasks.len(),
        "scan plan computed, inserting into cache"
    );

    let plan = Arc::new(tasks);
    plan_cache.insert(key, Arc::clone(&plan)).await;

    Ok(plan)
}
