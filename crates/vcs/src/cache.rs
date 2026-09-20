use std::sync::Arc;

use iceberg::table::Table;
use iceberg::{Catalog, TableIdent};
use moka::future::Cache;

use error::{Result, VeinError};

/// Maximum number of table entries kept in the hot cache.
/// At ~1KB per TableMetadata, 1000 tables ≈ 1MB of catalog RAM.
const MAX_TABLE_CACHE_ENTRIES: u64 = 1_000;

/// Hot metadata cache: TableIdent → Arc<Table>
///
/// - A cache hit returns the Arc<Table> with zero IO.
/// - A cache miss calls catalog.load_table() and inserts the result.
/// - Cache entries are evicted by count (LRU policy).
/// - Invalidation: call `invalidate(&ident)` when VCS receives a snapshot change.
#[derive(Clone, Debug)]
pub struct TableCache {
    inner: Cache<TableIdent, Arc<Table>>,
}

impl Default for TableCache {
    fn default() -> Self {
        Self::new()
    }
}

impl TableCache {
    /// Creates an empty table cache with the configured maximum capacity.
    pub fn new() -> Self {
        let inner = Cache::builder()
            .max_capacity(MAX_TABLE_CACHE_ENTRIES)
            .build();
        Self { inner }
    }

    /// Load a table — returns from cache if present, otherwise fetches from catalog.
    pub async fn get_or_load(
        &self,
        ident: &TableIdent,
        catalog: &dyn Catalog,
    ) -> Result<Arc<Table>> {
        // Check hot cache first
        if let Some(cached) = self.inner.get(ident).await {
            tracing::debug!(target: "vein.audit", event = "catalog_cache_hit", ?ident, "catalog_cache_hit");
            return Ok(cached);
        }

        // Cache miss — fetch from Iceberg catalog (reads metadata.json from disk/S3)
        tracing::debug!(target: "vein.audit", event = "catalog_cache_miss", ?ident, "catalog_cache_miss");
        let table = catalog
            .load_table(ident)
            .await
            .map_err(VeinError::Iceberg)?;
        let table = Arc::new(table);

        self.inner.insert(ident.clone(), Arc::clone(&table)).await;
        Ok(table)
    }

    /// Invalidate a specific table entry (called when a new snapshot is committed).
    pub async fn invalidate(&self, ident: &TableIdent) {
        self.inner.invalidate(ident).await;
        tracing::info!(target: "vein.audit", event = "catalog_cache_invalidated", ?ident, "catalog_cache_invalidated");
    }

    /// Invalidate all entries (e.g., after VCS restart or full refresh).
    pub async fn invalidate_all(&self) {
        self.inner.invalidate_all();
        tracing::info!(target: "vein.audit", event = "catalog_cache_invalidated_all", "catalog_cache_invalidated_all");
    }
}
