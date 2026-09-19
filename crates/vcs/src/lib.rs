#![deny(missing_docs)]

//! Vein Catalog Service: versioned metadata, scan planning, and the
//! planner-facing catalog seam.

/// The planner-facing catalog seam and its in-memory implementation.
pub mod catalog;

/// Hot table metadata cache.
pub mod cache;

/// Broadcast events sent to Master nodes (snapshot changes, heartbeats).
///
/// `missing_docs` is relaxed on this module only: `rkyv`'s `Archive` derive
/// generates public archived structs/functions that cannot carry source
/// documentation, while the hand-written surface is fully documented.
#[allow(missing_docs)]
pub mod event;

/// Authoritative shuffle exchange metadata.
pub mod exchange;

/// QUIC exchange metadata service.
pub mod rpc;

/// Cache of pre-computed scan plans for warm query replays.
pub mod plan_cache;

/// Scan planning: translates predicates into file scan tasks.
pub mod scan;

/// Storage backend configuration and file IO construction.
pub mod storage;

/// Smart source discovery and schema resolution.
pub mod discovery;

pub use catalog::{InMemoryCatalog, TableCatalog, TableEntry};
pub use discovery::{DiscoveredSource, resolve_source};
pub use exchange::{ExchangeCatalog, InMemoryExchangeCatalog, PersistentExchangeCatalog};
pub use rpc::VcsExchangeServer;

use std::collections::HashMap;
use std::sync::Arc;

use error::{Result, VeinError};
use iceberg::expr::Predicate;
use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent};

use crate::cache::TableCache;
use crate::plan_cache::{ScanPlan, ScanPlanCache};
use crate::scan::plan_scan;
use crate::storage::{StorageConfig, build_file_io};

/// The Vein Catalog Service.
///
/// This is the single source of truth for all catalog and storage concerns.
/// It wraps an Iceberg MemoryCatalog (backed by local FS or S3),
/// adds a hot metadata cache and a scan plan cache, and serves
/// pre-computed FileScanTask lists to Master nodes.
///
/// # Responsibilities
/// - Own the Iceberg catalog (table schema, snapshots, manifest metadata)
/// - Cache TableMetadata in RAM (zero IO on warm lookups)
/// - Compute and cache scan plans centrally (eliminates redundant S3 manifest reads)
/// - Broadcast TableInvalidated events to connected Master nodes (future)
///
/// # Does NOT
/// - Execute queries
/// - Talk to Executor nodes directly
/// - Know anything about QUIC routing or topology
pub struct VcsCatalog {
    catalog: Arc<dyn Catalog>,
    table_cache: TableCache,
    plan_cache: ScanPlanCache,
    exchange_catalog: Arc<dyn ExchangeCatalog>,
    #[allow(dead_code)]
    file_io: iceberg::io::FileIO,
}

fn exchange_manifest_path(warehouse: &str) -> String {
    format!("{}/_vein/exchanges/manifests.json", warehouse.trim_end_matches('/'))
}

impl VcsCatalog {
    /// Create a new VcsCatalog with the given storage configuration.
    ///
    /// # Arguments
    /// - `catalog_name`: A name for this catalog (e.g. "vein-prod")
    /// - `warehouse_location`: The root path where Iceberg table metadata lives
    /// - `storage`: Which storage backend to use (local FS or S3)
    pub async fn new(
        catalog_name: impl Into<String>,
        warehouse_location: impl Into<String>,
        storage: StorageConfig,
    ) -> Result<Self> {
        let file_io = build_file_io(&storage)?;
        let warehouse = warehouse_location.into();
        let exchange_manifest_path = exchange_manifest_path(&warehouse);
        let exchange_catalog = PersistentExchangeCatalog::open(
            file_io.clone(),
            exchange_manifest_path,
        )
        .await?;

        // Derive the StorageFactory from the StorageConfig for the MemoryCatalog.
        // The MemoryCatalog needs its own factory to read/write metadata.json files.
        let factory: Arc<dyn iceberg::io::StorageFactory> = match &storage {
            StorageConfig::LocalFs => Arc::new(iceberg::io::LocalFsStorageFactory),
            StorageConfig::S3 {
                endpoint: _,
                region: _,
                access_key_id: _,
                secret_access_key: _,
                path_style_access: _,
            } => {
                // For the catalog's metadata IO we pass the same OpenDAL factory.
                // The factory reads the StorageConfig props map at runtime.
                Arc::new(iceberg_storage_opendal::OpenDalStorageFactory::S3 {
                    customized_credential_load: None,
                })
            }
        };

        // Build the MemoryCatalog — metadata index lives in process RAM.
        // It uses the FileIO to read/write actual metadata.json files on disk/S3.
        let catalog = MemoryCatalogBuilder::default()
            .with_storage_factory(factory)
            .load(
                catalog_name,
                HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), warehouse.clone())]),
            )
            .await
            .map_err(VeinError::Iceberg)?;

        Ok(Self {
            catalog: Arc::new(catalog),
            table_cache: TableCache::new(),
            plan_cache: ScanPlanCache::new(),
            exchange_catalog: Arc::new(exchange_catalog),
            file_io,
        })
    }

    /// Returns the VCS-owned exchange catalog.
    pub fn exchange_catalog(&self) -> &dyn ExchangeCatalog {
        self.exchange_catalog.as_ref()
    }

    /// Returns a shareable handle for the VCS exchange RPC service.
    pub fn shared_exchange_catalog(&self) -> Arc<dyn ExchangeCatalog> {
        self.exchange_catalog.clone()
    }

    /// Creates or reopens an exchange through VCS.
    pub async fn create_exchange(
        &self,
        key: common::protocol::ExchangeKey,
        partition_count: u16,
        expected_tasks: Vec<common::identifiers::TaskId>,
        schema_fingerprint: impl Into<String>,
    ) -> Result<common::protocol::ExchangeManifest> {
        self.exchange_catalog
            .create_exchange(
                key,
                partition_count,
                expected_tasks,
                schema_fingerprint.into(),
            )
            .await
    }

    /// Publishes one durable exchange fragment through VCS.
    pub async fn publish_exchange_fragment(
        &self,
        key: common::protocol::ExchangeKey,
        fragment: common::protocol::ExchangeFragment,
    ) -> Result<()> {
        self.exchange_catalog.publish_fragment(key, fragment).await
    }

    /// Seals an exchange after all registered producers publish output.
    pub async fn seal_exchange(
        &self,
        key: common::protocol::ExchangeKey,
    ) -> Result<common::protocol::ExchangeManifest> {
        self.exchange_catalog.seal_exchange(key).await
    }

    /// Reads a sealed exchange manifest through VCS.
    pub async fn get_exchange_manifest(
        &self,
        key: common::protocol::ExchangeKey,
    ) -> Result<common::protocol::ExchangeManifest> {
        self.exchange_catalog.get_manifest(key).await
    }

    // -------------------------------------------------------------------------
    // Catalog Management — used by VCS admin / ingestion pipeline
    // -------------------------------------------------------------------------

    /// Register a new namespace in the catalog.
    pub async fn create_namespace(&self, ns: &NamespaceIdent) -> Result<()> {
        self.catalog
            .create_namespace(ns, HashMap::new())
            .await
            .map_err(VeinError::Iceberg)?;
        Ok(())
    }

    /// Register a new table in the catalog.
    pub async fn create_table(&self, ns: &NamespaceIdent, creation: TableCreation) -> Result<()> {
        self.catalog
            .create_table(ns, creation)
            .await
            .map_err(VeinError::Iceberg)?;
        Ok(())
    }

    // -------------------------------------------------------------------------
    // Query Planning — called by Master node for every incoming query
    // -------------------------------------------------------------------------

    /// Load a table from the catalog.
    ///
    /// Returns instantly from the in-process cache after the first load.
    /// Zero IO on warm cache hit.
    pub async fn load_table(&self, ident: &TableIdent) -> Result<iceberg::table::Table> {
        let table = self.table_cache.get_or_load(ident, &*self.catalog).await?;
        // Arc<Table> → clone the inner Table for the caller
        Ok((*table).clone())
    }

    /// Resolve a scan plan for the given table and optional predicate.
    ///
    /// Returns a pre-computed, pre-pruned list of `FileScanTask`s.
    /// The first call for a given (table, snapshot, predicate) pays the manifest IO cost.
    /// All subsequent calls return the cached result instantly.
    ///
    /// # Arguments
    /// - `ident`: Table identifier (namespace + name)
    /// - `columns`: Optional column projection. None = select all.
    /// - `predicate`: Optional predicate for partition pruning and row-group filtering.
    pub async fn resolve_scan(
        &self,
        ident: &TableIdent,
        columns: Option<Vec<String>>,
        predicate: Option<Predicate>,
    ) -> Result<ScanPlan> {
        let table = self.table_cache.get_or_load(ident, &*self.catalog).await?;
        plan_scan(&table, columns, predicate, &self.plan_cache).await
    }

    // -------------------------------------------------------------------------
    // Cache Invalidation — called when new data is ingested into a table
    // -------------------------------------------------------------------------

    /// Invalidate all cached state for a table.
    ///
    /// Call this when a new Iceberg snapshot is committed to the table.
    /// After this call, the next `resolve_scan()` will re-read metadata from storage.
    pub async fn invalidate_table(&self, ident: &TableIdent) {
        self.table_cache.invalidate(ident).await;
        self.plan_cache
            .invalidate_table(ident.namespace().as_ref(), ident.name())
            .await;
    }

    /// Retrieve the FileIO handle (used by Executors to directly read Parquet files).
    ///
    /// VCS does NOT pass this to Executors — instead, Executors construct their
    /// own FileIO from config. This method is for VCS-internal use (index building).
    #[allow(dead_code)]
    pub(crate) fn file_io(&self) -> &iceberg::io::FileIO {
        &self.file_io
    }
}
