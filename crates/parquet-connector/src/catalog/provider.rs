use std::fmt;
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use catalog::MetadataStore;
use common::statistics::RuntimeStatisticsStore;
use error::Result;
use plan::{CatalogProvider, TableMetadata};
use storage::{ReadStorage, ReadStorageFactory, StorageLocation};

use super::discovery::{discover, read_external_statistics};
use super::manifest::artifact_path;
use super::metadata::inspect_files;
use super::publication::ParquetStatisticsPublisher;
use super::snapshot;

/// Directory-backed Parquet catalog adapter using injected storage and shared metadata services.
#[derive(Clone)]
pub struct ParquetCatalog {
    pub(super) location: StorageLocation,
    pub(super) storage: Arc<dyn ReadStorage>,
    pub(super) metadata_store: MetadataStore,
    pub(super) runtime_statistics: RuntimeStatisticsStore,
    pub(super) publisher: Option<Arc<dyn ParquetStatisticsPublisher>>,
}

impl ParquetCatalog {
    /// Opens the catalog's storage namespace through the supplied backend.
    pub fn new(location: StorageLocation, factory: &dyn ReadStorageFactory) -> Result<Self> {
        Ok(Self {
            storage: factory.open(&location)?,
            location,
            metadata_store: MetadataStore::new(),
            runtime_statistics: RuntimeStatisticsStore::default(),
            publisher: None,
        })
    }

    /// Shares runtime synopses collected by the query executor.
    pub fn with_runtime_statistics_store(mut self, store: RuntimeStatisticsStore) -> Self {
        self.runtime_statistics = store;
        self
    }

    /// Injects the writer-side persistence adapter used by ANALYZE.
    pub fn with_statistics_publisher(
        mut self,
        publisher: Arc<dyn ParquetStatisticsPublisher>,
    ) -> Self {
        self.publisher = Some(publisher);
        self
    }

    /// Returns the metadata cache shared with the scan provider.
    pub fn metadata_store(&self) -> MetadataStore {
        self.metadata_store.clone()
    }
}

impl fmt::Debug for ParquetCatalog {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ParquetCatalog")
            .field("location", &self.location)
            .field("metadata_store", &self.metadata_store)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl CatalogProvider for ParquetCatalog {
    async fn get_table(&self, name: &str) -> Result<Arc<TableMetadata>> {
        let table_path = artifact_path(name)?;
        let started = Instant::now();
        let files = discover(self.storage.as_ref(), name, &table_path).await?;
        tracing::info!(
            target: "vein.audit",
            event = "catalog_discovery",
            table = %name,
            source = if files.manifest.is_some() { "manifest" } else { "storage" },
            file_count = files.paths.len(),
            discover_us = started.elapsed().as_micros() as u64,
            "catalog_discovery"
        );
        let mut metadata = inspect_files(
            name,
            &self.location,
            &table_path,
            &files.paths,
            &self.storage,
            &self.metadata_store,
        )
            .await?;
        self.apply_persisted_statistics(name, &mut metadata, files.manifest.as_ref())
            .await?;
        snapshot::pin_source_snapshot(&self.location, &mut metadata)?;
        snapshot::apply_runtime_statistics(self, &mut metadata)?;
        Ok(Arc::new(metadata))
    }

    fn metadata_store(&self) -> MetadataStore {
        self.metadata_store()
    }
}

impl ParquetCatalog {
    async fn apply_persisted_statistics(
        &self,
        name: &str,
        metadata: &mut TableMetadata,
        manifest: Option<&super::manifest::TableManifest>,
    ) -> Result<()> {
        let Some(statistics) = manifest.and_then(|manifest| manifest.statistics.as_ref()) else {
            if manifest.is_some() {
                return Ok(());
            }
            let Some(statistics) = read_external_statistics(self.storage.as_ref(), name).await? else {
                return Ok(());
            };
            return self.apply_analysis(metadata, &statistics, None);
        };
        self.apply_analysis(metadata, &statistics, manifest.map(|manifest| manifest.revision))
    }

    fn apply_analysis(
        &self,
        metadata: &mut TableMetadata,
        statistics: &catalog::AnalyzedTableStatistics,
        managed_revision: Option<u64>,
    ) -> Result<()> {
        let identities = snapshot::source_identities(metadata, &self.location, managed_revision.is_some());
        let revision = managed_revision.unwrap_or_else(|| {
            common::statistics::analysis::revision_from_identities(&identities)
        });
        super::apply_analyzed_statistics(metadata, revision, statistics, &identities);
        Ok(())
    }
}
