use std::sync::Arc;

use catalog::AnalyzedTableStatistics;
use common::Result;
use common::statistics::analysis::{
    StatisticsAnalysisProvider, analyze_table as analyze_statistics_table,
};
use common::statistics::{RuntimeStatisticsStore, StatisticsConfig};
use exec::operators::sink::WriteStorage;
use exec::operators::source::scan::ScanProviderRegistry;
use exec::runtime::TaskExecutor;
use functions::{FunctionCatalog, FunctionRegistry};
use parquet_connector::catalog::{ParquetCatalog, ParquetStatisticsPublisher};
use parquet_connector::scan::ParquetScanProvider;
use parquet_connector::writer::ParquetWriteStorage;
use plan::CatalogProvider;
use storage::opendal::OpenDalReadStorageFactory;
use storage::{ReadStorageFactory, StorageLocation};


mod adaptive;
mod prepare;
mod query;
mod query_context;
mod stream;
mod tasks;
#[cfg(test)]
mod tests;

pub use query::{LocalQuery, LocalResultStream};

/// A local query engine that plans against Parquet metadata and runs tasks in-process.
pub struct LocalQueryEngine {
    binder: binder::QueryBinder,
    optimizer: Arc<optimizer::OptimizerPipeline>,
    planner: Arc<physical::DefaultPhysicalPlanner>,
    adaptive_policy: Arc<dyn adaptive::AdaptiveCheckpointPolicy>,
    executor: Arc<dyn TaskExecutor>,
    write_storage: Arc<dyn WriteStorage>,
    statistics_provider: Arc<dyn StatisticsAnalysisProvider>,
    runtime_statistics: RuntimeStatisticsStore,
}

impl LocalQueryEngine {
    /// Creates an engine whose table names resolve beneath `data_root`.
    pub fn new(data_root: impl Into<std::path::PathBuf>) -> Self {
        let data_root = data_root.into();
        let runtime_statistics = RuntimeStatisticsStore::default();
        let read_storage_factory: Arc<dyn ReadStorageFactory> =
            Arc::new(OpenDalReadStorageFactory);
        let parquet_writer = Arc::new(ParquetWriteStorage::new(data_root.clone()));
        let write_storage: Arc<dyn WriteStorage> = parquet_writer.clone();
        let statistics_publisher: Arc<dyn ParquetStatisticsPublisher> = parquet_writer;
        let catalog = Arc::new(
            ParquetCatalog::new(
                StorageLocation::local(data_root.clone()),
                read_storage_factory.as_ref(),
            )
                .expect("open local catalog storage")
                .with_runtime_statistics_store(runtime_statistics.clone())
                .with_statistics_publisher(statistics_publisher),
        );
        let metadata_store = catalog.metadata_store();
        let statistics_provider: Arc<dyn StatisticsAnalysisProvider> = catalog.clone();
        let catalog: Arc<dyn CatalogProvider> = catalog;
        let functions = Arc::new(FunctionRegistry::with_builtins());
        let function_catalog: Arc<dyn FunctionCatalog> = functions.clone();
        let mut scan_providers = ScanProviderRegistry::empty();
        scan_providers
            .register(ParquetScanProvider::default())
            .expect("local engine registers one provider per format");
        let executor = Arc::new(
            exec::factory::TaskExecutorBuilder::new()
                .with_read_storage_factory(read_storage_factory)
                .with_metadata_store(metadata_store)
                .with_runtime_statistics_store(runtime_statistics.clone())
                .with_scan_provider_registry(scan_providers)
                .with_function_registry(Arc::clone(&functions))
                .with_write_storage(Arc::clone(&write_storage))
                .build(),
        );

        Self {
            binder: binder::QueryBinder::new(catalog, function_catalog),
            optimizer: Arc::new(optimizer::OptimizerPipeline::default()),
            planner: Arc::new(physical::DefaultPhysicalPlanner::new()),
            adaptive_policy: Arc::new(adaptive::StatisticsCheckpointPolicy),
            executor,
            write_storage,
            statistics_provider,
            runtime_statistics,
        }
    }

    /// Analyze a table with default bounded statistics settings and publish the result.
    pub async fn analyze_table(&self, table_name: &str) -> Result<AnalyzedTableStatistics> {
        self.analyze_table_with_config(table_name, StatisticsConfig::default())
            .await
    }

    /// Analyze a table with explicit synopsis, scan, and multi-column settings.
    pub async fn analyze_table_with_config(
        &self,
        table_name: &str,
        config: StatisticsConfig,
    ) -> Result<AnalyzedTableStatistics> {
        analyze_statistics_table(self.statistics_provider.as_ref(), table_name, config).await
    }

    /// Returns the shared runtime statistics populated by completed scans.
    pub fn runtime_statistics(&self) -> &RuntimeStatisticsStore {
        &self.runtime_statistics
    }
}
