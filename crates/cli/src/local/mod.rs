use std::path::PathBuf;
use std::sync::Arc;

use common::Result;
use common::statistics::RuntimeStatisticsStore;
use engine::local::{LocalEngineBuilder, LocalEngineComponents, StatisticsCheckpointPolicy};
use exec::operators::sink::WriteStorage;
use exec::operators::source::scan::ScanProviderRegistry;
use functions::FunctionRegistry;
use parquet_connector::catalog::{ParquetCatalog, ParquetStatisticsPublisher};
use parquet_connector::scan::ParquetScanProvider;
use parquet_connector::writer::ParquetWriteStorage;
use plan::CatalogProvider;
use storage::opendal::OpenDalReadStorageFactory;
use storage::{ReadStorageFactory, StorageLocation};

pub use engine::local::{
    AdaptiveBoundary, AdaptiveCheckpointPolicy, LocalQuery, LocalQueryEngine, LocalResultStream,
};

#[cfg(test)]
mod tests;

/// Creates the configured test engine from the connector composition used by CLI.
#[cfg(test)]
pub(crate) fn test_engine(data_root: impl Into<PathBuf>) -> LocalQueryEngine {
    build_local_engine(data_root).expect("construct local test engine")
}

/// Composes the generic local engine with filesystem and Parquet providers.
pub fn build_local_engine(data_root: impl Into<PathBuf>) -> Result<LocalQueryEngine> {
    let data_root = data_root.into();
    let runtime_statistics = RuntimeStatisticsStore::default();
    let read_storage_factory: Arc<dyn ReadStorageFactory> =
        Arc::new(OpenDalReadStorageFactory);
    let parquet_writer = Arc::new(ParquetWriteStorage::new(data_root.clone()));
    let write_storage: Arc<dyn WriteStorage> = parquet_writer.clone();
    let statistics_publisher: Arc<dyn ParquetStatisticsPublisher> = parquet_writer;
    let parquet_catalog = ParquetCatalog::new(
        StorageLocation::local(data_root),
        read_storage_factory.as_ref(),
    )?
    .with_runtime_statistics_store(runtime_statistics.clone())
    .with_statistics_publisher(statistics_publisher);
    let statistics_provider = Arc::new(parquet_catalog.clone());
    let catalog: Arc<dyn CatalogProvider> = Arc::new(parquet_catalog);
    let functions = Arc::new(FunctionRegistry::with_builtins());
    let mut scan_providers = ScanProviderRegistry::empty();
    scan_providers.register(ParquetScanProvider::default())?;

    let components = LocalEngineComponents {
        catalog,
        functions,
        scan_providers,
        read_storage_factory,
        write_storage,
        statistics_provider,
        runtime_statistics,
        optimizer: Arc::new(optimizer::OptimizerPipeline::default()),
        planner: Arc::new(physical::DefaultPhysicalPlanner::new()),
        adaptive_policy: Arc::new(StatisticsCheckpointPolicy),
    };
    Ok(LocalEngineBuilder::new(components).build())
}
