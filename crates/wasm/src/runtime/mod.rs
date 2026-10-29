//! Browser-only composition of Vein's existing engine and storage providers.

mod storage;

use std::sync::Arc;

use common::statistics::RuntimeStatisticsStore;
use engine::local::{
    LocalEngineBuilder, LocalEngineComponents, LocalQueryEngine, StatisticsCheckpointPolicy,
};
use exec::operators::source::scan::ScanProviderRegistry;
use functions::FunctionRegistry;
use optimizer::OptimizerPipeline;
use parquet_connector::{catalog::ParquetCatalog, scan::ParquetScanProvider};
use physical::DefaultPhysicalPlanner;
use ::storage::{ReadStorageFactory, StorageLocation, opendal::OpenDalReadStorage};
use error::{ErrorCategory, VeinResultExt};
use crate::{browser::files::BrowserFiles, file_storage::{FileIndex, FileStorage}};

use self::storage::{BrowserSpillFactory, ReadOnlyWriteStorage};

pub(crate) fn build_engine(
    registry: Arc<BrowserFiles>,
    files: FileIndex,
) -> common::Result<LocalQueryEngine> {
    let namespace = registry.namespace();
    let location = StorageLocation::uri(format!("browser-file://{namespace}"));
    let operator = opendal::Operator::new(FileStorage::new(namespace, files, registry))
        .map_external_err(ErrorCategory::Storage, "open browser file storage")?;
    let storage = Arc::new(OpenDalReadStorage::new(location.clone(), operator));
    let runtime_statistics = RuntimeStatisticsStore::default();
    let parquet_catalog = Arc::new(
        ParquetCatalog::new(location, storage.as_ref())?
            .with_runtime_statistics_store(runtime_statistics.clone()),
    );
    let mut scan_providers = ScanProviderRegistry::empty();
    scan_providers.register(ParquetScanProvider::new())?;

    let catalog: Arc<dyn plan::CatalogProvider> = parquet_catalog.clone();
    let statistics_provider: Arc<dyn common::statistics::analysis::StatisticsAnalysisProvider> =
        parquet_catalog;
    let read_storage: Arc<dyn ReadStorageFactory> = storage;
    let components = LocalEngineComponents {
        catalog,
        functions: Arc::new(FunctionRegistry::with_builtins()),
        scan_providers,
        read_storage_factory: read_storage,
        write_storage: Arc::new(ReadOnlyWriteStorage),
        statistics_provider,
        runtime_statistics,
        optimizer: Arc::new(OptimizerPipeline::production()),
        planner: Arc::new(DefaultPhysicalPlanner::new()),
        adaptive_policy: Arc::new(StatisticsCheckpointPolicy),
    };
    Ok(LocalEngineBuilder::new(components)
        .with_spill_storage_factory(Arc::new(BrowserSpillFactory))
        .build())
}
