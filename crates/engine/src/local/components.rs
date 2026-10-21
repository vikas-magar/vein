use std::sync::Arc;

use catalog::CatalogProvider;
use common::statistics::RuntimeStatisticsStore;
use common::statistics::analysis::StatisticsAnalysisProvider;
use exec::operators::sink::WriteStorage;
use exec::operators::source::scan::ScanProviderRegistry;
use functions::FunctionRegistry;
use optimizer::OptimizerPipeline;
use physical::PhysicalPlanner;
use storage::ReadStorageFactory;

use super::AdaptiveCheckpointPolicy;

/// Required format-neutral services used to construct a local query engine.
///
/// The caller chooses every catalog, format provider, and storage backend. The
/// engine does not install a concrete data-format implementation implicitly.
pub struct LocalEngineComponents {
    /// Catalog used by binding and shared metadata-cache access.
    pub catalog: Arc<dyn CatalogProvider>,
    /// Shared binder and execution function registry.
    pub functions: Arc<FunctionRegistry>,
    /// Explicit providers for all file formats visible to this engine.
    pub scan_providers: ScanProviderRegistry,
    /// Storage factory used by generic file-scan coordination.
    pub read_storage_factory: Arc<dyn ReadStorageFactory>,
    /// Transactional sink backend used by storage writes.
    pub write_storage: Arc<dyn WriteStorage>,
    /// Format adapter used by explicit table analysis requests.
    pub statistics_provider: Arc<dyn StatisticsAnalysisProvider>,
    /// Query and scan feedback store shared with catalog adapters.
    pub runtime_statistics: RuntimeStatisticsStore,
    /// Logical optimizer used for preparation and adaptive continuations.
    pub optimizer: Arc<OptimizerPipeline>,
    /// Physical planner used for initial and adaptive plans.
    pub planner: Arc<dyn PhysicalPlanner>,
    /// Policy that selects safe runtime checkpoints.
    pub adaptive_policy: Arc<dyn AdaptiveCheckpointPolicy>,
}
