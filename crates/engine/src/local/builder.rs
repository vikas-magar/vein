use std::sync::Arc;

use binder::QueryBinder;
use exec::factory::TaskExecutorBuilder;
use exec::runtime::TaskExecutor;
use exec::spill::SpillStorageFactory;
use functions::FunctionCatalog;

use super::{LocalEngineComponents, LocalQueryEngine};

/// Builds a local query engine from explicitly selected services.
pub struct LocalEngineBuilder {
    components: LocalEngineComponents,
    spill_storage_factory: Option<Arc<dyn SpillStorageFactory>>,
}

impl LocalEngineBuilder {
    /// Creates a builder that requires explicit catalog, storage, optimizer,
    /// planner, functions, and scan-provider implementations.
    pub fn new(components: LocalEngineComponents) -> Self {
        Self { components, spill_storage_factory: None }
    }

    /// Installs query-scoped spill storage without replacing the engine's other providers.
    pub fn with_spill_storage_factory(mut self, factory: Arc<dyn SpillStorageFactory>) -> Self {
        self.spill_storage_factory = Some(factory);
        self
    }

    /// Constructs orchestration and a task executor using the same metadata
    /// cache exposed by the injected catalog.
    pub fn build(self) -> LocalQueryEngine {
        let Self { components, spill_storage_factory } = self;
        let LocalEngineComponents {
            catalog,
            functions,
            scan_providers,
            read_storage_factory,
            write_storage,
            statistics_provider,
            runtime_statistics,
            optimizer,
            planner,
            adaptive_policy,
        } = components;
        let metadata_store = catalog.metadata_store();
        let function_catalog: Arc<dyn FunctionCatalog> = functions.clone();
        let mut task_executor = TaskExecutorBuilder::new()
            .with_read_storage_factory(read_storage_factory)
            .with_metadata_store(metadata_store)
            .with_runtime_statistics_store(runtime_statistics.clone())
            .with_scan_provider_registry(scan_providers)
            .with_function_registry(functions)
            .with_write_storage(write_storage.clone());
        if let Some(factory) = spill_storage_factory {
            task_executor = task_executor.with_spill_storage_factory(factory);
        }
        let executor: Arc<dyn TaskExecutor> = Arc::new(task_executor.build());
        LocalQueryEngine {
            binder: QueryBinder::new(catalog, function_catalog),
            optimizer,
            planner,
            adaptive_policy,
            executor,
            write_storage,
            statistics_provider,
            runtime_statistics,
        }
    }
}
