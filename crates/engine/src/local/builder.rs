use std::sync::Arc;

use binder::QueryBinder;
use exec::factory::TaskExecutorBuilder;
use exec::runtime::TaskExecutor;
use functions::FunctionCatalog;

use super::{LocalEngineComponents, LocalQueryEngine};

/// Builds a local query engine from explicitly selected services.
pub struct LocalEngineBuilder {
    components: LocalEngineComponents,
}

impl LocalEngineBuilder {
    /// Creates a builder that requires explicit catalog, storage, optimizer,
    /// planner, functions, and scan-provider implementations.
    pub fn new(components: LocalEngineComponents) -> Self {
        Self { components }
    }

    /// Constructs orchestration and a task executor using the same metadata
    /// cache exposed by the injected catalog.
    pub fn build(self) -> LocalQueryEngine {
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
        } = self.components;
        let metadata_store = catalog.metadata_store();
        let function_catalog: Arc<dyn FunctionCatalog> = functions.clone();
        let executor: Arc<dyn TaskExecutor> = Arc::new(
            TaskExecutorBuilder::new()
                .with_read_storage_factory(read_storage_factory)
                .with_metadata_store(metadata_store)
                .with_runtime_statistics_store(runtime_statistics.clone())
                .with_scan_provider_registry(scan_providers)
                .with_function_registry(functions)
                .with_write_storage(write_storage.clone())
                .build(),
        );
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
