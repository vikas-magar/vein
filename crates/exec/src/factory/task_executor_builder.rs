use std::sync::Arc;

use super::default_executor_builder::DefaultExecutorBuilder;
use crate::expression::ExpressionKernelProvider;
use crate::memory::ExecutionResourceLimits;
use crate::operators::join::JoinBloomFilterFactory;
use crate::operators::sink::WriteStorage;
use crate::operators::source::scan::ScanProviderRegistry;
use crate::runtime::ExecutorBuilder;
use crate::runtime::filter::{
    ConservativeRuntimeFilterPolicy, RuntimeFilterDecisionPolicy, RuntimeFilterFactory,
};
use crate::spill::{LocalSpillStorageFactory, SpillStorageFactory};
use catalog::MetadataStore;
use common::statistics::RuntimeStatisticsStore;
use functions::FunctionRegistry;
use storage::ReadStorageFactory;

/// Configures and builds the local pipeline task executor.
pub struct TaskExecutorBuilder {
    builder: Arc<dyn ExecutorBuilder>,
    registry: Arc<FunctionRegistry>,
    expression_kernels: Arc<dyn ExpressionKernelProvider>,
    storage: Option<Arc<dyn WriteStorage>>,
    metadata_store: MetadataStore,
    resource_limits: ExecutionResourceLimits,
    spill_storage_factory: Arc<dyn SpillStorageFactory>,
    runtime_filter_factory: Arc<dyn RuntimeFilterFactory>,
    runtime_filter_policy: Arc<dyn RuntimeFilterDecisionPolicy>,
    runtime_statistics: RuntimeStatisticsStore,
    scan_providers: ScanProviderRegistry,
    read_storage_factory: Option<Arc<dyn ReadStorageFactory>>,
}

impl TaskExecutorBuilder {
    /// Creates an executor builder with the default operators and providers.
    pub fn new() -> Self {
        let registry = Arc::new(FunctionRegistry::with_builtins());
        let expression_kernels = Arc::new(crate::expression::ExpressionKernelRegistry::with_builtins());
        let scan_providers = ScanProviderRegistry::default();
        let builder = DefaultExecutorBuilder::new(Arc::clone(&registry))
            .with_scan_provider_registry(scan_providers.clone())
            .with_expression_kernels(expression_kernels.clone());
        Self {
            builder: Arc::new(builder),
            registry,
            expression_kernels,
            storage: None,
            metadata_store: MetadataStore::new(),
            resource_limits: ExecutionResourceLimits::default(),
            spill_storage_factory: Arc::new(LocalSpillStorageFactory),
            runtime_filter_factory: Arc::new(JoinBloomFilterFactory::default()),
            runtime_filter_policy: Arc::new(ConservativeRuntimeFilterPolicy),
            runtime_statistics: RuntimeStatisticsStore::default(),
            scan_providers,
            read_storage_factory: None,
        }
    }

    /// Shares catalog-parsed metadata with scan readers.
    pub fn with_metadata_store(mut self, store: MetadataStore) -> Self {
        self.metadata_store = store;
        self.rebuild_default();
        self
    }

    /// Shares runtime scan observations with source operators.
    pub fn with_runtime_statistics_store(mut self, store: RuntimeStatisticsStore) -> Self {
        self.runtime_statistics = store;
        self.rebuild_default();
        self
    }

    /// Installs the providers used to plan and execute file scans.
    pub fn with_scan_provider_registry(mut self, registry: ScanProviderRegistry) -> Self {
        self.scan_providers = registry;
        self.rebuild_default();
        self
    }

    /// Installs the storage factory used by source scans.
    pub fn with_read_storage_factory(mut self, factory: Arc<dyn ReadStorageFactory>) -> Self {
        self.read_storage_factory = Some(factory);
        self.rebuild_default();
        self
    }

    /// Sets shared per-query memory and spill-disk budgets.
    pub fn with_resource_limits(mut self, limits: ExecutionResourceLimits) -> Self {
        self.resource_limits = limits;
        self
    }

    /// Configures the query-local spill backend factory.
    pub fn with_spill_storage_factory(mut self, factory: Arc<dyn SpillStorageFactory>) -> Self {
        self.spill_storage_factory = factory;
        self
    }

    /// Configures construction of query-local join filters.
    pub fn with_runtime_filter_factory(mut self, factory: Arc<dyn RuntimeFilterFactory>) -> Self {
        self.runtime_filter_factory = factory;
        self.rebuild_default();
        self
    }

    /// Configures selection of safe runtime-filter candidates.
    pub fn with_runtime_filter_decision_policy(
        mut self,
        policy: Arc<dyn RuntimeFilterDecisionPolicy>,
    ) -> Self {
        self.runtime_filter_policy = policy;
        self.rebuild_default();
        self
    }

    /// Sets the registry used to resolve functions during execution.
    pub fn with_function_registry(mut self, registry: Arc<FunctionRegistry>) -> Self {
        self.registry = registry;
        self.rebuild_default();
        self
    }

    /// Injects the transactional destination backend used by `WRITE` sinks.
    pub fn with_write_storage(mut self, storage: Arc<dyn WriteStorage>) -> Self {
        self.storage = Some(storage);
        self.rebuild_default();
        self
    }

    /// Installs a composed expression-kernel provider.
    pub fn with_expression_kernels(mut self, kernels: Arc<dyn ExpressionKernelProvider>) -> Self {
        self.expression_kernels = kernels;
        self.rebuild_default();
        self
    }

    fn rebuild_default(&mut self) {
        let mut builder = DefaultExecutorBuilder::new(Arc::clone(&self.registry))
            .with_expression_kernels(Arc::clone(&self.expression_kernels))
            .with_metadata_store(self.metadata_store.clone())
            .with_runtime_statistics_store(self.runtime_statistics.clone())
            .with_scan_provider_registry(self.scan_providers.clone())
            .with_runtime_filter_factory(Arc::clone(&self.runtime_filter_factory))
            .with_runtime_filter_decision_policy(Arc::clone(&self.runtime_filter_policy));
        if let Some(storage) = &self.storage {
            builder = builder.with_write_storage(Arc::clone(storage));
        }
        if let Some(factory) = &self.read_storage_factory {
            builder = builder.with_read_storage_factory(Arc::clone(factory));
        }
        self.builder = Arc::new(builder);
    }

    /// Replaces the protocol-to-pipeline builder entirely.
    pub fn with_executor_builder(mut self, builder: Arc<dyn ExecutorBuilder>) -> Self {
        self.builder = builder;
        self
    }

    /// Builds the task executor.
    pub fn build(self) -> crate::runtime::DefaultTaskExecutor {
        crate::runtime::DefaultTaskExecutor::with_resource_limits(self.builder, self.resource_limits)
            .with_spill_storage_factory(self.spill_storage_factory)
    }
}

impl Default for TaskExecutorBuilder {
    fn default() -> Self {
        Self::new()
    }
}
