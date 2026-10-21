use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::expression::{ExpressionKernelProvider, ExpressionKernelRegistry};
use crate::operators::join::JoinBloomFilterFactory;
use crate::operators::sink::WriteStorage;
use crate::operators::source::scan::ScanProviderRegistry;
use crate::runtime::filter::{
    ConservativeRuntimeFilterPolicy, RuntimeFilterDecisionPolicy, RuntimeFilterFactory,
};
use catalog::MetadataStore;
use common::statistics::RuntimeStatisticsStore;
use functions::FunctionRegistry;
use storage::ReadStorageFactory;

use super::DefaultExecutorBuilder;

impl DefaultExecutorBuilder {
    /// Creates a builder with the given function registry and default providers.
    pub fn new(registry: Arc<FunctionRegistry>) -> Self {
        Self {
            registry,
            expression_kernels: Arc::new(ExpressionKernelRegistry::with_builtins()),
            storage: None,
            metadata_store: MetadataStore::new(),
            runtime_filter_factory: Arc::new(JoinBloomFilterFactory::default()),
            runtime_filter_policy: Arc::new(ConservativeRuntimeFilterPolicy),
            runtime_statistics: RuntimeStatisticsStore::default(),
            scan_providers: ScanProviderRegistry::default(),
            read_storage_factory: None,
            storages: Mutex::new(HashMap::new()),
        }
    }

    /// Injects the kernel provider used by typed expression stages.
    pub fn with_expression_kernels(mut self, kernels: Arc<dyn ExpressionKernelProvider>) -> Self {
        self.expression_kernels = kernels;
        self
    }

    /// Attaches a transactional destination storage implementation.
    pub fn with_write_storage(mut self, storage: Arc<dyn WriteStorage>) -> Self {
        self.storage = Some(storage);
        self
    }

    /// Shares parsed metadata with catalog and scan consumers.
    pub fn with_metadata_store(mut self, store: MetadataStore) -> Self {
        self.metadata_store = store;
        self
    }

    /// Shares lazily collected runtime scan observations.
    pub fn with_runtime_statistics_store(mut self, store: RuntimeStatisticsStore) -> Self {
        self.runtime_statistics = store;
        self
    }

    /// Installs providers that plan and execute file scans.
    pub fn with_scan_provider_registry(mut self, registry: ScanProviderRegistry) -> Self {
        self.scan_providers = registry;
        self
    }

    /// Injects the backend factory used to open root-scoped read handles.
    pub fn with_read_storage_factory(mut self, factory: Arc<dyn ReadStorageFactory>) -> Self {
        self.read_storage_factory = Some(factory);
        self
    }

    /// Configures creation of query-local join filters.
    pub fn with_runtime_filter_factory(mut self, factory: Arc<dyn RuntimeFilterFactory>) -> Self {
        self.runtime_filter_factory = factory;
        self
    }

    /// Configures selection of safe join runtime-filter candidates.
    pub fn with_runtime_filter_decision_policy(
        mut self,
        policy: Arc<dyn RuntimeFilterDecisionPolicy>,
    ) -> Self {
        self.runtime_filter_policy = policy;
        self
    }
}
