//! Constructors and resource configuration for the default task executor.

use super::*;

impl DefaultTaskExecutor {
    /// Create a task executor that builds pipelines with `builder`.
    pub fn new(builder: Arc<dyn ExecutorBuilder>) -> Self {
        Self {
            builder,
            resource_limits: ExecutionResourceLimits::default(),
            spill_storage_factory: Arc::new(LocalSpillStorageFactory),
            intermediate_sources: checkpoint::IntermediateSourceRegistry::default(),
        }
    }

    /// Create a task executor with a per-execution budget for blocking state.
    pub fn with_memory_limit(builder: Arc<dyn ExecutorBuilder>, memory_limit: usize) -> Self {
        Self::with_resource_limits(
            builder,
            ExecutionResourceLimits::new(memory_limit, u64::MAX),
        )
    }

    /// Create a task executor with memory and spill-disk limits per execution.
    pub fn with_resource_limits(
        builder: Arc<dyn ExecutorBuilder>,
        resource_limits: ExecutionResourceLimits,
    ) -> Self {
        Self {
            builder,
            resource_limits,
            spill_storage_factory: Arc::new(LocalSpillStorageFactory),
            intermediate_sources: checkpoint::IntermediateSourceRegistry::default(),
        }
    }

    /// Use a factory that creates one isolated spill backend per query.
    pub fn with_spill_storage_factory(mut self, factory: Arc<dyn SpillStorageFactory>) -> Self {
        self.spill_storage_factory = factory;
        self
    }
}
