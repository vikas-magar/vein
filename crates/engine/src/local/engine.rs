use std::sync::Arc;

use catalog::AnalyzedTableStatistics;
use common::Result;
use common::statistics::analysis::{
    StatisticsAnalysisProvider, analyze_table as analyze_statistics_table,
};
use common::statistics::{RuntimeStatisticsStore, StatisticsConfig};
use exec::operators::sink::WriteStorage;
use exec::runtime::TaskExecutor;
use optimizer::OptimizerPipeline;
use physical::PhysicalPlanner;

use super::AdaptiveCheckpointPolicy;

/// A local query engine whose external services are selected by its caller.
pub struct LocalQueryEngine {
    pub(in crate::local) binder: binder::QueryBinder,
    pub(in crate::local) optimizer: Arc<OptimizerPipeline>,
    pub(in crate::local) planner: Arc<dyn PhysicalPlanner>,
    pub(in crate::local) adaptive_policy: Arc<dyn AdaptiveCheckpointPolicy>,
    pub(in crate::local) executor: Arc<dyn TaskExecutor>,
    pub(in crate::local) write_storage: Arc<dyn WriteStorage>,
    pub(in crate::local) statistics_provider: Arc<dyn StatisticsAnalysisProvider>,
    pub(in crate::local) runtime_statistics: RuntimeStatisticsStore,
}

impl LocalQueryEngine {
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

    /// Returns the executor used for task and stream execution.
    pub fn task_executor(&self) -> &Arc<dyn TaskExecutor> {
        &self.executor
    }

    /// Replaces the task executor while preserving the rest of the engine
    /// composition. This is useful for custom runtimes and executor decorators.
    pub fn with_task_executor(mut self, executor: Arc<dyn TaskExecutor>) -> Self {
        self.executor = executor;
        self
    }

    /// Replaces the adaptive checkpoint policy.
    pub fn with_adaptive_policy(
        mut self,
        policy: Arc<dyn AdaptiveCheckpointPolicy>,
    ) -> Self {
        self.adaptive_policy = policy;
        self
    }
}
