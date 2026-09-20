//! Shared optimizer configuration, context, and execution diagnostics.

use std::sync::Arc;
use std::time::Duration;

use plan::{TableMetadata, TableStatistics};

/// Optimizer execution configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OptimizerConfig {
    /// Maximum number of complete rule sweeps before optimization fails.
    pub max_sweeps: usize,
}

impl Default for OptimizerConfig {
    fn default() -> Self {
        Self { max_sweeps: 8 }
    }
}

/// Supplies table statistics to optimizer rules.
pub trait StatisticsProvider: Send + Sync {
    /// Returns statistics for a table when available.
    fn table_statistics(&self, table: &TableMetadata) -> Option<TableStatistics>;
}

/// Reads statistics directly from catalog table metadata.
#[derive(Debug, Default)]
pub struct CatalogStatisticsProvider;

impl StatisticsProvider for CatalogStatisticsProvider {
    fn table_statistics(&self, table: &TableMetadata) -> Option<TableStatistics> {
        table.statistics.clone()
    }
}

/// Shared context supplied to optimizer rules.
#[derive(Clone)]
pub struct OptimizerContext {
    config: Arc<OptimizerConfig>,
    statistics: Arc<dyn StatisticsProvider>,
}

impl OptimizerContext {
    /// Creates a context using catalog-provided table statistics.
    pub fn new(config: OptimizerConfig) -> Self {
        Self::with_statistics_provider(config, Arc::new(CatalogStatisticsProvider))
    }

    /// Creates a context with a custom statistics provider.
    pub fn with_statistics_provider(
        config: OptimizerConfig,
        statistics: Arc<dyn StatisticsProvider>,
    ) -> Self {
        Self {
            config: Arc::new(config),
            statistics,
        }
    }

    /// Returns the optimizer configuration.
    pub fn config(&self) -> &OptimizerConfig {
        self.config.as_ref()
    }

    /// Returns statistics for a table when available.
    pub fn table_statistics(&self, table: &TableMetadata) -> Option<TableStatistics> {
        self.statistics.table_statistics(table)
    }
}

impl Default for OptimizerContext {
    fn default() -> Self {
        Self::new(OptimizerConfig::default())
    }
}

/// A logical phase in which an optimizer rule executes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OptimizerPhase {
    /// Deterministic expression and plan normalization.
    Normalize,
    /// Always-safe heuristic rewrites.
    Heuristic,
    /// Statistics-driven alternative selection.
    CostBased,
}

/// Diagnostics for one rule invocation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuleExecution {
    /// Rule name.
    pub rule_name: String,
    /// Rule phase.
    pub phase: OptimizerPhase,
    /// Sweep number, starting at one.
    pub sweep: usize,
    /// Whether the rule changed the plan.
    pub changed: bool,
    /// Time spent applying the rule.
    pub elapsed: Duration,
}

/// Diagnostics returned with an optimized plan.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct OptimizationDiagnostics {
    /// Per-rule execution records.
    pub rule_executions: Vec<RuleExecution>,
    /// Number of completed sweeps.
    pub sweeps: usize,
}
