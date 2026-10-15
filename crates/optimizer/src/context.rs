//! Shared optimizer configuration, context, and execution diagnostics.

use std::sync::Arc;

use plan::{TableMetadata, TableStatistics};
use crate::estimation::{CardinalityEstimator, NodeEstimate, StatisticsEstimator};

mod diagnostics;
pub use diagnostics::{
    JoinCardinalityAssumption, JoinCostBreakdown, JoinOrderAlternative, JoinOrderDecision,
    JoinOrderOutcome, OptimizationDiagnostics, OptimizerPhase, RuleExecution,
};

/// Optimizer execution configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OptimizerConfig {
    /// Maximum number of complete rule sweeps before optimization fails.
    pub max_sweeps: usize,
    /// Bounds for statistics-driven join-order search.
    pub join_order: JoinOrderConfig,
}

impl Default for OptimizerConfig {
    fn default() -> Self {
        Self {
            max_sweeps: 8,
            join_order: JoinOrderConfig::default(),
        }
    }
}

/// Resource limits for exact join-order enumeration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinOrderConfig {
    /// Maximum number of relations in an exact-search region.
    pub max_relations: usize,
    /// Maximum number of subset splits examined by one region.
    pub max_candidate_splits: usize,
}

impl Default for JoinOrderConfig {
    fn default() -> Self {
        Self {
            max_relations: 8,
            max_candidate_splits: 10_000,
        }
    }
}

/// Freshness supplied by the statistics owner, independently of its estimates.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatisticsFreshness {
    /// The provider validated the snapshot against the current table revision.
    Current,
    /// Catalog statistics do not carry a freshness guarantee.
    Unverified,
    /// The provider knows the snapshot no longer describes the table.
    Stale,
}

/// Borrowed statistics with explicit freshness; no metadata or bounds are cloned.
#[derive(Debug, Clone, Copy)]
pub struct StatisticsSnapshot<'a> {
    /// Statistics owned by the catalog or provider.
    pub statistics: &'a TableStatistics,
    /// The provider's freshness assessment.
    pub freshness: StatisticsFreshness,
}

/// Supplies borrowed table statistics to optimizer models.
pub trait StatisticsProvider: Send + Sync {
    /// Returns statistics for a table when available.
    fn table_statistics<'a>(&'a self, table: &'a TableMetadata) -> Option<StatisticsSnapshot<'a>>;
}

/// Reads statistics directly from catalog table metadata.
#[derive(Debug, Default)]
pub struct CatalogStatisticsProvider;

impl StatisticsProvider for CatalogStatisticsProvider {
    fn table_statistics<'a>(&'a self, table: &'a TableMetadata) -> Option<StatisticsSnapshot<'a>> {
        table.statistics.as_ref().map(|statistics| StatisticsSnapshot {
            statistics,
            freshness: StatisticsFreshness::Unverified,
        })
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
    pub fn table_statistics<'a>(&'a self, table: &'a TableMetadata) -> Option<StatisticsSnapshot<'a>> {
        self.statistics.table_statistics(table)
    }

    /// Estimates the authoritative logical tree using the shared provider.
    pub fn estimate_plan(&self, plan: &plan::LogicalPlan) -> Vec<NodeEstimate> {
        StatisticsEstimator::new(self.statistics.as_ref()).estimate_plan(plan)
    }

    pub(crate) fn estimate_relation<'a>(
        &'a self,
        plan: &'a plan::LogicalPlan,
    ) -> crate::estimation::RelationEstimate<'a> {
        StatisticsEstimator::new(self.statistics.as_ref()).estimate_relation(plan)
    }
}

impl Default for OptimizerContext {
    fn default() -> Self {
        Self::new(OptimizerConfig::default())
    }
}
