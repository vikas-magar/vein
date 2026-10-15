#![deny(missing_docs)]

//! The Vein logical plan optimizer.
//!
//! An extensible rewrite framework over logical plans. Production pipelines
//! are assembled by the default pipeline; custom pipelines compose rules.

/// Shared optimizer configuration and diagnostics.
pub mod context;

/// Borrowed metadata estimates and extensible cardinality/selectivity strategies.
/// Includes validated scan-schema bindings shared with physical predicate staging.
pub mod estimation;

mod statistics;

/// The optimizer rule trait and rule categories.
pub mod rule;

/// The pipeline that applies rules to a plan.
pub mod optimizer;

/// Concrete optimizer rules.
pub mod rules;

pub use context::{
    CatalogStatisticsProvider, JoinCardinalityAssumption, JoinCostBreakdown,
    JoinOrderAlternative, JoinOrderConfig, JoinOrderDecision, JoinOrderOutcome,
    OptimizationDiagnostics, OptimizerConfig, OptimizerContext, OptimizerPhase, RuleExecution,
    StatisticsFreshness, StatisticsProvider, StatisticsSnapshot,
};
pub use optimizer::{Optimizer, OptimizerPipeline};
pub use rule::{OptimizerRule, RuleType};
pub use rules::{
    column_pruning::ColumnPruningRule, constant_fold::ConstantFoldingRule,
    expression_simplify::ExpressionSimplificationRule, filter_normalize::FilterNormalizationRule,
    join_transitivity::JoinPredicateTransitivityRule, predicate_pushdown::PredicatePushdownRule,
    scan_filter_pushdown::ScanFilterPushdownRule,
    scan_statistics_pruning::ScanStatisticsPruningRule,
    scalar_aggregate_decorrelation::ScalarAggregateDecorrelationRule,
};

/// Join-order cost model extension point and default cumulative-cardinality model.
pub use rules::join_order::{
    CumulativeCardinalityCost, JoinCostInput, JoinCostModel, JoinOrderRule,
};
