#![deny(missing_docs)]

//! The Vein logical plan optimizer.
//!
//! An extensible rewrite framework over [`plan::LogicalPlan`]: rules
//! transform plan nodes bottom-up and an [`OptimizerPipeline`] drives them to
//! a fixpoint. Production pipelines are assembled with
//! [`OptimizerPipeline::default`]; custom pipelines compose rules via
//! [`OptimizerPipeline::with_rule`].

/// Shared optimizer configuration and diagnostics.
pub mod context;

/// The optimizer rule trait and rule categories.
pub mod rule;

/// The pipeline that applies rules to a plan.
pub mod optimizer;

/// Concrete optimizer rules.
pub mod rules;

pub use context::{
    CatalogStatisticsProvider, OptimizationDiagnostics, OptimizerConfig, OptimizerContext,
    OptimizerPhase, RuleExecution, StatisticsProvider,
};
pub use optimizer::{Optimizer, OptimizerPipeline};
pub use rule::{OptimizerRule, RuleType};
pub use rules::constant_fold::ConstantFoldingRule;