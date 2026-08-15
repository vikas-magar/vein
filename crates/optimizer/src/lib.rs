#![deny(missing_docs)]

//! The Vein logical plan optimizer.
//!
//! An extensible rewrite framework over [`plan::LogicalPlan`]: rules
//! transform plan nodes bottom-up and an [`OptimizerPipeline`] drives them to
//! a fixpoint. Production pipelines are assembled with
//! [`OptimizerPipeline::default`]; custom pipelines compose rules via
//! [`OptimizerPipeline::with_rule`].

/// The optimizer rule trait and rule categories.
pub mod rule;

/// The pipeline that applies rules to a plan.
pub mod optimizer;

/// Concrete optimizer rules.
pub mod rules;

pub use optimizer::{Optimizer, OptimizerPipeline};
pub use rule::{OptimizerRule, RuleType};
pub use rules::constant_fold::ConstantFoldingRule;