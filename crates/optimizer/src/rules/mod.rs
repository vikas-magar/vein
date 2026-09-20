//! Concrete optimizer rules.

/// Heuristic evaluation of constant expressions at plan time.
pub mod constant_fold;

/// Semantics-preserving algebraic expression simplification.
pub mod expression_simplify;

/// Filter predicate and plan-node normalization.
pub mod filter_normalize;

pub use constant_fold::ConstantFoldingRule;
pub use expression_simplify::ExpressionSimplificationRule;
pub use filter_normalize::FilterNormalizationRule;