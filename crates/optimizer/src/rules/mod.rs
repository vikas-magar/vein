//! Concrete optimizer rules.

/// Heuristic evaluation of constant expressions at plan time.
pub mod constant_fold;

/// Semantics-preserving algebraic expression simplification.
pub mod expression_simplify;

pub use constant_fold::ConstantFoldingRule;
pub use expression_simplify::ExpressionSimplificationRule;