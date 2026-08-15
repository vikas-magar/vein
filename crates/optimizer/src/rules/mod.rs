//! Concrete optimizer rules.

/// Heuristic evaluation of constant expressions at plan time.
pub mod constant_fold;

pub use constant_fold::ConstantFoldingRule;