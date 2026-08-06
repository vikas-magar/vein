//! The Extensible Optimizer Framework.
//! Contains the Cascades Memo architecture, Rules, and Cost Models for ML optimization.

/// Memoization structures for Cost-Based Optimizer.
pub mod memo;
/// Optimizer Rule trait and types.
pub mod rule;
/// Cost Models and Statistics.
pub mod cost;
/// Implementations of optimizer rules.
pub mod rules;
/// Adopt optimizer module.
pub mod adopt_optimizer;
