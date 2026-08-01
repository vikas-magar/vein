#![deny(missing_docs)]
//! The Query Planner and Optimizer for Vein.
//! 
//! This crate is responsible for taking a raw, untyped AST from `parser`,
//! binding it against a catalog to produce a valid `LogicalPlan`, and then
//! heavily optimizing that plan using rule-based and cost-based optimizers.

/// Logical expressions.
pub mod expr;

/// Logical plan nodes.
pub mod plan;

/// Binder for AST -> LogicalPlan.
pub mod binder;

/// Extensible optimizer framework (Cascades, Rules, Cost Models).
pub mod optimizer;
