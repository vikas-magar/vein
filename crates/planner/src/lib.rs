#![deny(missing_docs)]

//! Vein Planner: Semantic frontend of the engine
//!
//! Consumes the syntax-only AST produced by parser and produces a fully
//! resolved, type-coerced LogicalPlan.

/// AST to Logical Plan binding logic
pub mod binder;
/// Logical expressions and operators
pub mod expr;
/// Plan optimization rules
pub mod optimizer;
/// Logical query plan structures
pub mod plan;
/// Data types and schema definitions
pub mod schema;
/// Testing utilities (InMemory VCS Catalog)
pub mod testing;
/// Core traits for binder and optimizer
pub mod traits;
