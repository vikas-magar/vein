#![deny(missing_docs)]

//! Vein Planner: Semantic frontend of the engine
//!
//! Consumes the syntax-only AST produced by parser and produces a fully
//! resolved, type-coerced LogicalPlan.

/// Data types and schema definitions
pub mod schema;
/// Logical expressions and operators
pub mod expr;
/// Logical query plan structures
pub mod plan;
/// Core traits for binder and optimizer
pub mod traits;
/// AST to Logical Plan binding logic
pub mod binder;
/// Plan optimization rules
pub mod optimizer;
/// Testing utilities (InMemory VCS Catalog)
pub mod testing;
