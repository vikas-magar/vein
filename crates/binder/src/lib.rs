#![deny(missing_docs)]

//! Vein Binder: the semantic frontend of the engine.
//!
//! Consumes the syntax-only AST produced by the parser and produces a fully
//! resolved, type-coerced [`plan::LogicalPlan`]. Column resolution, scoping,
//! correlation, type coercion and window splitting all happen here; plan
//! optimization happens later, in the optimizer.

/// Core traits for binder and function registry.
pub mod traits;

/// Built-in and user-registered function signatures.
pub mod functions;

/// Evaluation semantics for pipe operators over bound plans.
pub mod pipe;

/// Lexical scope stack and column resolution.
pub mod scope;

/// The expression binder: AST expressions into logical expressions.
pub mod expr;

/// Type coercion framework for binary operations.
pub mod coerce;

/// Date/time literal parsing helpers.
pub mod date_time;

/// Wildcard (`*`, `table.*`) expansion.
pub mod star;

/// Window function binding and lifting.
pub mod window;

/// Binder context: catalog, functions, CTEs, and column id allocation.
pub mod context;

/// Testing utilities (mock function registry, end-to-end bind helper).
#[cfg(test)]
pub mod testing;

pub use pipe::query::QueryBinder;
pub use functions::BuiltinFunctionRegistry;
pub use traits::{FunctionKind, FunctionRegistry, FunctionSignature, LogicalBinder, RuntimeKind};

#[cfg(test)]
mod tests;