//! Stateless executor for Vein.
//!
//! Each Executor receives self-contained `ExecuteTask` messages,
//! reads Parquet files, applies operators, and pushes results to ESS.

#![deny(missing_docs)]

/// Physical Expression Evaluator
pub mod expr;

/// Operators for physical execution
pub mod operators;

/// Executor daemon entrypoints and runtime environment.
pub mod daemon;
/// Execution seam: stage-factory, task-executor, and expression traits.
pub mod runtime;
/// Default stage factory mapping the wire protocol onto executor stages.
pub mod factory;
/// Ready-to-run executor node (worker pool + control plane + task executor).
pub mod executor_node;

pub use executor_node::ExecutorNode;
