//! Stateless executor for Vein.
//!
//! Each Executor receives self-contained `ExecuteTask` messages,
//! reads Parquet files, applies operators, and pushes results to ESS.

#![deny(missing_docs)]

/// Parquet scan stage — reads a file and streams RecordBatches.
pub mod scan;

/// Physical Expression Evaluator
pub mod expr;

/// Operators for physical execution
pub mod operators;

/// Executor daemon entrypoints and runtime environment.
pub mod daemon;
