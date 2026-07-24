//! Stateless executor for Vein.
//!
//! Each Executor receives self-contained `ExecuteTask` messages,
//! reads Parquet files, applies operators, and pushes results to ESS.

#![deny(missing_docs)]

/// Parquet scan stage — reads a file and streams RecordBatches.
pub mod scan;
