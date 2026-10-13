//! Stateless executor for Vein.
//!
//! Each Executor receives self-contained `ExecuteTask` messages,
//! reads Parquet files, applies operators, and pushes results to ESS.

#![deny(missing_docs)]

/// Streaming pipeline abstractions and DataChunk primitives.
pub mod pipeline;

/// User-Defined Function (UDF) extensibility and registry.
pub mod udf;

/// Operators for physical execution.
pub mod operators;

/// Default stage factory mapping the wire protocol onto executor stages.
pub mod factory;
/// Execution seam: stage-factory, task-executor, and expression traits.
pub mod runtime;
/// Query-scoped reservation accounting for blocking operator state.
pub mod memory;
/// Query-local runtime filters shared by joins and capable scan adapters.
pub mod runtime_filter;
/// Typed Parquet footer statistics shared by catalog discovery and scan pruning.
pub mod parquet_statistics;
/// Query-scoped spill storage, accounting, and Arrow IPC run lifecycle.
pub mod spill;
/// Vector VM execution engine interpreting Micro-IR programs.
pub mod vm;

/// Structured execution tracing for canonical plans and batch flow.
mod trace;

pub use pipeline::{DataChunk, PipelineNode, Sink, Source, Transform};
