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

/// Executor daemon entrypoints and runtime environment.
pub mod daemon;
/// Execution seam: stage-factory, task-executor, and expression traits.
pub mod runtime;
/// Default stage factory mapping the wire protocol onto executor stages.
pub mod factory;
/// Vector VM execution engine interpreting Micro-IR programs.
pub mod vm;

pub use daemon::{ExecutorDaemon, ExecutorDaemonBuilder};
pub use pipeline::{DataChunk, PipelineNode, Sink, Source, Transform};

