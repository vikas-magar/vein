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

/// Execution over the engine's own vector representation.
pub mod execution;

/// Default stage factory mapping the wire protocol onto executor stages.
pub mod factory;
/// Execution seam: stage-factory, task-executor, and expression traits.
pub mod runtime;
/// Vector VM execution engine interpreting Micro-IR programs.
pub mod vm;

pub use pipeline::{DataChunk, PipelineNode, Sink, Source, Transform};
