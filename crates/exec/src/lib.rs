//! Stateless executor for Vein.
//!
//! Each Executor receives self-contained `ExecuteTask` messages,
//! reads registered file sources, applies operators, and pushes results to ESS.

#![deny(missing_docs)]

/// Streaming pipeline abstractions and the canonical reference-backed batch view.
pub mod pipeline;

/// Operators for physical execution.
pub mod operators;
/// Direct typed expression evaluation over reference-backed pipeline views.
pub mod expression;

/// Default stage factory mapping the wire protocol onto executor stages.
pub mod factory;
/// Execution seam: stage-factory, task-executor, and expression traits.
pub mod runtime;
/// Query-scoped reservation accounting for blocking operator state.
pub mod memory;
/// Query-scoped spill storage, accounting, and Arrow IPC run lifecycle.
pub mod spill;
/// Structured execution tracing for canonical plans and batch flow.
mod trace;

pub use pipeline::{DataChunkView, PipelineNode, Sink, Source, Transform};
