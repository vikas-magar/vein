//! Daemon architecture for the Executor node.
//!
//! Uses asynchronous worker-pool LocalSets to balance network IO (QUIC) with
//! CPU cache-local push pipelines.

/// Daemon context and shared state.
pub mod context;
/// QUIC network listener logic.
pub mod server;
/// LocalSet worker pool implementation.
pub mod worker;

pub use context::DaemonContext;
pub use server::{ExecutorDaemon, ExecutorDaemonBuilder, ExecutorDaemonConfig};
pub use worker::WorkerPool;

/// Pipeline abstractions re-exported from crate root.
pub use crate::pipeline;
pub use crate::pipeline::{DataChunk, PipelineNode, Sink, Source, Transform};

