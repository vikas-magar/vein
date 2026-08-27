//! Daemon architecture for the Executor node.
//!
//! Uses asynchronous worker-pool LocalSets to balance network IO (QUIC) with
//! CPU cache-local push pipelines.

/// Daemon context and shared state.
pub mod context;
/// Pipeline abstractions for push execution.
pub mod pipeline;
/// QUIC network listener logic.
pub mod server;
/// LocalSet worker pool implementation.
pub mod worker;

pub use context::DaemonContext;
pub use pipeline::{PipelineNode, Sink, Source, Transform};
pub use server::ExecutorDaemon;
pub use worker::WorkerPool;
