//! Daemon architecture for the Executor node.
//! 
//! Uses Asynchronous Morsel-Driven LocalSets to perfectly balance
//! network IO (QUIC) and CPU cache-local push pipelines.

/// Daemon context and shared state.
pub mod context;
/// Pipeline abstractions for morsel push execution.
pub mod pipeline;
/// QUIC network listener logic.
pub mod server;
/// LocalSet worker pool implementation.
pub mod worker;

pub use context::DaemonContext;
pub use pipeline::{MorselTask, PipelineNode};
pub use server::ExecutorDaemon;
pub use worker::WorkerPool;
