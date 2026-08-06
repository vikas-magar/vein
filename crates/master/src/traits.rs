use common::Result;
use async_trait::async_trait;
use common::protocol::MorselTask;

/// Highly abstract Task Dispatcher.
/// Allows swapping our QUIC network layer with TCP, gRPC, or an in-memory mock for local testing.
#[async_trait]
pub trait TaskDispatcher: Send + Sync {
    /// Dispatch a compiled physical Morsel to a specific Executor node over the network.
    async fn dispatch(&self, node_id: u32, task: MorselTask) -> Result<()>;
}

