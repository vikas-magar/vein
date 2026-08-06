use common::Result;
use async_trait::async_trait;
use common::protocol::MorselTask;
use crate::traits::TaskDispatcher;

/// The QUIC Task Dispatcher.
/// Responsible for serializing `MorselTask`s and streaming them to the appropriate
/// Executor node via a highly concurrent, multiplexed s2n-quic stream.
pub struct QuicTaskDispatcher;

impl QuicTaskDispatcher {
    pub fn new() -> Self {
        Self {}
    }
}

#[async_trait]
impl TaskDispatcher for QuicTaskDispatcher {
    async fn dispatch(&self, node_id: u32, task: MorselTask) -> Result<()> {
        // TODO: Look up QUIC connection for node_id and serialize task to Arrow IPC
        tracing::info!("QUIC Dispatcher sending MorselTask to node {}", node_id);
        Ok(())
    }
}
