use common::Result;
use async_trait::async_trait;
use common::protocol::MorselTask;
use crate::traits::TaskDispatcher;
use common::identifiers::NodeId;

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
    async fn dispatch(&self, _node_id: NodeId, _task: MorselTask) -> Result<()> {
        // TODO: Actually open a QUIC stream to the Executor node and serialize the MorselTask
        Ok(())
    }
}
