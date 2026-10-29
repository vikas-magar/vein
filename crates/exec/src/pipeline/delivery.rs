//! One delivery boundary for synchronous consumers and asynchronous pipelines.
use common::Result;
use super::{DataChunkView, PipelineNode};

/// Receives a batch, retaining its Arrow owners until delivery completes.
#[async_trait::async_trait]
pub trait BatchConsumer: Send {
    /// Completes when the consumer has accepted ownership of the batch.
    async fn consume(&mut self, batch: DataChunkView) -> Result<()>;
}

#[async_trait::async_trait]
impl<F> BatchConsumer for F
where
    F: FnMut(DataChunkView) -> Result<()> + Send,
{
    async fn consume(&mut self, batch: DataChunkView) -> Result<()> {
        self(batch)
    }
}

/// Borrows the downstream pipeline; does not buffer or materialize payloads.
pub struct PipelineConsumer<'a>(pub &'a mut dyn PipelineNode);

#[async_trait::async_trait]
impl BatchConsumer for PipelineConsumer<'_> {
    async fn consume(&mut self, batch: DataChunkView) -> Result<()> {
        self.0.push(batch).await
    }
}
