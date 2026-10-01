use crate::pipeline::{DataChunk, PipelineNode, Source};
use arrow::record_batch::RecordBatch;
use common::Result;

/// An in-memory data source over a single [`RecordBatch`].
pub struct MemorySource {
    batch: Option<RecordBatch>,
}

impl MemorySource {
    /// Create a memory source from a single RecordBatch.
    pub fn new(batch: RecordBatch) -> Self {
        Self { batch: Some(batch) }
    }
}

#[async_trait::async_trait]
impl Source<DataChunk> for MemorySource {
    async fn execute(&mut self, next: &mut dyn PipelineNode<DataChunk>) -> Result<()> {
        if let Some(batch) = self.batch.take() {
            let chunk = DataChunk::from_record_batch(batch);
            next.push(chunk)?;
        }
        next.flush()?;
        Ok(())
    }
}
