use crate::pipeline::{DataChunkView, PipelineNode, Source};
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
impl Source<DataChunkView> for MemorySource {
    async fn execute(&mut self, next: &mut dyn PipelineNode<DataChunkView>) -> Result<()> {
        if let Some(batch) = self.batch.take() {
            next.push(DataChunkView::from_record_batch(batch)?)?;
        }
        next.flush()?;
        Ok(())
    }
}
