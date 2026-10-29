use crate::pipeline::{DataChunkView, PipelineNode, Source};
use common::Result;

/// An in-memory data source over a single reference-backed view.
pub struct MemorySource {
    view: Option<DataChunkView>,
}

impl MemorySource {
    /// Create a memory source from a view without changing its row mapping.
    pub fn new(view: DataChunkView) -> Self {
        Self { view: Some(view) }
    }
}

#[async_trait::async_trait]
impl Source for MemorySource {
    async fn execute(&mut self, next: &mut dyn PipelineNode) -> Result<()> {
        if let Some(view) = self.view.take() {
            next.push(view).await?;
        }
        next.flush().await?;
        Ok(())
    }
}
