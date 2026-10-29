//! Limit transform operator: truncates and offsets in-flight rows.

use crate::pipeline::{DataChunkView, PipelineNode, Transform};
use common::Result;
/// Resumable LIMIT state and stage adapters.
pub mod resumable;
mod window;
pub use resumable::LimitState;
use window::LimitWindow;

/// A synchronous [`Transform`] that limits the number of rows produced by a pipeline.
pub struct LimitTransform {
    window: LimitWindow,
    next: Box<dyn PipelineNode>,
}

impl LimitTransform {
    /// Create a new [`LimitTransform`] with the given offset and count.
    pub fn new(
        offset: u64,
        count: u64,
        next: Box<dyn PipelineNode>,
    ) -> Self {
        Self {
            window: LimitWindow::new(offset, count),
            next,
        }
    }
}

#[async_trait::async_trait]
impl PipelineNode for LimitTransform {
    async fn push(&mut self, chunk: DataChunkView) -> Result<()> {
        if let Some(limited) = self.window.apply(chunk)? {
            self.next.push(limited).await?;
        }
        Ok(())
    }

    async fn flush(&mut self) -> Result<()> {
        self.next.flush().await
    }

    /// Once the window is full, nothing further down can matter.
    fn finished(&self) -> bool {
        self.window.is_complete()
    }
}

impl Transform for LimitTransform {}

#[cfg(test)]
mod tests;
