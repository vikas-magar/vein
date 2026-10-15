use std::sync::{Arc, Mutex};

use crate::pipeline::{DataChunkView, PipelineNode};
use common::Result;
use error::VeinError;

#[derive(Clone, Default)]
/// Collects runtime filters derived from the join build side.
pub struct JoinFilterCollector {
    output: Arc<Mutex<Option<DataChunkView>>>,
}

impl JoinFilterCollector {
    /// Clears the previous candidate batch before evaluating another one.
    pub fn clear(&self) -> Result<()> {
        *self.output.lock().map_err(|_| VeinError::internal("join filter collector poisoned"))? = None;
        Ok(())
    }

    /// Takes the filtered candidate batch, if the predicate retained any rows.
    pub fn take(&self) -> Result<Option<DataChunkView>> {
        Ok(self.output.lock().map_err(|_| VeinError::internal("join filter collector poisoned"))?.take())
    }
}

impl PipelineNode<DataChunkView> for JoinFilterCollector {
    fn push(&mut self, view: DataChunkView) -> Result<()> {
        *self.output.lock().map_err(|_| VeinError::internal("join filter collector poisoned"))? = Some(view);
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        Ok(())
    }
}
