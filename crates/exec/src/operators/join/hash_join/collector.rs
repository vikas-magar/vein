use std::sync::{Arc, Mutex};

use crate::pipeline::{DataChunkView, PipelineNode};
use common::Result;
use error::VeinError;

#[derive(Clone, Default)]
/// Receives the residual predicate's accepted candidate view.
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

impl PipelineNode for JoinFilterCollector {
    fn push(&mut self, view: DataChunkView) -> Result<()> {
        *self.output.lock().map_err(|_| VeinError::internal("join filter collector poisoned"))? = Some(view);
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        Ok(())
    }

    fn combine_local(self: Box<Self>, canonical: &mut dyn PipelineNode) -> Result<()> {
        let target = canonical.as_any_mut().downcast_mut::<Self>()
            .ok_or_else(|| VeinError::internal("join residual collector type mismatch"))?;
        if self.take()?.is_some() || target.take()?.is_some() {
            return Err(VeinError::internal("join residual collector contains unconsumed output"));
        }
        Ok(())
    }
}
