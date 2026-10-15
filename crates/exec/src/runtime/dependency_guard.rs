use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use common::protocol::PipelineId;
use common::Result;
use error::VeinError;

use crate::pipeline::{DataChunkView, PipelineNode};

/// Guards the first probe push (or empty-input flush) with graph prerequisites.
pub(super) struct PipelineDependencyGuard {
    inner: Box<dyn PipelineNode<DataChunkView>>,
    dependencies: Vec<(PipelineId, Arc<AtomicBool>)>,
    checked: bool,
}

impl PipelineDependencyGuard {
    pub(super) fn new(
        inner: Box<dyn PipelineNode<DataChunkView>>,
        dependencies: Vec<(PipelineId, Arc<AtomicBool>)>,
    ) -> Self {
        Self {
            inner,
            dependencies,
            checked: false,
        }
    }

    fn ensure_ready(&mut self) -> Result<()> {
        if self.checked {
            return Ok(());
        }
        if let Some((pipeline, _)) = self
            .dependencies
            .iter()
            .find(|(_, completed)| !completed.load(Ordering::Acquire))
        {
            return Err(VeinError::internal(format!(
                "join probe started before prerequisite pipeline {:?} completed",
                pipeline
            )));
        }
        self.checked = true;
        Ok(())
    }
}

impl PipelineNode<DataChunkView> for PipelineDependencyGuard {
    fn push(&mut self, view: DataChunkView) -> Result<()> {
        self.ensure_ready()?;
        self.inner.push(view)
    }

    fn flush(&mut self) -> Result<()> {
        self.ensure_ready()?;
        self.inner.flush()
    }

    fn finished(&self) -> bool {
        self.inner.finished()
    }

    fn fork_local(&self) -> Result<Option<Box<dyn PipelineNode<DataChunkView>>>> {
        let Some(inner) = self.inner.fork_local()? else {
            return Ok(None);
        };
        Ok(Some(Box::new(Self::new(
            inner,
            self.dependencies.clone(),
        ))))
    }

    fn combine_local(
        self: Box<Self>,
        canonical: &mut dyn PipelineNode<DataChunkView>,
    ) -> Result<()> {
        let target = canonical
            .as_any_mut()
            .downcast_mut::<PipelineDependencyGuard>()
            .ok_or_else(|| VeinError::internal("join dependency state has a mismatched target"))?;
        if self.dependencies.len() != target.dependencies.len()
            || !self
            .dependencies
            .iter()
            .zip(&target.dependencies)
            .all(|(local, canonical)| local.0 == canonical.0)
        {
            return Err(VeinError::internal(
                "cannot combine join probes with different dependencies",
            ));
        }
        self.inner.combine_local(target.inner.as_mut())?;
        target.checked |= self.checked;
        Ok(())
    }
}
