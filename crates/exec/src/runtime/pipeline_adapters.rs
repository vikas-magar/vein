use super::*;

/// A [`PipelineNode`] wrapper that aborts the pipeline if cancellation has been
/// requested. Placed just above the sink, it is invoked once per batch that
/// reaches the end of the chain, letting a long-running source stop promptly.
pub(super) struct CancelGuard {
    inner: Box<dyn PipelineNode>,
    cancel: CancellationToken,
}

impl CancelGuard {
    pub(super) fn new(inner: Box<dyn PipelineNode>, cancel: CancellationToken) -> Self {
        Self { inner, cancel }
    }
}

#[async_trait::async_trait]
impl PipelineNode for CancelGuard {
    async fn push(&mut self, view: DataChunkView) -> Result<()> {
        if self.cancel.is_cancelled() {
            return Err(VeinError::cancelled());
        }
        self.inner.push(view).await
    }

    async fn flush(&mut self) -> Result<()> {
        if self.cancel.is_cancelled() {
            return Err(VeinError::cancelled());
        }
        self.inner.flush().await
    }
}

/// A [`PipelineNode`] adapter that forwards pushes into a shared sink, so the
/// runner can retrieve the sink's result via [`Sink::collect`] after execution.
pub(super) struct SinkHandle(pub(super) Arc<tokio::sync::Mutex<Box<dyn Sink>>>);

#[async_trait::async_trait]
impl PipelineNode for SinkHandle {
    async fn push(&mut self, view: DataChunkView) -> Result<()> {
        self.0
            .lock()
            .await
            .push(view).await
    }

    async fn flush(&mut self) -> Result<()> {
        self.0
            .lock()
            .await
            .flush().await
    }
}

/// Adapts an already-built transform so another pipeline stage can own it as a node.
pub(super) struct TransformHandle(pub(super) Box<dyn Transform>);

#[async_trait::async_trait]
impl PipelineNode for TransformHandle {
    async fn push(&mut self, view: DataChunkView) -> Result<()> {
        self.0.push(view).await
    }

    async fn flush(&mut self) -> Result<()> {
        self.0.flush().await
    }

    fn finished(&self) -> bool {
        self.0.finished()
    }

    fn fork_local(&self) -> Result<Option<Box<dyn PipelineNode>>> {
        let Some(transform) = self.0.fork_transform()? else {
            return Ok(None);
        };
        Ok(Some(Box::new(Self(transform))))
    }

    fn combine_local(
        self: Box<Self>,
        canonical: &mut dyn PipelineNode,
    ) -> Result<()> {
        let target = canonical
            .as_any_mut()
            .downcast_mut::<TransformHandle>()
            .ok_or_else(|| VeinError::internal("transform local state has a mismatched target"))?;
        self.0.combine_transform(target.0.as_mut())
    }
}
pub(super) struct ChannelPipelineNode {
    pub(super) sender: flume::Sender<DataChunkView>,
    pub(super) finished: Arc<AtomicBool>,
}


#[async_trait::async_trait]
impl PipelineNode for ChannelPipelineNode {
    async fn push(&mut self, view: DataChunkView) -> Result<()> {
        self.sender.send_async(view).await.map_err(|_| VeinError::cancelled())
    }
    async fn flush(&mut self) -> Result<()> {
        Ok(())
    }
    fn finished(&self) -> bool {
        self.finished.load(Ordering::Acquire)
    }
}
pub(super) struct SuppressFlush {
    inner: Box<dyn PipelineNode>,
}

impl SuppressFlush {
    pub(super) fn new(inner: Box<dyn PipelineNode>) -> Self {
        Self { inner }
    }

    #[cfg(target_arch = "wasm32")]
    pub(super) async fn finish(self) -> Result<()> {
        let mut inner = self.inner;
        inner.flush().await
    }
}

#[async_trait::async_trait]
impl PipelineNode for SuppressFlush {
    async fn push(&mut self, view: DataChunkView) -> Result<()> {
        self.inner.push(view).await
    }

    async fn flush(&mut self) -> Result<()> {
        Ok(())
    }

    fn combine_local(
        self: Box<Self>,
        canonical: &mut dyn PipelineNode,
    ) -> Result<()> {
        self.inner.combine_local(canonical)
    }
}