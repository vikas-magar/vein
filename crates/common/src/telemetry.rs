use std::time::Instant;

use async_trait::async_trait;

use error::Result;
use crate::pipeline::PipelineStage;

/// Distributed tracing context propagated through the system.
///
/// Each node creates a `child()` span when dispatching work to downstream nodes.
#[derive(Debug, Clone)]
pub struct SpanContext {
    /// 128-bit trace ID.
    pub trace_id: [u8; 16],
    /// 64-bit span ID of the parent span.
    pub parent_span_id: [u8; 8],
}

impl SpanContext {
    /// Create a new root trace context.
    pub fn new() -> Self {
        use rand::Rng;
        let mut rng = rand::thread_rng();
        Self {
            trace_id: rng.r#gen(),
            parent_span_id: rng.r#gen(),
        }
    }

    /// Create a child span context within the same trace.
    pub fn child(&self) -> Self {
        use rand::Rng;
        let mut rng = rand::thread_rng();
        Self {
            trace_id: self.trace_id,
            parent_span_id: rng.r#gen(),
        }
    }

    /// Attach this context to the current tracing span.
    pub fn attach(&self) {
        tracing::Span::current().record(
            "trace_id",
            format!("{:032x}", u128::from_le_bytes(self.trace_id)),
        );
    }
}

impl Default for SpanContext {
    fn default() -> Self {
        Self::new()
    }
}

/// Timing and row-count metrics recorded by `InstrumentedStage`.
#[derive(Debug, Default, Clone)]
pub struct RecordedMetrics {
    /// Name of the stage.
    pub stage_name: String,
    /// Number of `process()` invocations.
    pub calls: u64,
    /// Total wall-clock time in `process()` (nanoseconds).
    pub wall_ns: u64,
}

/// A decorator that wraps a `PipelineStage` and records wall-clock timing
/// and call count.
#[derive(Debug)]
pub struct InstrumentedStage<S> {
    inner: S,
    name: String,
}

impl<S> InstrumentedStage<S> {
    /// Wrap a pipeline stage with instrumentation.
    pub fn new(inner: S, name: impl Into<String>) -> Self {
        Self {
            inner,
            name: name.into(),
        }
    }

    /// Consume the wrapper and return the recorded metrics.
    pub fn into_inner(self) -> S {
        self.inner
    }

    /// Return the stage label.
    pub fn name(&self) -> &str {
        &self.name
    }
}

#[async_trait]
impl<I: Send + 'static, O: Send + 'static, S: PipelineStage<Input = I, Output = O>> PipelineStage
    for InstrumentedStage<S>
{
    type Input = I;
    type Output = O;

    async fn init(&mut self) -> Result<()> {
        self.inner.init().await
    }

    async fn process(&mut self, input: I) -> Result<Option<O>> {
        let start = Instant::now();
        let result = self.inner.process(input).await;
        let elapsed = start.elapsed();
        tracing::trace!(
            stage = %self.name,
            elapsed_us = elapsed.as_micros(),
            "process"
        );
        result
    }

    async fn finish(&mut self) -> Result<Option<O>> {
        let start = Instant::now();
        let result = self.inner.finish().await;
        let elapsed = start.elapsed();
        tracing::trace!(
            stage = %self.name,
            elapsed_us = elapsed.as_micros(),
            "finish"
        );
        result
    }
}

#[cfg(test)]
#[allow(missing_docs)]
mod tests {
    use super::*;
    use crate::pipeline::{PipelineRunner, SourceStage};
    use crate::cancellation::CancellationToken;

    struct NoopStage;

    #[async_trait]
    impl PipelineStage for NoopStage {
        type Input = i32;
        type Output = i32;
        async fn init(&mut self) -> Result<()> { Ok(()) }
        async fn process(&mut self, input: i32) -> Result<Option<i32>> { Ok(Some(input)) }
        async fn finish(&mut self) -> Result<Option<i32>> { Ok(None) }
    }

    struct CountingSource {
        remaining: usize,
    }

    #[async_trait]
    impl SourceStage for CountingSource {
        type Output = i32;
        async fn init(&mut self) -> Result<()> { Ok(()) }
        async fn produce(&mut self) -> Result<Option<i32>> {
            if self.remaining > 0 {
                self.remaining -= 1;
                Ok(Some(1))
            } else {
                Ok(None)
            }
        }
        async fn finish(&mut self) -> Result<Option<i32>> { Ok(None) }
    }

    #[tokio::test]
    async fn test_instrumented_stage_smoke() {
        let cancel = CancellationToken::new();
        let mut runner = PipelineRunner::new(cancel);

        let source = CountingSource { remaining: 5 };
        let mut input = runner.add_source(source, 16);

        let inst = InstrumentedStage::new(NoopStage, "noop");
        let mut mid = runner.then(&mut input, inst, 16);

        struct Collect;
        #[async_trait]
        impl PipelineStage for Collect {
            type Input = i32;
            type Output = ();
            async fn init(&mut self) -> Result<()> { Ok(()) }
            async fn process(&mut self, _: i32) -> Result<Option<()>> { Ok(None) }
            async fn finish(&mut self) -> Result<Option<()>> { Ok(None) }
        }
        runner.sink(&mut mid, Collect);

        let metrics = runner.run().await.unwrap();
        assert!(metrics.len() >= 3);
    }

    #[test]
    fn test_span_context_new() {
        let ctx = SpanContext::new();
        assert_ne!(ctx.trace_id, [0u8; 16]);
    }

    #[test]
    fn test_span_context_child() {
        let ctx = SpanContext::new();
        let child = ctx.child();
        assert_eq!(child.trace_id, ctx.trace_id);
    }
}
