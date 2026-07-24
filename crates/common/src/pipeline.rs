use async_trait::async_trait;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::cancellation::CancellationToken;
use error::{Result, VeinError};

/// A processing stage in a data pipeline.
///
/// # Lifecycle
/// 1. `init()` — called once before data flows. Opens files, allocates state.
/// 2. `process(input)` — called N times. Returns `Some(output)` or `None`.
/// 3. `finish()` — called after the last input. Emits remaining buffered output.
#[async_trait]
pub trait PipelineStage: Send + 'static {
    /// The type this stage consumes.
    type Input: Send + 'static;
    /// The type this stage produces.
    type Output: Send + 'static;

    /// Initialize the stage. Called once before any `process` calls.
    async fn init(&mut self) -> Result<()>;

    /// Process a single input. Return `Some(output)` to emit, or `None` to signal no more output.
    async fn process(&mut self, input: Self::Input) -> Result<Option<Self::Output>>;

    /// Finish processing. Return any remaining buffered output.
    async fn finish(&mut self) -> Result<Option<Self::Output>>;
}

/// A source stage that produces data without consuming input.
#[async_trait]
pub trait SourceStage: Send + 'static {
    /// The type this source produces.
    type Output: Send + 'static;

    /// Initialize the source.
    async fn init(&mut self) -> Result<()>;

    /// Produce the next item. Return `None` when the source is exhausted.
    async fn produce(&mut self) -> Result<Option<Self::Output>>;

    /// Finish the source and return any remaining output.
    async fn finish(&mut self) -> Result<Option<Self::Output>>;
}

/// A handle carrying the output channel of one stage for connection to the next.
///
/// Created by `add_source` and `then`. Passed by mutable reference to `then` or `sink`.
pub struct PipelineInput<I> {
    rx: Option<mpsc::Receiver<Result<I>>>,
}

impl<I> PipelineInput<I> {
    /// Take the internal receiver, leaving `None`. Called by `then` / `sink`.
    pub fn take_rx(&mut self) -> Option<mpsc::Receiver<Result<I>>> {
        self.rx.take()
    }
}

/// Per-stage execution metrics collected during a pipeline run.
#[derive(Debug, Default, Clone)]
pub struct PerStageMetrics {
    /// Name or label of the stage.
    pub stage_name: String,
    /// Number of batches received as input.
    pub batches_in: u64,
    /// Number of batches produced as output (for processing stages).
    pub batches_out: u64,
    /// Number of records processed (input rows).
    pub rows_in: u64,
    /// Total wall-clock time spent in `process()`.
    pub wall_ns: u64,
}

impl PerStageMetrics {
    fn new(name: &str) -> Self {
        Self {
            stage_name: name.to_string(),
            ..Default::default()
        }
    }
}

/// Aggregate metrics for an entire pipeline run.
pub type StageMetrics = Vec<PerStageMetrics>;

/// Orchestrates execution of a pipeline stage graph.
///
/// ```ignore
/// let mut runner = PipelineRunner::new(cancel);
/// let input = runner.add_source(source, 64);
/// let mid = runner.then(&mut input, filter, 64);
/// runner.sink(&mut mid, sink);
/// let metrics = runner.run().await?;
/// ```
pub struct PipelineRunner {
    cancel: CancellationToken,
    handles: Vec<JoinHandle<Result<PerStageMetrics>>>,
}

impl PipelineRunner {
    /// Create a new runner. The `cancel` token stops all stages when triggered.
    pub fn new(cancel: CancellationToken) -> Self {
        Self {
            cancel,
            handles: Vec::new(),
        }
    }

    /// Add a source stage. Returns a `PipelineInput` carrying the source's output channel.
    pub fn add_source<S: SourceStage + 'static>(
        &mut self,
        stage: S,
        buffer: usize,
    ) -> PipelineInput<S::Output> {
        let (tx, rx) = mpsc::channel(buffer);
        let cancel = self.cancel.child();
        let handle: JoinHandle<Result<PerStageMetrics>> = tokio::spawn(async move {
            let mut stage = stage;
            let mut metrics = PerStageMetrics::new(std::any::type_name::<S>());
            stage.init().await?;
            loop {
                if cancel.is_cancelled() {
                    return Err(VeinError::cancelled());
                }
                match stage.produce().await? {
                    Some(item) => {
                        metrics.batches_in += 1;
                        tx.send(Ok(item))
                            .await
                            .map_err(|_| VeinError::internal("source channel closed"))?;
                    }
                    None => break,
                }
            }
            while let Some(item) = stage.finish().await? {
                tx.send(Ok(item))
                    .await
                    .map_err(|_| VeinError::internal("source finish send failed"))?;
            }
            Ok(metrics)
        });
        self.handles.push(handle);
        PipelineInput { rx: Some(rx) }
    }

    /// Chain a processing stage after a previous stage's output.
    /// Takes `input` by mutable ref, consuming its internal receiver.
    pub fn then<I: Send + 'static, O: Send + 'static, P: PipelineStage<Input = I, Output = O>>(
        &mut self,
        input: &mut PipelineInput<I>,
        stage: P,
        buffer: usize,
    ) -> PipelineInput<O> {
        let rx = input.take_rx().expect("then called twice on same input");
        let (tx, out_rx) = mpsc::channel(buffer);
        let cancel = self.cancel.child();
        let handle: JoinHandle<Result<PerStageMetrics>> = tokio::spawn(async move {
            let mut stage = stage;
            let mut metrics = PerStageMetrics::new(std::any::type_name::<P>());
            stage.init().await?;
            let mut rx = rx;
            while let Some(msg) = rx.recv().await {
                if cancel.is_cancelled() {
                    return Err(VeinError::cancelled());
                }
                let input = msg?;
                metrics.batches_in += 1;
                if let Some(output) = stage.process(input).await? {
                    metrics.batches_out += 1;
                    tx.send(Ok(output))
                        .await
                        .map_err(|_| VeinError::internal("stage channel closed"))?;
                }
            }
            while let Some(output) = stage.finish().await? {
                metrics.batches_out += 1;
                tx.send(Ok(output))
                    .await
                    .map_err(|_| VeinError::internal("stage finish send failed"))?;
            }
            Ok(metrics)
        });
        self.handles.push(handle);
        PipelineInput { rx: Some(out_rx) }
    }

    /// Attach a terminal sink stage. Consumes `input` and discards any stage output.
    pub fn sink<I: Send + 'static, Sink: PipelineStage<Input = I>>(
        &mut self,
        input: &mut PipelineInput<I>,
        stage: Sink,
    ) {
        let rx = input.take_rx().expect("sink called twice on same input");
        let cancel = self.cancel.child();
        let handle: JoinHandle<Result<PerStageMetrics>> = tokio::spawn(async move {
            let mut stage = stage;
            let mut metrics = PerStageMetrics::new(std::any::type_name::<Sink>());
            stage.init().await?;
            let mut rx = rx;
            while let Some(msg) = rx.recv().await {
                if cancel.is_cancelled() {
                    return Err(VeinError::cancelled());
                }
                let input = msg?;
                metrics.batches_in += 1;
                stage.process(input).await?;
            }
            stage.finish().await?;
            Ok(metrics)
        });
        self.handles.push(handle);
    }

    /// Run the pipeline to completion. Returns per-stage metrics.
    /// Returns `Err` on the first stage failure; remaining tasks are cancelled.
    pub async fn run(mut self) -> Result<StageMetrics> {
        let mut results = Vec::new();
        while let Some(handle) = self.handles.pop() {
            match handle.await {
                Ok(Ok(metrics)) => results.push(metrics),
                Ok(Err(e)) => {
                    self.cancel.cancel();
                    return Err(e);
                }
                Err(e) => {
                    self.cancel.cancel();
                    return Err(VeinError::internal(format!("task panicked: {e}")));
                }
            }
        }
        Ok(results)
    }
}

#[cfg(test)]
#[allow(missing_docs)]
mod tests {
    use super::*;

    struct MockSource {
        remaining: usize,
    }

    #[async_trait]
    impl SourceStage for MockSource {
        type Output = i32;

        async fn init(&mut self) -> Result<()> {
            Ok(())
        }

        async fn produce(&mut self) -> Result<Option<Self::Output>> {
            if self.remaining > 0 {
                self.remaining -= 1;
                Ok(Some(self.remaining as i32))
            } else {
                Ok(None)
            }
        }

        async fn finish(&mut self) -> Result<Option<Self::Output>> {
            Ok(None)
        }
    }

    struct Adder {
        amount: i32,
    }

    #[async_trait]
    impl PipelineStage for Adder {
        type Input = i32;
        type Output = i32;

        async fn init(&mut self) -> Result<()> {
            Ok(())
        }

        async fn process(&mut self, input: i32) -> Result<Option<Self::Output>> {
            Ok(Some(input + self.amount))
        }

        async fn finish(&mut self) -> Result<Option<Self::Output>> {
            Ok(None)
        }
    }

    struct Collector {
        results: Vec<i32>,
    }

    #[async_trait]
    impl PipelineStage for Collector {
        type Input = i32;
        type Output = ();

        async fn init(&mut self) -> Result<()> {
            Ok(())
        }

        async fn process(&mut self, input: i32) -> Result<Option<Self::Output>> {
            self.results.push(input);
            Ok(None)
        }

        async fn finish(&mut self) -> Result<Option<Self::Output>> {
            Ok(None)
        }
    }

    #[tokio::test]
    async fn test_source_to_sink() {
        let cancel = CancellationToken::new();
        let mut runner = PipelineRunner::new(cancel.clone());

        let source = MockSource { remaining: 5 };
        let mut input = runner.add_source(source, 16);

        let collector = Collector { results: Vec::new() };
        runner.sink(&mut input, collector);

        let metrics = runner.run().await.unwrap();
        assert!(metrics.len() >= 2);
    }

    #[tokio::test]
    async fn test_source_then_sink() {
        let cancel = CancellationToken::new();
        let mut runner = PipelineRunner::new(cancel.clone());

        let source = MockSource { remaining: 3 };
        let mut input = runner.add_source(source, 16);

        let adder = Adder { amount: 10 };
        let mut mid = runner.then(&mut input, adder, 16);

        let collector = Collector { results: Vec::new() };
        runner.sink(&mut mid, collector);

        let metrics = runner.run().await.unwrap();
        assert_eq!(metrics.len(), 3);
    }

    #[tokio::test]
    async fn test_cancellation_before_run() {
        let cancel = CancellationToken::new();
        cancel.cancel();
        let mut runner = PipelineRunner::new(cancel.clone());

        let source = MockSource { remaining: 100 };
        let mut input = runner.add_source(source, 16);
        let collector = Collector { results: Vec::new() };
        runner.sink(&mut input, collector);

        let result = runner.run().await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_cancellation_during_run() {
        let cancel = CancellationToken::new();
        let mut runner = PipelineRunner::new(cancel.clone());

        // A source that produces slow enough for cancellation to arrive
        struct SlowSource {
            remaining: usize,
        }
        #[async_trait]
        impl SourceStage for SlowSource {
            type Output = i32;
            async fn init(&mut self) -> Result<()> {
                Ok(())
            }
            async fn produce(&mut self) -> Result<Option<Self::Output>> {
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                if self.remaining > 0 {
                    self.remaining -= 1;
                    Ok(Some(0))
                } else {
                    Ok(None)
                }
            }
            async fn finish(&mut self) -> Result<Option<Self::Output>> {
                Ok(None)
            }
        }

        let source = SlowSource { remaining: 100 };
        let mut input = runner.add_source(source, 16);
        let collector = Collector { results: Vec::new() };
        runner.sink(&mut input, collector);

        let cancel2 = cancel.clone();
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            cancel2.cancel();
        });

        let result = runner.run().await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_backpressure() {
        let cancel = CancellationToken::new();
        let mut runner = PipelineRunner::new(cancel.clone());

        let source = MockSource { remaining: 50 };
        let mut input = runner.add_source(source, 2);

        let collector = Collector { results: Vec::new() };
        runner.sink(&mut input, collector);

        let result =
            tokio::time::timeout(std::time::Duration::from_secs(5), runner.run()).await;
        assert!(result.is_ok(), "pipeline should complete under backpressure");
    }
}
