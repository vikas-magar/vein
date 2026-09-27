//! Core streaming pipeline node traits.

use arrow::record_batch::RecordBatch;
use common::Result;

/// The synchronous trait implemented by all CPU-bound operators (Transforms
/// and Sinks).
///
/// Generic over the batch unit `B` so a pipeline can process any columnar
/// representation (e.g. [`DataChunk`](super::DataChunk) today). Implementations must NEVER yield
/// to an asynchronous scheduler; they process the batch and immediately return
/// or push to the next operator.
pub trait PipelineNode<B>: Send + Sync {
    /// Pushes a chunk of data into the operator.
    fn push(&mut self, batch: B) -> Result<()>;

    /// Signals that no more data will be pushed.
    fn flush(&mut self) -> Result<()>;
}

/// The trait implemented by all data sources.
///
/// Generic over the batch unit `B` it produces. Sources read data and push it
/// into the execution pipeline.
#[async_trait::async_trait(?Send)]
pub trait Source<B> {
    /// Execute the source, pushing all batches into the `next` pipeline node.
    async fn execute(&mut self, next: &mut dyn PipelineNode<B>) -> Result<()>;

    /// Number of bytes read from the underlying storage, for execution metrics.
    /// Sources that track this should override the default.
    fn bytes_read(&self) -> u64 {
        0
    }
}

/// A marker trait for middle operations that transform data and pass it on.
pub trait Transform<B>: PipelineNode<B> {}

/// A marker trait for termination nodes that consume data.
///
/// A sink is the terminal stage of a pipeline. A sink that must hand its result
/// back to the caller (e.g. `DataSink::Client`) overrides [`Sink::collect`] to
/// return the produced batches — analogous to Spark's `collect()`.
pub trait Sink<B>: PipelineNode<B> {
    /// Retrieve the sink's produced output, if any. Defaults to no output.
    fn collect(&mut self) -> Vec<RecordBatch> {
        Vec::new()
    }

    /// Number of rows consumed by this sink.
    fn row_count(&self) -> u64 {
        0
    }

}
