//! Core streaming pipeline node traits.

use std::any::Any;

use super::DataChunkView;
use common::Result;
use error::VeinError;

/// Object-safe downcasting for runtime-created pipeline nodes.
///
/// Fan-in combines different concrete nodes through `dyn PipelineNode`; keeping
/// the downcast boundary here lets individual operators validate the matching
/// local-state type without exposing `Any` throughout the executor API.
pub trait PipelineNodeDowncast: Any + Send + Sync {
    /// Exposes this node for checked, internal type dispatch.
    fn as_any(&self) -> &dyn Any;
    /// Exposes this node mutably for checked state combination.
    fn as_any_mut(&mut self) -> &mut dyn Any;
    /// Consumes this node into its type-erased owner.
    fn into_any(self: Box<Self>) -> Box<dyn Any + Send + Sync>;
}

impl<T: Any + Send + Sync> PipelineNodeDowncast for T {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn into_any(self: Box<Self>) -> Box<dyn Any + Send + Sync> {
        self
    }
}

/// Batch-oriented operators with cooperative downstream delivery.
///
/// Every pipeline operator consumes and emits the engine's canonical
/// reference-backed view. CPU kernels remain synchronous; batch handoffs may
/// suspend on backpressure so producers and consumers can share one scheduler.
#[async_trait::async_trait]
pub trait PipelineNode: PipelineNodeDowncast + Send + Sync {
    /// Pushes a view of columnar data into the operator.
    async fn push(&mut self, view: DataChunkView) -> Result<()>;

    /// Signals that no more data will be pushed.
    async fn flush(&mut self) -> Result<()>;

    /// Whether this node has all the data it will ever need, so the source
    /// feeding it may stop reading.
    ///
    /// A limit is the only node that says yes today: once it has emitted its
    /// rows, everything the source would read next is discarded anyway, and at
    /// scan batch sizes that is the difference between reading one batch and
    /// reading the table. The default is `false` so a node that cannot finish
    /// early never has to say so.
    fn finished(&self) -> bool {
        false
    }

    /// Builds independent worker-local state when this operator can be safely
    /// partitioned. `None` is an explicit serial fallback for operators whose
    /// semantics or resources cannot currently be combined.
    fn fork_local(&self) -> Result<Option<Box<dyn PipelineNode>>> {
        Ok(None)
    }

    /// Combines a completed worker-local node into the canonical node.
    ///
    /// The local node is consumed so operators can transfer owned state and
    /// reservations instead of copying batch payloads. Implementations must
    /// validate that both nodes describe the same operation.
    fn combine_local(self: Box<Self>, _canonical: &mut dyn PipelineNode) -> Result<()> {
        Err(VeinError::internal(
            "pipeline node does not support local-state combination",
        ))
    }

}

/// The trait implemented by all data sources.
///
/// Sources are `Send`: a task's whole pipeline must be movable across threads
/// so independent tasks can be executed concurrently.
#[async_trait::async_trait]
pub trait Source: Send {
    /// Execute the source, pushing views into the next pipeline node.
    async fn execute(&mut self, next: &mut dyn PipelineNode) -> Result<()>;

    /// Number of bytes read from the underlying storage, for execution metrics.
    /// Sources that track this should override the default.
    fn bytes_read(&self) -> u64 {
        0
    }
}

/// A marker trait for middle operations that transform data and pass it on.
pub trait Transform: PipelineNode {
    /// Recreates independent evaluation state with a caller-owned output node.
    /// This lets nested operators retain their own collector without sharing it
    /// across workers. Unsupported transforms retain the serial fallback.
    fn fork_with_output(
        &self,
        _output: Box<dyn PipelineNode>,
    ) -> Result<Option<Box<dyn Transform>>> {
        Ok(None)
    }

    /// Builds a worker-local transform when its mutable execution state can be
    /// recreated independently and combined after its input is drained.
    fn fork_transform(&self) -> Result<Option<Box<dyn Transform>>> {
        Ok(None)
    }

    /// Combines a completed transform worker into its canonical counterpart.
    fn combine_transform(self: Box<Self>, _canonical: &mut dyn Transform) -> Result<()> {
        Err(VeinError::internal(
            "transform does not support local-state combination",
        ))
    }
}

/// A marker trait for termination nodes that consume data.
///
/// A sink is the terminal stage of a pipeline. A sink that must hand its result
/// back to the caller (e.g. `DataSink::Client`) overrides [`Sink::collect`] to
/// return the produced batches — analogous to Spark's `collect()`.
pub trait Sink: PipelineNode {
    /// Retrieve the sink's produced output, if any. Defaults to no output.
    fn collect(&mut self) -> Vec<DataChunkView> {
        Vec::new()
    }

    /// Number of rows consumed by this sink.
    fn row_count(&self) -> u64 {
        0
    }

    /// Receipt for a successfully committed storage write, if this is a write sink.
    fn write_receipt(&self) -> Option<&super::super::operators::sink::WriteReceipt> {
        None
    }
}
