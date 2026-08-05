// Removed unused RecordBatch
use common::identifiers::TaskId;
use std::sync::Arc;
use common::protocol::{DataSource, DataSink, Operator};
use common::Result;
use arrow::array::ArrayRef;
use common::arrow::SelectionVector;

/// A physical column of data (wraps Arrow Arrays for now).
#[derive(Debug, Clone)]
pub struct ValueVector {
    /// The underlying array data.
    pub data: ArrayRef,
}

/// A horizontal slice of data processed in a pipeline.
#[derive(Debug, Clone)]
pub struct DataChunk {
    /// The columns in this chunk.
    pub vectors: Vec<ValueVector>,
    /// The selection vector indicating active rows.
    pub selection: SelectionVector,
}

/// Prepares the engine for Phase 3 (Factorization).
pub enum VeinBatch {
    /// A flat chunk of data.
    Flat(DataChunk),
    /// A factorized chunk of data (Epic 3).
    Factorized(()), // Placeholder for Epic 3
}

/// A chunk of work dynamically routed to an idle LocalSet.
pub struct MorselTask {
    /// Task identifier.
    pub task_id: TaskId,
    /// Upstream source.
    pub source: DataSource,
    /// Physical execution plan.
    pub plan: Arc<Vec<Operator>>,
    /// Downstream sink.
    pub sink: DataSink,
}

/// The synchronous trait implemented by all CPU-bound operators (Transforms and Sinks).
///
/// Implementations must NEVER yield to an asynchronous scheduler.
/// They must process the batch and immediately return or push to the next operator.
pub trait PipelineNode: Send + Sync {
    /// Pushes a chunk of data into the operator.
    fn push(&mut self, batch: VeinBatch) -> Result<()>;
    
    /// Signals that no more data will be pushed.
    fn flush(&mut self) -> Result<()>;
}

/// The trait implemented by all data sources.
/// Sources read data and push it into the execution pipeline.
#[async_trait::async_trait(?Send)]
pub trait Source {
    /// Execute the source, pushing all batches into the `next` pipeline node.
    async fn execute(&mut self, next: &mut dyn PipelineNode) -> Result<()>;
}

/// A marker trait for middle operations that transform data and pass it on.
pub trait Transform: PipelineNode {}

/// A marker trait for termination nodes that consume data.
pub trait Sink: PipelineNode {}
