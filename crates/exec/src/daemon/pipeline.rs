// Removed unused RecordBatch
use arrow::array::ArrayRef;
use common::Result;
use common::arrow::SelectionVector;
use common::identifiers::TaskId;
use common::protocol::{DataSink, DataSource, Operator};
use std::sync::Arc;

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

/// A group of vectors that share the same cardinality and selection mask.
#[derive(Debug, Clone)]
pub struct DataBlock {
    /// The columns in this block.
    pub vectors: Vec<ValueVector>,
    /// The selection vector indicating active rows.
    pub selection: SelectionVector,
}

/// A factorized chunk containing multiple independent data blocks.
#[derive(Debug, Clone)]
pub struct FactorizedChunk {
    /// Each DataBlock represents a "factor" in the join.
    pub blocks: Vec<DataBlock>,
}

/// Prepares the engine for Phase 3 (Factorization).
pub enum VeinBatch {
    /// A flat chunk of data.
    Flat(DataChunk),
    /// A factorized chunk of data (Epic 3).
    Factorized(FactorizedChunk),
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

/// A pipeline breaker acts as the ultimate `Sink` for a pipeline.
/// It absorbs `VeinBatch`es (e.g., building a hash table in memory) but does not
/// immediately push them to the next operator.
pub trait PipelineBreaker: PipelineNode {
    /// Called when all morsels for the pipeline have been completely processed.
    /// This signals the breaker to finalize its internal state (e.g., merge hash tables)
    /// so it can act as a Source for the *next* pipeline.
    fn finalize(&mut self) -> common::Result<()>;
}

/// Represents a single executable chain, bounded by a source and a pipeline breaker.
pub struct Pipeline {
    /// The source from which this pipeline reads data.
    pub source: Box<dyn Source>,
    /// The terminating node that consumes data.
    pub breaker: Box<dyn PipelineBreaker>,
}
