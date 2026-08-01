use async_trait::async_trait;
use error::Result;
use crate::identifiers::OperatorId;
use super::chunk::Chunk;
use crate::identifiers::EpochId;
use super::stream::StreamProducer;

/// Runtime metadata about an operator.
#[derive(Debug, Default, Clone)]
pub struct OperatorMeta {
    /// Total rows received as input across all epochs.
    pub rows_in: u64,
    /// Total rows emitted as output across all epochs.
    pub rows_out: u64,
    /// Number of epochs processed.
    pub epochs_processed: u64,
    /// Total wall-clock time in eval() calls (nanoseconds).
    pub eval_ns: u64,
}

// ── Base trait (all operators) ──

/// Base trait for all DBSP operators.
#[async_trait]
pub trait DbspOperator: Send + 'static {
    /// Human-readable operator name for debugging and metrics.
    fn name(&self) -> &'static str;

    /// Unique identifier for this operator instance.
    fn operator_id(&self) -> OperatorId;

    /// Initialize the operator. Called once when the circuit starts.
    async fn init(&mut self) -> Result<()>;

    /// Called at the START of each epoch (before any eval calls).
    async fn clock_start(&mut self, _epoch: EpochId) -> Result<()> {
        Ok(())
    }

    /// Called at the END of each epoch (after all eval calls).
    async fn clock_end(&mut self, _epoch: EpochId) -> Result<()> {
        Ok(())
    }

    /// Collect runtime metadata about this operator.
    fn metadata(&self) -> OperatorMeta {
        OperatorMeta::default()
    }
}

// ── Concrete operator shapes ──

/// Source operator: produces data without consuming input.
#[async_trait]
pub trait SourceOperator: DbspOperator {
    /// Produce the next epoch's output. Called once per epoch.
    async fn eval(&mut self) -> Result<Chunk>;
}

/// Unary operator: one input stream, one output stream.
#[async_trait]
pub trait UnaryOperator: DbspOperator {
    /// Process one epoch's input diff and produce the output diff.
    async fn eval(&mut self, input: &Chunk) -> Result<Chunk>;
}

/// Binary operator: two input streams, one output stream.
#[async_trait]
pub trait BinaryOperator: DbspOperator {
    /// Process one epoch's input from both sides and produce the output diff.
    async fn eval(&mut self, lhs: &Chunk, rhs: &Chunk) -> Result<Chunk>;
}

/// Sink operator: consumes input without producing output.
#[async_trait]
pub trait SinkOperator: DbspOperator {
    /// Consume one epoch's output diff.
    async fn eval(&mut self, input: &Chunk) -> Result<()>;
}

/// Strict operator: output depends only on PREVIOUS epochs, not current input.
/// Used for feedback loops (z⁻¹ delay line).
#[async_trait]
pub trait StrictOperator: DbspOperator {
    /// Produce the output BEFORE receiving input for the current epoch.
    async fn eval_strict(&mut self) -> Result<Chunk>;
}

// ── OperatorNode (typed dispatch enum) ──

/// A node in the circuit DAG. Wraps any operator shape with its stream handles.
pub enum OperatorNode {
    /// Source: produces data without consuming input.
    Source {
        /// The source operator implementation.
        op: Box<dyn SourceOperator>,
        /// Output stream to downstream operators.
        output: StreamProducer,
    },
    /// Unary: produces one output stream.
    Unary {
        /// The unary operator implementation.
        op: Box<dyn UnaryOperator>,
        /// Output stream to downstream operators.
        output: StreamProducer,
    },
    /// Binary: produces one output stream.
    Binary {
        /// The binary operator implementation.
        op: Box<dyn BinaryOperator>,
        /// Output stream to downstream operators.
        output: StreamProducer,
    },
    /// Sink: consumes data without producing output.
    Sink {
        /// The sink operator implementation.
        op: Box<dyn SinkOperator>,
    },
    /// Strict: feedback operator whose output depends only on previous epochs.
    Strict {
        /// The strict operator implementation.
        op: Box<dyn StrictOperator>,
        /// Output stream to downstream operators.
        output: StreamProducer,
    },
}

impl OperatorNode {
    /// Return the operator name.
    pub fn name(&self) -> &'static str {
        match self {
            Self::Source { op, .. } => op.name(),
            Self::Unary { op, .. } => op.name(),
            Self::Binary { op, .. } => op.name(),
            Self::Sink { op, .. } => op.name(),
            Self::Strict { op, .. } => op.name(),
        }
    }

    /// Return the operator ID.
    pub fn operator_id(&self) -> OperatorId {
        match self {
            Self::Source { op, .. } => op.operator_id(),
            Self::Unary { op, .. } => op.operator_id(),
            Self::Binary { op, .. } => op.operator_id(),
            Self::Sink { op, .. } => op.operator_id(),
            Self::Strict { op, .. } => op.operator_id(),
        }
    }

    /// Call init on the inner operator.
    pub async fn init(&mut self) -> Result<()> {
        match self {
            Self::Source { op, .. } => op.init().await,
            Self::Unary { op, .. } => op.init().await,
            Self::Binary { op, .. } => op.init().await,
            Self::Sink { op, .. } => op.init().await,
            Self::Strict { op, .. } => op.init().await,
        }
    }

    /// Call clock_start on the inner operator.
    pub async fn clock_start(&mut self, epoch: EpochId) -> Result<()> {
        match self {
            Self::Source { op, .. } => op.clock_start(epoch).await,
            Self::Unary { op, .. } => op.clock_start(epoch).await,
            Self::Binary { op, .. } => op.clock_start(epoch).await,
            Self::Sink { op, .. } => op.clock_start(epoch).await,
            Self::Strict { op, .. } => op.clock_start(epoch).await,
        }
    }

    /// Call clock_end on the inner operator.
    pub async fn clock_end(&mut self, epoch: EpochId) -> Result<()> {
        match self {
            Self::Source { op, .. } => op.clock_end(epoch).await,
            Self::Unary { op, .. } => op.clock_end(epoch).await,
            Self::Binary { op, .. } => op.clock_end(epoch).await,
            Self::Sink { op, .. } => op.clock_end(epoch).await,
            Self::Strict { op, .. } => op.clock_end(epoch).await,
        }
    }
}
