/// Columnar Z-set type (RecordBatch with embedded Int64 weight column).
pub mod chunk;
/// Epoch types, epoch barriers, and multi-input barrier alignment.
pub mod epoch;
/// Streaming channel between operators (Stream, StreamProducer, StreamConsumer).
pub mod stream;
/// Columnar hash index for z⁻¹ state storage.
pub mod arrow_hash_index;
/// DBSP circuit: DAG of operators, epoch execution loop.
pub mod circuit;
/// DBSP operator traits and OperatorNode enum.
pub mod r#operator;
/// I (integrate) / D (differentiate) wrappers for incremental computation.
pub mod incremental;
/// Three-tier state registry and ZSetState trait.
pub mod state_registry;
/// DSL for constructing DBSP circuits.
pub mod builder;
/// Built-in DBSP operators (Filter, Project, Map, Z1, Negate, Distinct, HashJoin, HashAgg).
pub mod operators;
/// Aggregate operators (Count, Sum, Min, Max, Avg).
pub mod aggregates;

pub use chunk::Chunk;
pub use epoch::{BarrierAligner, EpochBarrier, StreamPayload};
pub use stream::{Stream, StreamConsumer, StreamProducer};
pub use incremental::{IncrementalOperator, IncrementalPipelineStage};
pub use r#operator::{
    BinaryOperator, DbspOperator, OperatorMeta, OperatorNode, SinkOperator, SourceOperator,
    StrictOperator, UnaryOperator,
};
