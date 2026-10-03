//! Inter-node protocol types, vectorized Micro-IR, and RPC definitions.

pub mod ir;
pub mod task;

// Re-export Micro-IR definitions
pub use ir::op::MicroOp;
pub use ir::program::{MicroProgram, RegId};
pub use ir::state::{AggOpCode, JoinKind, StateMode};

// The catalog owns table-identity types; the protocol carries them on the wire.
pub use catalog::CatalogSnapshotToken;

// Re-export task & pipeline definitions
pub use task::{
    AggregatePlan, DataSink, DataSource, ExecuteTask, ExecutionMode, FileFormat, FileScanTask,
    KeyValue, JoinOutput, JoinPlan, LimitSpec, ScanPredicate, SortKey, SortPlan, StorageTarget,
    StreamSource,
};
