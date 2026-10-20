//! Typed physical plan, task, and expression protocol definitions.

pub mod expression;
pub mod operators;
pub mod pipeline_graph;
pub mod task;
mod validation;

// Re-export operator semantics used throughout physical planning and execution.
pub use operators::{AggOpCode, JoinKind, StateMode};
pub use expression::{
    BinaryExprOp, ExprKind, ExpressionProgram, ExpressionStep, PhysicalExpr, UnaryExprOp,
};

// The catalog owns table-identity types; the protocol carries them on the wire.
pub use catalog::CatalogSnapshotToken;

// Re-export task & pipeline definitions
pub use task::{
    AggregatePlan, DataSink, DataSource, ExecuteTask, ExecutionMode, FileFormat, FileScanTask,
    KeyValue, JoinOutput, JoinPlan, JoinResidualPlan, LimitSpec, PhysicalPlan,
    InputMaterialization, OperatorDataFlow, OutputRepresentation,
    PhysicalPlanNode, PhysicalPlanOperator, ScanPredicate, ScanPredicateStage, SortKey, SortPlan,
    RuntimeStatisticsScan, StorageTarget, StreamSource,
};

// Derived scheduling metadata references nodes in the canonical physical plan.
pub use pipeline_graph::{
    DerivedPipeline, DerivedPipelineGraph, PipelineBoundary, PipelineBoundaryKind,
    PipelineDependency, PipelineDependencyKind, PipelineId,
};
