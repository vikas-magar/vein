#![deny(missing_docs)]

//! Single-node physical planning from logical plans to executable tasks.

/// The physical plan builder subsystem.
pub mod builder;

/// Typed scalar-expression and physical-plan lowering.
pub mod compiler;

/// Pipeline boundaries and dependencies derived from the canonical plan tree.
pub mod pipeline_graph {
    pub use common::protocol::pipeline_graph::*;
}

/// A single-stage physical plan.
pub mod job;

/// The [`PhysicalPlanner`] abstraction and its default implementation.
pub mod planner;

/// A group of parallel tasks in one local query plan.
pub mod stage;

/// The atomic, executor-bound unit of work.
pub mod task;

pub use job::Job;
pub use planner::{DefaultPhysicalPlanner, PhysicalPlanner};
pub use pipeline_graph::{
    DerivedPipeline, DerivedPipelineGraph, PipelineBoundary, PipelineBoundaryKind,
    PipelineDependency, PipelineDependencyKind, PipelineId,
};
pub use stage::Stage;
pub use task::Task;
