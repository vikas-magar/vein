#![deny(missing_docs)]

//! The physical execution model of the Vein engine.
//!
//! This crate owns the distributed physical plan produced by lowering a bound
//! [`plan::LogicalPlan`]: a hierarchy of [`Job`]s, [`Stage`]s, and [`Task`]s.
//!
//! - A [`Job`] is one query's full physical plan: the DAG of stages that must
//!   be scheduled to answer it.
//! - A [`Stage`] is a set of parallel [`Task`]s that execute together between
//!   shuffle barriers.
//! - A [`Task`] is the atomic unit of work dispatched to a single executor.
//!
//! The [`PhysicalPlanner`] trait is the seam between the logical optimizer and
//! this model: it lowers an optimized [`plan::LogicalPlan`] into a [`Job`].

/// The physical plan builder subsystem.
pub mod builder;

/// The job graph: stages and the shuffle edges between them.
pub mod job;

/// The [`PhysicalPlanner`] abstraction and its default implementation.
pub mod planner;

/// A group of parallel tasks between shuffle barriers.
pub mod stage;

/// The atomic, executor-bound unit of work.
pub mod task;

pub use job::{Job, StageEdge};
pub use planner::{DagPhysicalPlanner, PhysicalPlanner};
pub use stage::Stage;
pub use task::Task;
