//! The [`PhysicalPlanner`] abstraction and its default implementation.

use crate::job::Job;
use common::Result;
use plan::LogicalPlan;

/// Abstraction for the physical planner.
///
/// Responsible for lowering an optimized [`LogicalPlan`] into a distributed
/// physical [`Job`] of stages and tasks that the scheduler can execute.
pub trait PhysicalPlanner: Send + Sync {
    /// Lower a logical plan into a physical [`Job`].
    fn create_physical_plan(&self, logical_plan: LogicalPlan) -> Result<Job>;
}

/// The default physical planner.
///
/// Translation from [`LogicalPlan`] to [`Job`] is not yet implemented; this is
/// the seam the stage/task builder will fill in.
pub struct DagPhysicalPlanner;

impl DagPhysicalPlanner {
    /// Create a new [`DagPhysicalPlanner`].
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for DagPhysicalPlanner {
    fn default() -> Self {
        Self::new()
    }
}

impl PhysicalPlanner for DagPhysicalPlanner {
    fn create_physical_plan(&self, _logical_plan: LogicalPlan) -> Result<Job> {
        Err(error::VeinError::internal(
            "DagPhysicalPlanner::create_physical_plan is not yet implemented",
        ))
    }
}
