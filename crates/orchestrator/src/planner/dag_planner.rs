use crate::planner::dag::StageDag;
use crate::traits::PhysicalPlanner;
use common::Result;
use plan::LogicalPlan;

/// The DAG Physical Planner.
/// Responsible for lowering the optimized logical plan into a highly parallel `StageDag`
/// of physical execution tasks.
pub struct DagPhysicalPlanner;

impl DagPhysicalPlanner {
    /// Creates a new DagPhysicalPlanner instance.
    pub fn new() -> Self {
        Self {}
    }
}

impl PhysicalPlanner for DagPhysicalPlanner {
    fn create_physical_plan(&self, _logical_plan: LogicalPlan) -> Result<StageDag> {
        // TODO: Implement translation from LogicalPlan to StageDag
        Err(error::VeinError::internal(
            "DagPhysicalPlanner::create_physical_plan is not yet implemented",
        ))
    }
}
