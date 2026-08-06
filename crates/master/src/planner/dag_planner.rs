use common::Result;
use planner::traits::PhysicalPlanner;

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
    fn create_physical_plan(&self, logical_plan: String) -> Result<String> {
        // TODO: Implement translation to StageDag
        Ok(format!("PhysicalDag({})", logical_plan))
    }
}
