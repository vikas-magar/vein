use common::Result;
use crate::traits::PhysicalPlanner;
use planner::plan::LogicalPlan;
use crate::planner::dag::StageDag;
use common::identifiers::StageId;
use common::protocol::ExecutionMode;

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
        Ok(StageDag::new(vec![], vec![], StageId::from_raw(0), ExecutionMode::Batch).unwrap())
    }
}
