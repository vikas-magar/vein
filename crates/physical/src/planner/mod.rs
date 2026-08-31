use crate::builder::{PhysicalPlanBuilder, PlanBuilder};
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
#[derive(Debug, Default, Clone)]
pub struct DagPhysicalPlanner;

impl DagPhysicalPlanner {
    /// Create a new [`DagPhysicalPlanner`].
    pub fn new() -> Self {
        Self
    }
}

impl PhysicalPlanner for DagPhysicalPlanner {
    fn create_physical_plan(&self, logical_plan: LogicalPlan) -> Result<Job> {
        PhysicalPlanBuilder::new().build(logical_plan)
    }
}
