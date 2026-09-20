//! The physical planner abstraction and its default distributed implementation.

pub mod default;

pub use default::DefaultPhysicalPlanner;

use crate::job::Job;
use common::identifiers::RequestId;
use common::Result;
use plan::LogicalPlan;

/// Abstraction for the physical planner.
///
/// Responsible for lowering an optimized [`LogicalPlan`] into a distributed
/// physical [`Job`] of stages and tasks that the cluster scheduler can execute.
pub trait PhysicalPlanner: Send + Sync {
    /// Lower a logical plan into a physical [`Job`].
    fn create_physical_plan(&self, request_id: RequestId, logical_plan: LogicalPlan) -> Result<Job>;
}
