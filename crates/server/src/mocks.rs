use common::Result;
use async_trait::async_trait;
use common::protocol::MorselTask;
use master::traits::{TaskDispatcher, JoinOptimizer};
use planner::traits::{LogicalBinder, PhysicalPlanner};

pub struct MockBinder;
impl LogicalBinder for MockBinder {
    fn bind(&self, _ast: String) -> Result<String> {
        Ok("MockLogicalPlan".into())
    }
}

pub struct MockOptimizer;
impl JoinOptimizer for MockOptimizer {
    fn optimize_join_order(&self, _logical_plan: String) -> Result<String> {
        Ok("MockOptimizedPlan".into())
    }
}

pub struct MockPlanner;
impl PhysicalPlanner for MockPlanner {
    fn create_physical_plan(&self, _logical_plan: String) -> Result<String> {
        Ok("MockStageDag".into())
    }
}

pub struct MockDispatcher;
#[async_trait]
impl TaskDispatcher for MockDispatcher {
    async fn dispatch(&self, node_id: u32, _task: MorselTask) -> Result<()> {
        tracing::info!("Mock dispatching task to node {}", node_id);
        Ok(())
    }
}
