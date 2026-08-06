use common::Result;
use async_trait::async_trait;
use common::protocol::MorselTask;
use planner::plan::LogicalPlan;
use crate::planner::dag::StageDag;

/// Abstraction for the Physical Planner.
/// Responsible for lowering a bound Logical Plan into a distributed physical Execution DAG.
pub trait PhysicalPlanner: Send + Sync {
    /// Takes a Logical Plan and generates a distributed StageDag of MorselTasks.
    fn create_physical_plan(&self, logical_plan: LogicalPlan) -> Result<StageDag>;
}

use common::identifiers::NodeId;

/// Highly abstract Task Dispatcher.
/// Allows swapping our QUIC network layer with TCP, gRPC, or an in-memory mock for local testing.
#[async_trait]
pub trait TaskDispatcher: Send + Sync {
    /// Dispatch a compiled physical Morsel to a specific Executor node over the network.
    async fn dispatch(&self, node_id: NodeId, task: MorselTask) -> Result<()>;
}

