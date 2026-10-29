use std::pin::Pin;
use std::sync::Arc;

use common::identifiers::{QueryId, RequestId};
use common::protocol::ExecuteTask;
use common::statistics::QueryStatisticsContext;
use common::{Result, VeinError};
use exec::operators::sink::WriteReceipt;
use exec::pipeline::DataChunkView;
use exec::runtime::{QueryExecutionGuard, QueryExecutionResources, TaskExecutor};
use futures::Stream;
use plan::LogicalPlan;

use super::adaptive::{AdaptiveBoundary, AdaptiveCheckpointPolicy};

mod context;
mod execution;
mod write;
pub use context::query_statistics_context;

#[cfg(test)]
mod tests;

/// A planned local query whose output can be consumed incrementally.
pub struct LocalQuery {
    pub(in crate::local) tasks: Vec<ExecuteTask>,
    pub(in crate::local) executor: Arc<dyn TaskExecutor>,
    pub(in crate::local) scope: QueryExecutionGuard,
    pub(in crate::local) logical_plan: Arc<LogicalPlan>,
    pub(in crate::local) is_write: bool,
    pub(in crate::local) adaptive_checkpoint: Option<PreparedAdaptiveCheckpoint>,
    pub(in crate::local) adaptive_policy: Arc<dyn AdaptiveCheckpointPolicy>,
    pub(in crate::local) optimizer: Arc<optimizer::OptimizerPipeline>,
    pub(in crate::local) planner: Arc<dyn physical::PhysicalPlanner>,
}

pub(in crate::local) struct PreparedAdaptiveCheckpoint {
    pub(in crate::local) boundary: AdaptiveBoundary,
    pub(in crate::local) producer_task: ExecuteTask,
}

impl LocalQuery {
    /// Identifier shared by preparation, physical tasks, and completion logs.
    pub fn request_id(&self) -> RequestId {
        self.scope.resources().request_id()
    }

    /// Identifier shared by all physical stages of this query.
    pub fn query_id(&self) -> QueryId {
        self.scope.resources().query_id()
    }

    /// Returns the optimized logical plan retained for adaptive continuations.
    pub fn logical_plan(&self) -> &Arc<LogicalPlan> {
        &self.logical_plan
    }

    /// Returns the shared query statistics context used by planning and execution.
    pub fn statistics_context(&self) -> &Arc<QueryStatisticsContext> {
        self.scope.resources().statistics_context()
    }

    /// Shared identity, cancellation, memory, spill and statistics authority.
    pub fn execution_resources(&self) -> &Arc<QueryExecutionResources> {
        self.scope.resources()
    }

    /// Whether this prepared query commits data to a storage target.
    pub fn is_write(&self) -> bool {
        self.is_write
    }

    /// Whether runtime feedback selected a checkpoint before execution.
    pub fn has_adaptive_checkpoint(&self) -> bool {
        self.adaptive_checkpoint.is_some()
    }

    /// Number of physical tasks prepared for this query stage.
    pub fn task_count(&self) -> usize {
        self.tasks.len()
    }

    /// Physical plan for one prepared task, for diagnostics and explain tooling.
    pub fn physical_plan(&self, index: usize) -> Option<&common::protocol::PhysicalPlan> {
        self.tasks.get(index).map(|task| &task.plan)
    }

    /// Query statistics attached to a prepared task, when task metadata exists.
    pub fn task_statistics_context(&self, index: usize) -> Option<&Arc<QueryStatisticsContext>> {
        self.tasks.get(index)?.statistics_context.as_ref()
    }
}

/// A bounded, incremental stream of reference-backed query result batches.
pub type LocalResultStream = Pin<Box<dyn Stream<Item=Result<DataChunkView>> + Send>>;
