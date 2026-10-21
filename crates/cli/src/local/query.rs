use std::pin::Pin;
use std::sync::Arc;

use common::cancellation::CancellationToken;
use common::identifiers::{QueryId, RequestId};
use common::protocol::ExecuteTask;
use common::statistics::QueryStatisticsContext;
use common::{Result, VeinError};
use exec::operators::sink::WriteReceipt;
use exec::pipeline::DataChunkView;
use exec::runtime::TaskExecutor;
use futures::Stream;
use plan::LogicalPlan;

use super::adaptive::{AdaptiveBoundary, AdaptiveCheckpointPolicy};

/// A planned local query whose output can be consumed incrementally.
pub struct LocalQuery {
    pub(in crate::local) tasks: Vec<ExecuteTask>,
    pub(in crate::local) executor: Arc<dyn TaskExecutor>,
    pub(in crate::local) statistics_context: Arc<QueryStatisticsContext>,
    pub(in crate::local) logical_plan: Arc<LogicalPlan>,
    pub(in crate::local) is_write: bool,
    pub(in crate::local) adaptive_checkpoint: Option<PreparedAdaptiveCheckpoint>,
    pub(in crate::local) adaptive_policy: Arc<dyn AdaptiveCheckpointPolicy>,
    pub(in crate::local) optimizer: Arc<optimizer::OptimizerPipeline>,
    pub(in crate::local) planner: Arc<physical::DefaultPhysicalPlanner>,
    pub(in crate::local) request_id: RequestId,
    pub(in crate::local) query_id: QueryId,
}

pub(in crate::local) struct PreparedAdaptiveCheckpoint {
    pub(in crate::local) boundary: AdaptiveBoundary,
    pub(in crate::local) producer_task: ExecuteTask,
}

impl LocalQuery {
    /// Returns the optimized logical plan retained for adaptive continuations.
    pub fn logical_plan(&self) -> &Arc<LogicalPlan> {
        &self.logical_plan
    }

    /// Returns the shared query statistics context used by planning and execution.
    pub fn statistics_context(&self) -> &Arc<QueryStatisticsContext> {
        &self.statistics_context
    }

    /// Whether this prepared query commits data to a storage target.
    pub fn is_write(&self) -> bool {
        self.is_write
    }

    /// Execute a storage write and return only after its commit is confirmed.
    pub async fn execute_write(self) -> Result<WriteReceipt> {
        if !self.is_write || self.tasks.len() != 1 {
            return Err(VeinError::planning(
                "prepared query is not a single-task WRITE",
                None,
            ));
        }
        let result = self
            .executor
            .execute(&self.tasks[0], CancellationToken::new())
            .await?;
        result
            .write_receipt
            .ok_or_else(|| VeinError::internal("WRITE task completed without a commit receipt"))
    }
}

pub type LocalResultStream = Pin<Box<dyn Stream<Item = Result<DataChunkView>> + Send>>;
