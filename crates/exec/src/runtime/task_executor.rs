//! Asynchronous execution contract for one physical task.

use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use common::Result;
use common::cancellation::CancellationToken;
use common::identifiers::{IntermediateSourceId, QueryId};
use common::protocol::ExecuteTask;
use error::VeinError;

use crate::operators::sink::ViewResultBatch;
use super::checkpoint;
use super::{QueryExecutionResources, TaskResult};

/// Executes a single task (`source -> transforms -> sink`) end-to-end.
///
/// This is the unit of work the distributed master schedules: the executor
/// receives a self-contained [`ExecuteTask`] and runs it, returning its result.
///
/// Every future here is `Send`, so tasks can be dispatched onto a multi-threaded
/// runtime and executed concurrently; nothing in the pipeline holds `!Send`
/// state.
#[async_trait]
pub trait TaskExecutor: Send + Sync {
    /// Create one resource scope for a multi-stage query execution.
    ///
    /// Executors that do not support query-wide accounting return an error;
    /// callers can then keep the original single-stage plan.
    fn create_query_resources(
        &self,
        _cancel: CancellationToken,
    ) -> Result<Arc<QueryExecutionResources>> {
        Err(VeinError::unsupported(
            "executor does not support query-scoped resources",
            None,
        ))
    }

    /// Run one task, driving its source through the operator chain.
    async fn execute(&self, task: &ExecuteTask, cancel: CancellationToken) -> Result<TaskResult>;

    /// Executes the same canonical plan while retaining client result views.
    async fn execute_views(
        &self,
        task: &ExecuteTask,
        cancel: CancellationToken,
    ) -> Result<TaskResult<ViewResultBatch>>;

    /// Runs a client-result task into a query-accounted, replayable checkpoint.
    ///
    /// This is the producer-side seam for multi-stage adaptive execution. The
    /// checkpoint retains `DataChunkView`s while memory permits and spills via
    /// the same tracker used by the task.
    async fn execute_to_checkpoint(
        &self,
        task: &ExecuteTask,
        cancel: CancellationToken,
    ) -> Result<checkpoint::CheckpointTaskResult>;

    /// Run a checkpoint producer with the query's shared memory/spill budget.
    async fn execute_to_checkpoint_with_resources(
        &self,
        task: &ExecuteTask,
        cancel: CancellationToken,
        _resources: Arc<QueryExecutionResources>,
    ) -> Result<checkpoint::CheckpointTaskResult> {
        self.execute_to_checkpoint(task, cancel).await
    }

    /// Make a sealed query-local relation available to a later physical fragment.
    ///
    /// The returned guard owns the registration and removes it when dropped.
    /// Custom executors may leave this unsupported until they implement runtime
    /// source resolution.
    fn register_intermediate_source(
        &self,
        _query_id: QueryId,
        _source_id: IntermediateSourceId,
        _relation: checkpoint::IntermediateRelation,
    ) -> Result<checkpoint::IntermediateSourceRegistration> {
        Err(VeinError::unsupported(
            "executor does not support query-local intermediate sources",
            None,
        ))
    }

    /// Streams reference-backed results through a bounded channel.
    async fn execute_views_streaming(
        &self,
        task: &ExecuteTask,
        cancel: CancellationToken,
        output: flume::Sender<ViewResultBatch>,
    ) -> Result<TaskResult<ViewResultBatch>>;

    /// Stream a continuation using the same resource budget as earlier stages.
    async fn execute_views_streaming_with_resources(
        &self,
        task: &ExecuteTask,
        cancel: CancellationToken,
        output: flume::Sender<ViewResultBatch>,
        _resources: Arc<QueryExecutionResources>,
    ) -> Result<TaskResult<ViewResultBatch>> {
        self.execute_views_streaming(task, cancel, output).await
    }

    /// Stream client batches using the query-wide memory and spill budget.
    async fn execute_streaming_with_resources(
        &self,
        task: &ExecuteTask,
        cancel: CancellationToken,
        output: flume::Sender<RecordBatch>,
        _resources: Arc<QueryExecutionResources>,
    ) -> Result<TaskResult> {
        self.execute_streaming(task, cancel, output).await
    }

    /// Execute a task while forwarding client batches through a bounded channel.
    ///
    /// The default adapter preserves compatibility for custom executors; the
    /// default executor overrides this to avoid retaining the complete output.
    async fn execute_streaming(
        &self,
        task: &ExecuteTask,
        cancel: CancellationToken,
        output: flume::Sender<RecordBatch>,
    ) -> Result<TaskResult> {
        let mut result = self.execute(task, cancel).await?;
        for batch in result.output.drain(..) {
            output
                .send_async(batch)
                .await
                .map_err(|e| VeinError::internal(format!("task output channel closed: {e}")))?;
        }
        Ok(result)
    }
}
