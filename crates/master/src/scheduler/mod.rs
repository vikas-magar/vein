pub mod dispatcher;

use crate::executor::ExecutorRegistry;
use crate::planner::dag::StageDag;
use common::identifiers::{QueryId, TaskId};
use common::protocol::{ExecuteTask, ExecutionMode};
use dispatcher::TaskDispatcher;
use error::VeinError;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinSet;
use common::cancellation::CancellationToken;

/// Orchestrates the execution of a StageDag across the cluster.
pub struct Scheduler {
    registry: Arc<ExecutorRegistry>,
    dispatcher: Arc<dyn TaskDispatcher>,
    max_retries: u32,
    timeout: Duration,
}

impl Scheduler {
    /// Create a new Scheduler instance.
    pub fn new(
        registry: Arc<ExecutorRegistry>,
        dispatcher: Arc<dyn TaskDispatcher>,
        max_retries: u32,
        timeout: Duration,
    ) -> Self {
        Self {
            registry,
            dispatcher,
            max_retries,
            timeout,
        }
    }

    /// Execute the StageDag, choosing the strategy based on ExecutionMode.
    pub async fn execute(
        &self,
        query_id: QueryId,
        dag: StageDag,
        cancel: CancellationToken,
    ) -> Result<(), VeinError> {
        match dag.mode() {
            ExecutionMode::Batch => self.execute_batch_waves(query_id, dag, cancel).await,
            ExecutionMode::Streaming => self.execute_streaming_topology(query_id, dag, cancel).await,
        }
    }

    /// Schedule a Batch query using wave (bottom-up) execution.
    /// It traverses the DAG topologically, waiting for each wave to complete
    /// before starting the next wave.
    async fn execute_batch_waves(
        &self,
        query_id: QueryId,
        dag: StageDag,
        cancel: CancellationToken,
    ) -> Result<(), VeinError> {
        let order = dag.topological_order();

        for stage_id in order {
            // Check cancellation before each wave
            if cancel.is_cancelled() {
                return Err(VeinError::Cancelled);
            }

            let stage = dag
                .get_stage(stage_id)
                .ok_or_else(|| VeinError::internal("Invalid StageId in topological order"))?;

            // Get available executors (with 15s heartbeat timeout)
            let executors = self.registry.available(Duration::from_secs(15)).await;
            if executors.is_empty() {
                return Err(VeinError::NoExecutorsAvailable);
            }

            // In a real query, a stage might be split into multiple parallel tasks (e.g. 10 files = 10 tasks).
            // For scaffolding, we dispatch 1 task representing the entire stage.
            let task = ExecuteTask {
                mode: ExecutionMode::Batch,
                task_id: TaskId::new(),
                query_id,
                stage_id: stage.stage_id,
                source: stage.source.clone(),
                operators: stage.operators.clone(),
                sink: stage.sink.clone(),
            };

            // Pick the least-loaded executor (executors are already sorted by load)
            let executor = executors[0].clone();

            let mut attempts = 0;
            loop {
                // 1. Dispatch
                if let Err(e) = self.dispatcher.dispatch(&executor, task.clone()).await {
                    tracing::warn!("Dispatch failed to node {}: {}", executor.node_id, e);
                    attempts += 1;
                    if attempts > self.max_retries {
                        return Err(e);
                    }
                    continue; // Retry dispatch
                }

                // 2. Wait for completion
                let dispatcher = self.dispatcher.clone();
                let tid = task.task_id;
                let c = cancel.clone();

                let wait_result = tokio::select! {
                    res = dispatcher.wait_for_completion(query_id, tid) => res,
                    _ = c.cancelled() => {
                        // Attempt graceful cancel
                        let _ = self.dispatcher.cancel_task(&executor, query_id, tid).await;
                        return Err(VeinError::Cancelled);
                    }
                    _ = tokio::time::sleep(self.timeout) => {
                        Err(VeinError::TaskFailed { task_id: tid.to_string(), message: "Task timeout".to_string() })
                    }
                };

                match wait_result {
                    Ok(_) => break, // Success, move to next stage in wave
                    Err(e) => {
                        tracing::warn!("Task {} failed on node {}: {}", tid, executor.node_id, e);
                        attempts += 1;
                        if attempts > self.max_retries {
                            return Err(e);
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Schedule a Streaming query by dispatching all stages simultaneously.
    async fn execute_streaming_topology(
        &self,
        query_id: QueryId,
        dag: StageDag,
        cancel: CancellationToken,
    ) -> Result<(), VeinError> {
        let order = dag.topological_order();

        let executors = self.registry.available(Duration::from_secs(15)).await;
        if executors.is_empty() {
            return Err(VeinError::NoExecutorsAvailable);
        }

        let mut join_set = JoinSet::new();

        for (i, stage_id) in order.into_iter().enumerate() {
            let stage = dag.get_stage(stage_id).unwrap();
            let executor = &executors[i % executors.len()]; // Round-robin assign

            let task = ExecuteTask {
                mode: ExecutionMode::Streaming,
                task_id: TaskId::new(),
                query_id,
                stage_id: stage.stage_id,
                source: stage.source.clone(),
                operators: stage.operators.clone(),
                sink: stage.sink.clone(),
            };

            // Dispatch task
            self.dispatcher.dispatch(executor, task.clone()).await?;

            // Spawn a monitoring task for this continuous fragment
            let dispatcher = self.dispatcher.clone();
            let tid = task.task_id;
            let c = cancel.clone();

            join_set.spawn(async move {
                tokio::select! {
                    res = dispatcher.wait_for_completion(query_id, tid) => res,
                    _ = c.cancelled() => Err(VeinError::Cancelled),
                }
            });
        }

        // Wait for all fragments to either fail or get canceled
        while let Some(res) = join_set.join_next().await {
            match res {
                Ok(Err(e)) => {
                    tracing::error!("Streaming fragment failed: {}", e);
                    cancel.cancel(); // Trigger topology rollback/cancellation
                    return Err(e);
                }
                Ok(Ok(_)) => {
                    // A streaming fragment finished cleanly (EOF on source)
                }
                Err(e) => {
                    tracing::error!("Monitoring task panicked: {}", e);
                    cancel.cancel();
                    return Err(VeinError::internal("Monitoring task panicked"));
                }
            }
        }

        Ok(())
    }
}
