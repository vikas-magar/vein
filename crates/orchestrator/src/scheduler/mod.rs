pub mod elasticity;
pub mod quic_dispatcher;

use crate::executor::ExecutorRegistry;
use crate::traits::TaskDispatcher;
use common::cancellation::CancellationToken;
use common::protocol::ExecutionMode;
use error::VeinError;
use physical::Job;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::task::JoinSet;

/// Orchestrates the execution of a [`Job`] across the cluster.
///
/// The scheduler consumes a physical [`Job`], walks its stages in topological
/// order, and dispatches every task in a stage before advancing to the next
/// stage.
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

    /// Borrows the active executor registry.
    pub fn registry(&self) -> &Arc<ExecutorRegistry> {
        &self.registry
    }

    /// Borrows the task dispatcher.
    pub fn dispatcher(&self) -> &Arc<dyn TaskDispatcher> {
        &self.dispatcher
    }

    /// Execute the [`Job`], choosing the strategy based on its execution mode.
    pub async fn execute(&self, job: Job, cancel: CancellationToken) -> Result<(), VeinError> {
        match job.mode() {
            ExecutionMode::Batch => self.execute_batch_waves(job, cancel).await,
            ExecutionMode::Streaming => self.execute_streaming_topology(job, cancel).await,
        }
    }

    /// Schedule a Batch query using wave (bottom-up) execution.
    ///
    /// It traverses the job topologically, dispatching all tasks of each stage
    /// concurrently and waiting for the whole stage to complete before starting
    /// the next wave.
    async fn execute_batch_waves(
        &self,
        job: Job,
        cancel: CancellationToken,
    ) -> Result<(), VeinError> {
        let order = job.topological_order();

        for stage_id in order {
            // Check cancellation before each wave
            if cancel.is_cancelled() {
                return Err(VeinError::Cancelled);
            }

            let stage = job
                .get_stage(stage_id)
                .ok_or_else(|| VeinError::internal("Invalid StageId in topological order"))?;

            // Get available executors (with 15s heartbeat timeout)
            let executors = self.registry.available(Duration::from_secs(15)).await;
            if executors.is_empty() {
                return Err(VeinError::NoExecutorsAvailable);
            }

            let mut join_set = JoinSet::new();
            for (i, task) in stage.tasks().iter().enumerate() {
                let query_id = job.query_id;
                let mode = *job.mode();
                let execute_task = task
                    .clone()
                    .into_execute_task(job.request_id, query_id, stage.stage_id, mode);

                // Pick the least-loaded executor (executors are already sorted by load).
                let executor = executors[i % executors.len()].clone();
                let dispatcher = self.dispatcher.clone();
                let cancel = cancel.clone();
                let timeout = self.timeout;
                let max_retries = self.max_retries;

                join_set.spawn(async move {
                    let task_id = execute_task.task_id;
                    let stage_id = execute_task.stage_id;
                    let mut attempts = 0;
                    let task_started = Instant::now();
                    loop {
                        // 1. Dispatch
                        let dispatch_started = Instant::now();
                        tracing::info!(
                            target: "vein.audit",
                            event = "task_dispatch_started",
                            %query_id,
                            %stage_id,
                            %task_id,
                            executor = %executor.node_id,
                            attempt = attempts,
                            "task_dispatch_started"
                        );
                        if let Err(e) = dispatcher.dispatch(&executor, execute_task.clone()).await {
                            tracing::warn!(
                                target: "vein.audit",
                                event = "task_dispatch_failed",
                                %query_id,
                                %stage_id,
                                %task_id,
                                executor = %executor.node_id,
                                attempt = attempts,
                                duration_ms = dispatch_started.elapsed().as_millis() as u64,
                                error = %e,
                                "task_dispatch_failed"
                            );
                            attempts += 1;
                            if attempts > max_retries {
                                return Err(e);
                            }
                            continue; // Retry dispatch
                        }

                        // 2. Wait for completion
                        let d = dispatcher.clone();
                        let c = cancel.clone();
                        let wait_result = tokio::select! {
                            res = d.wait_for_completion(query_id, task_id) => res,
                            _ = c.cancelled() => {
                                let _ = d.cancel_task(&executor, query_id, task_id).await;
                                return Err(VeinError::Cancelled);
                            }
                            _ = tokio::time::sleep(timeout) => {
                                let _ = d.cancel_task(&executor, query_id, task_id).await;
                                return Err(VeinError::TaskFailed {
                                    task_id: task_id.to_string(),
                                    message: "Task timeout".to_string(),
                                })
                            }
                        };

                        match wait_result {
                            Ok(_) => {
                                tracing::info!(
                                    target: "vein.audit",
                                    event = "task_completed",
                                    %query_id,
                                    %stage_id,
                                    %task_id,
                                    executor = %executor.node_id,
                                    attempt = attempts,
                                    duration_ms = task_started.elapsed().as_millis() as u64,
                                    "task_completed"
                                );
                                return Ok(());
                            }
                            Err(e) => {
                                tracing::warn!(
                                    target: "vein.audit",
                                    event = "task_attempt_failed",
                                    %query_id,
                                    %stage_id,
                                    %task_id,
                                    executor = %executor.node_id,
                                    attempt = attempts,
                                    duration_ms = task_started.elapsed().as_millis() as u64,
                                    error = %e,
                                    "task_attempt_failed"
                                );
                                attempts += 1;
                                if attempts > max_retries {
                                    return Err(e);
                                }
                            }
                        }
                    }
                });
            }

            // Wait for every task of the stage before advancing the wave.
            while let Some(res) = join_set.join_next().await {
                match res {
                    Ok(Ok(())) => {}
                    Ok(Err(e)) => {
                        cancel.cancel();
                        return Err(e);
                    }
                    Err(e) => {
                        cancel.cancel();
                        return Err(VeinError::internal(format!(
                            "stage task panicked: {e}"
                        )));
                    }
                }
            }
        }

        Ok(())
    }

    /// Schedule a Streaming query by dispatching all stages simultaneously.
    async fn execute_streaming_topology(
        &self,
        job: Job,
        cancel: CancellationToken,
    ) -> Result<(), VeinError> {
        let order = job.topological_order();

        let executors = self.registry.available(Duration::from_secs(15)).await;
        if executors.is_empty() {
            return Err(VeinError::NoExecutorsAvailable);
        }

        let mut join_set = JoinSet::new();

        for (i, stage_id) in order.into_iter().enumerate() {
            let stage = job
                .get_stage(stage_id)
                .ok_or_else(|| VeinError::internal("Invalid StageId in topological order"))?;
            let executor = &executors[i % executors.len()]; // Round-robin assign

            for task in stage.tasks() {
                let execute_task = task
                    .clone()
                    .into_execute_task(job.request_id, job.query_id, stage.stage_id, ExecutionMode::Streaming);

                // Dispatch task
                self.dispatcher.dispatch(executor, execute_task.clone()).await?;

                // Spawn a monitoring task for this continuous fragment
                let dispatcher = self.dispatcher.clone();
                let tid = execute_task.task_id;
                let c = cancel.clone();

                join_set.spawn(async move {
                    tokio::select! {
                        res = dispatcher.wait_for_completion(job.query_id, tid) => res,
                        _ = c.cancelled() => Err(VeinError::Cancelled),
                    }
                });
            }
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