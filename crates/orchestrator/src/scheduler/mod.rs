pub mod elasticity;
pub mod quic_dispatcher;

use crate::executor::ExecutorRegistry;
use crate::traits::{TaskCompletion, TaskDispatcher};
use common::cancellation::CancellationToken;
use common::identifiers::NodeId;
use common::protocol::ExecutionMode;
use error::VeinError;
use physical::Job;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::task::JoinSet;

const QUEUE_ADMISSION_RETRY_DELAY: Duration = Duration::from_millis(25);
const QUEUE_ADMISSION_TIMEOUT: Duration = Duration::from_secs(30);

fn is_executor_queue_full(error: &VeinError) -> bool {
    matches!(
        error,
        VeinError::TaskFailed { message, .. }

            if message.starts_with("EXECUTOR_QUEUE_FULL:")
    )
}

async fn wait_for_queue_admission(
    cancel: &CancellationToken,
    task_id: &str,
    elapsed: Duration,
) -> Result<(), VeinError> {
    if elapsed >= QUEUE_ADMISSION_TIMEOUT {
        return Err(VeinError::TaskFailed {
            task_id: task_id.to_string(),
            message: format!(
                "executor admission timed out after {:?}",
                QUEUE_ADMISSION_TIMEOUT
            ),
        });
    }

    tokio::select! {
        _ = cancel.cancelled() => Err(VeinError::Cancelled),
        _ = tokio::time::sleep(QUEUE_ADMISSION_RETRY_DELAY) => Ok(()),
    }
}

async fn retry_executor(
    registry: &ExecutorRegistry,
    attempted: &[NodeId],
    current: &crate::executor::registry::NodeInfo,
) -> crate::executor::registry::NodeInfo {
    let available = registry.available(Duration::from_secs(15)).await;
    available
        .iter()
        .find(|executor| !attempted.contains(&executor.node_id))
        .or_else(|| available.first())
        .cloned()
        .unwrap_or_else(|| current.clone())
}

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
    pub async fn execute(
        &self,
        job: Job,
        cancel: CancellationToken,
    ) -> Result<TaskCompletion, VeinError> {
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
    ) -> Result<TaskCompletion, VeinError> {
        let order = job.topological_order();
        let output_stage_id = job.output_stage_id();
        let mut total = TaskCompletion::default();

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
                let task_template =
                    task.clone()
                        .into_execute_task(job.request_id, query_id, stage.stage_id, mode);

                // Pick the least-loaded executor (executors are already sorted by load).
                let executor = executors[i % executors.len()].clone();
                let registry = self.registry.clone();
                let dispatcher = self.dispatcher.clone();
                let cancel = cancel.clone();
                let timeout = self.timeout;
                let max_retries = self.max_retries;

                join_set.spawn(async move {
                    let task_id = task_template.task_id;
                    let stage_id = task_template.stage_id;
                    let mut attempts = 0;
                    let task_started = Instant::now();
                    let mut executor = executor;
                    let mut attempted_executors = vec![executor.node_id];
                    loop {
                        let mut execute_task = task_template.clone();
                        execute_task.attempt = attempts;
                        if let common::protocol::DataSink::Shuffle(target) = &mut execute_task.sink
                        {
                            target.attempt = attempts;
                        }
                        let attempt_started = Instant::now();

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
                            executor = retry_executor(&registry, &attempted_executors, &executor).await;
                            attempted_executors.push(executor.node_id);
                            continue; // Retry dispatch
                        }

                        // 2. Wait for completion
                        let d = dispatcher.clone();
                        let c = cancel.clone();
                        let wait_result = tokio::select! {
                            res = d.wait_for_completion(query_id, task_id, attempts) => res,
                            _ = c.cancelled() => {
                                if let Err(error) = d.cancel_task(&executor, query_id, task_id, attempts).await {
                                    tracing::warn!(
                                        target: "vein.audit",
                                        event = "task_cancel_delivery_failed",
                                        %query_id,
                                        %stage_id,
                                        %task_id,
                                        executor = %executor.node_id,
                                        attempt = attempts,
                                        error = %error,
                                        "failed to deliver task cancellation"
                                    );
                                }
                                return Err(VeinError::Cancelled);
                            }
                            _ = tokio::time::sleep(timeout) => {
                                if let Err(error) = d.cancel_task(&executor, query_id, task_id, attempts).await {
                                    return Err(VeinError::TaskFailed {
                                        task_id: task_id.to_string(),
                                        message: format!("task timed out and cancellation delivery failed: {error}"),
                                    });
                                }
                                tracing::warn!(
                                    target: "vein.audit",
                                    event = "task_attempt_timeout",
                                    %query_id,
                                    %stage_id,
                                    %task_id,
                                    executor = %executor.node_id,
                                    attempt = attempts,
                                    duration_ms = attempt_started.elapsed().as_millis() as u64,
                                    "task_attempt_timeout"
                                );
                                attempts += 1;
                                if attempts > max_retries {
                                    return Err(VeinError::TaskFailed {
                                        task_id: task_id.to_string(),
                                        message: "Task timeout".to_string(),
                                    });
                                }
                                executor = retry_executor(&registry, &attempted_executors, &executor).await;
                                attempted_executors.push(executor.node_id);
                                continue;
                            }
                        };

                        match wait_result {
                            Ok(completion) => {
                                tracing::info!(
                                    target: "vein.audit",
                                    event = "task_completed",
                                    %query_id,
                                    %stage_id,
                                    %task_id,
                                    executor = %executor.node_id,
                                    attempt = attempts,
                                    rows = completion.row_count,
                                    duration_ms = task_started.elapsed().as_millis() as u64,
                                    "task_completed"
                                );
                                return Ok(completion);
                            }
                            Err(e) if is_executor_queue_full(&e) => {
                                let admission_elapsed = task_started.elapsed();
                                tracing::debug!(
                                    target: "vein.audit",
                                    event = "task_admission_wait",
                                    %query_id,
                                    %stage_id,
                                    %task_id,
                                    executor = %executor.node_id,
                                    attempt = attempts,
                                    duration_ms = admission_elapsed.as_millis() as u64,
                                    retry_delay_ms = QUEUE_ADMISSION_RETRY_DELAY.as_millis() as u64,
                                    "executor queue is full; retrying admission without a new attempt"
                                );
                                if let Err(error) = wait_for_queue_admission(
                                    &cancel,
                                    &task_id.to_string(),
                                    admission_elapsed,
                                )
                                .await
                                {
                                    return Err(error);
                                }
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
                                executor = retry_executor(&registry, &attempted_executors, &executor).await;
                                attempted_executors.push(executor.node_id);
                            }
                        }
                    }
                });
            }

            // Only the final output stage contributes query/write rows.
            let is_output_stage = stage_id == output_stage_id;
            // Wait for every task of the stage before advancing the wave.
            while let Some(res) = join_set.join_next().await {
                match res {
                    Ok(Ok(completion)) if is_output_stage => {
                        total.row_count += completion.row_count;
                        total.bytes_read += completion.bytes_read;
                        total.duration_ns += completion.duration_ns;

                        total.write_parts.extend(completion.write_parts);
                    }
                    Ok(Ok(_)) => {}
                    Ok(Err(e)) => {
                        cancel.cancel();
                        return Err(e);
                    }
                    Err(e) => {
                        cancel.cancel();
                        return Err(VeinError::internal(format!("stage task panicked: {e}")));
                    }
                }
            }
        }

        Ok(total)
    }

    /// Schedule a Streaming query by dispatching all stages simultaneously.
    async fn execute_streaming_topology(
        &self,
        job: Job,
        cancel: CancellationToken,
    ) -> Result<TaskCompletion, VeinError> {
        let order = job.topological_order();
        let mut total = TaskCompletion::default();

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
                let execute_task = task.clone().into_execute_task(
                    job.request_id,
                    job.query_id,
                    stage.stage_id,
                    ExecutionMode::Streaming,
                );

                // Dispatch task
                self.dispatcher
                    .dispatch(executor, execute_task.clone())
                    .await?;

                // Spawn a monitoring task for this continuous fragment
                let dispatcher = self.dispatcher.clone();
                let tid = execute_task.task_id;
                let c = cancel.clone();

                join_set.spawn(async move {
                    tokio::select! {
                        res = dispatcher.wait_for_completion(job.query_id, tid, execute_task.attempt) => res,
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
                Ok(Ok(completion)) => {
                    total.row_count += completion.row_count;
                    total.bytes_read += completion.bytes_read;
                    total.duration_ns += completion.duration_ns;
                }
                Err(e) => {
                    tracing::error!("Monitoring task panicked: {}", e);
                    cancel.cancel();
                    return Err(VeinError::internal("Monitoring task panicked"));
                }
            }
        }

        Ok(total)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        QUEUE_ADMISSION_TIMEOUT, is_executor_queue_full, retry_executor,
        wait_for_queue_admission,
    };
    use crate::executor::ExecutorRegistry;
    use common::cancellation::CancellationToken;
    use common::identifiers::NodeId;
    use common::protocol::{NodeKind, NodeLoad};
    use error::VeinError;
    use std::time::Duration;

    #[test]
    fn queue_full_is_retryable_but_execution_failure_is_not() {
        let queue_full = VeinError::TaskFailed {
            task_id: "task".to_string(),
            message: "EXECUTOR_QUEUE_FULL: executor admission queue is temporarily full"
                .to_string(),
        };
        let execution_failure = VeinError::TaskFailed {
            task_id: "task".to_string(),
            message: "scan failed".to_string(),
        };

        assert!(is_executor_queue_full(&queue_full));
        assert!(!is_executor_queue_full(&execution_failure));
    }

    #[tokio::test]
    async fn queue_admission_wait_honors_cancellation() {
        let cancel = CancellationToken::new();
        cancel.cancel();

        let result = wait_for_queue_admission(&cancel, "task", Duration::ZERO).await;

        assert!(matches!(result, Err(VeinError::Cancelled)));
    }

    #[tokio::test]
    async fn queue_admission_timeout_is_bounded() {
        let cancel = CancellationToken::new();

        let result = wait_for_queue_admission(&cancel, "task", QUEUE_ADMISSION_TIMEOUT).await;

        assert!(matches!(result, Err(VeinError::TaskFailed { .. })));
    }

    #[tokio::test]
    async fn bounded_queue_drains_thirty_two_tasks() {
        let (sender, mut receiver) = tokio::sync::mpsc::channel(4);
        let producer = tokio::spawn(async move {
            for task in 0..32u32 {
                sender.send(task).await.unwrap();
            }
        });

        for expected in 0..32u32 {
            assert_eq!(receiver.recv().await.unwrap(), expected);
        }

        producer.await.unwrap();
    }

    #[tokio::test]
    async fn retry_prefers_a_different_healthy_executor() {
        let registry = ExecutorRegistry::new();
        let first = NodeId::new();
        let second = NodeId::new();
        registry
            .record_heartbeat(
                first,
                "127.0.0.1:8081".parse().unwrap(),
                NodeKind::Executor,
                NodeLoad {
                    active_tasks: 0,
                    cpu_percent: 0.0,
                    memory_bytes: 0,
                },
            )
            .await;
        registry
            .record_heartbeat(
                second,
                "127.0.0.1:8082".parse().unwrap(),
                NodeKind::Executor,
                NodeLoad {
                    active_tasks: 1,
                    cpu_percent: 0.0,
                    memory_bytes: 0,
                },
            )
            .await;
        let current = registry.available(Duration::from_secs(15)).await[0].clone();

        let retry = retry_executor(&registry, &[current.node_id], &current).await;

        assert_ne!(retry.node_id, current.node_id);
    }
}
