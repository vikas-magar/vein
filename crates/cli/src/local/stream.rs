use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use common::{Result, VeinError};

use super::adaptive;
use super::query::{LocalQuery, LocalResultStream};
use super::tasks::run_task;

const OUTPUT_BUFFER_BATCHES: usize = 2;

impl LocalQuery {
    /// Runs the query and yields reference-backed batches through bounded channels.
    ///
    /// Tasks execute concurrently, yet their batches are drained in planning
    /// order, so the result is identical to sequential execution. Every task owns
    /// a bounded channel, which caps buffering at `workers * OUTPUT_BUFFER_BATCHES`
    /// batches no matter how large the result is. The yielded views retain source
    /// arrays and row mappings; consumers opt into contiguous Arrow conversion.
    pub fn into_stream(self) -> LocalResultStream {
        Box::pin(async_stream::stream! {
            let LocalQuery {
                tasks: initial_tasks,
                executor,
                statistics_context,
                logical_plan,
                adaptive_checkpoint,
                adaptive_policy,
                optimizer,
                planner,
                request_id,
                query_id,
                ..
            } = self;
            let mut adaptive_execution = adaptive::AdaptiveCoordinator {
                executor: Arc::clone(&executor), optimizer, planner, policy: adaptive_policy,
                statistics: Arc::clone(&statistics_context), request_id, query_id,
            }.execute(initial_tasks, logical_plan, adaptive_checkpoint).await;
            let tasks = std::mem::take(&mut adaptive_execution.tasks);
            let query_resources = adaptive_execution.resources.clone();

            let tasks = Arc::new(tasks);
            let task_count = tasks.len();
            if task_count == 0 {
                return;
            }

            // As in DuckDB's `TryGetMaxThreads`, the machine's parallelism caps
            // how wide a single stage may fan out.
            let workers = std::thread::available_parallelism()
                .map(|parallelism| parallelism.get())
                .unwrap_or(1)
                .min(task_count);

            tracing::info!(
                target: "vein.audit",
                event = "task_dispatch",
                task_count,
                workers,
                "task_dispatch"
            );

            // One bounded channel per task, in planning order: the consumer
            // drains them in order, so a slow early task throttles the later ones
            // instead of reordering the result.
            let mut senders = Vec::with_capacity(task_count);
            let mut receivers = Vec::with_capacity(task_count);
            for _ in 0..task_count {
                let (sender, receiver) = flume::bounded(OUTPUT_BUFFER_BATCHES);
                senders.push(Mutex::new(Some(sender)));
                receivers.push(receiver);
            }
            let senders = Arc::new(senders);
            // Written once per task; the consumer reads it after that task's
            // channel disconnects to learn whether the task failed.
            let outcomes: Arc<Mutex<Vec<Option<Result<()>>>>> =
                Arc::new(Mutex::new((0..task_count).map(|_| None).collect()));

            if task_count == 1 {
                // Still dispatched rather than awaited here: the consumer below
                // only starts draining after this point, so a task that fills
                // the bounded channel would block forever. A join is the first
                // operator to emit more chunks than the channel holds, which is
                // how this surfaced — a scan emits one chunk per file.
                let executor = executor.clone();
                let tasks = Arc::clone(&tasks);
                let senders = Arc::clone(&senders);
                let outcomes = Arc::clone(&outcomes);
                let query_resources = query_resources.clone();
                tokio::spawn(async move {
                    run_task(&executor, &tasks[0], &senders, &outcomes, 0, query_resources).await;
                });
            } else {
                // The first `workers` tasks are claimed up front; afterwards a
                // worker pulls the next unclaimed index, so a straggler is
                // absorbed by whichever worker finishes first.
                let next = Arc::new(AtomicUsize::new(workers));
                for worker in 0..workers {
                    let executor = executor.clone();
                    let tasks = Arc::clone(&tasks);
                    let senders = Arc::clone(&senders);
                    let outcomes = Arc::clone(&outcomes);
                    let query_resources = query_resources.clone();
                    let next = Arc::clone(&next);
                    tokio::spawn(async move {
                        let mut index = worker;
                        loop {
                            run_task(&executor, &tasks[index], &senders, &outcomes, index, query_resources.clone()).await;
                            index = next.fetch_add(1, Ordering::Relaxed);
                            if index >= tasks.len() {
                                return;
                            }
                        }
                    });
                }
            }

            for (index, receiver) in receivers.into_iter().enumerate() {
                while let Ok(batch) = receiver.recv_async().await {
                    yield Ok(batch);
                }
                let outcome = outcomes
                    .lock()
                    .ok()
                    .and_then(|mut outcomes| outcomes.get_mut(index).and_then(Option::take));
                match outcome {
                    Some(Ok(())) => {}
                    Some(Err(error)) => {
                        yield Err(error);
                        return;
                    }
                    None => {
                        yield Err(VeinError::internal(
                            "task worker stopped without reporting an outcome",
                        ));
                        return;
                    }
                }
            }
        })
    }
}
