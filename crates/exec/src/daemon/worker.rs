use crate::runtime::TaskExecutor;
use common::cancellation::CancellationToken;
use common::protocol::ExecuteTask;
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use tokio::task::LocalSet;
use tracing::{error, info};

/// A pool of OS threads pinning Tokio LocalSets for cache-local execution.
///
/// Each worker pulls [`ExecuteTask`]s off a shared queue and runs them through
/// the injected [`TaskExecutor`] — the abstraction that builds and drives the
/// pipeline — so the pool itself stays free of any execution logic.
pub struct WorkerPool {
    /// Pure compute threads pinned to CPU cores via Tokio LocalSets.
    cpu_handles: Vec<JoinHandle<()>>,
}

impl WorkerPool {
    /// Start `cores` worker threads that pull tasks off `queue` and execute
    /// each through `executor`.
    pub fn start(
        cores: usize,
        queue: flume::Receiver<ExecuteTask>,
        executor: Arc<dyn TaskExecutor>,
    ) -> Self {
        let mut cpu_handles = Vec::with_capacity(cores);

        for core_id in 0..cores {
            let queue_clone = queue.clone();
            let executor = executor.clone();

            let handle = thread::Builder::new()
                .name(format!("vein-worker-{}", core_id))
                .spawn(move || {
                    info!("Started worker thread {} pinned to CPU core", core_id);

                    // A single-threaded runtime strictly for this thread, with a
                    // LocalSet so `!Send` task futures (driving a Source) stay local.
                    let rt = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .expect("failed to build current-thread runtime");
                    let local = LocalSet::new();

                    local.block_on(&rt, async move {
                        // Work-stealing loop: pull tasks asynchronously
                        while let Ok(task) = queue_clone.recv_async().await {
                            let executor = executor.clone();
                            tokio::task::spawn_local(async move {
                                let cancel = CancellationToken::new();
                                match executor.execute(&task, cancel).await {
                                    Ok(result) => info!(
                                        "Worker {} finished task {}: {} rows",
                                        core_id, task.task_id, result.row_count
                                    ),
                                    Err(e) => error!(
                                        "Worker {} task {} failed: {}",
                                        core_id, task.task_id, e
                                    ),
                                }
                            });
                        }
                        info!("Worker {} shutting down", core_id);
                    });
                })
                .expect("Failed to spawn worker thread");

            cpu_handles.push(handle);
        }

        Self { cpu_handles }
    }

    /// Wait for all workers to finish.
    pub fn join(self) {
        for handle in self.cpu_handles {
            let _ = handle.join();
        }
    }
}