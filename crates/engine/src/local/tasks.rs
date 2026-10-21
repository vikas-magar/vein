use std::sync::{Arc, Mutex};

use common::Result;
use common::cancellation::CancellationToken;
use common::protocol::ExecuteTask;
use common::statistics::QueryStatisticsContext;
use exec::pipeline::DataChunkView;
use exec::runtime::TaskExecutor;

pub(super) fn tasks_from_job(
    job: physical::Job,
    statistics_context: Arc<QueryStatisticsContext>,
    query_id: common::identifiers::QueryId,
) -> Result<Vec<ExecuteTask>> {
    let (request_id, _, mode, _, stage) = job.into_parts();
    let stage_id = stage.stage_id;
    stage
        .tasks
        .into_iter()
        .map(|task| {
            let mut task = task.into_execute_task(request_id, query_id, stage_id, mode);
            task.query_id = query_id;
            task.statistics_context = Some(Arc::clone(&statistics_context));
            Ok(task)
        })
        .collect()
}

/// Executes one task into its output channel and records the outcome.
///
/// The channel's last sender is dropped only after the outcome is recorded, so
/// the consumer never observes a disconnected channel whose outcome is unset.
pub(super) async fn run_task(
    executor: &Arc<dyn TaskExecutor>,
    task: &ExecuteTask,
    senders: &[Mutex<Option<flume::Sender<DataChunkView>>>],
    outcomes: &Mutex<Vec<Option<Result<()>>>>,
    index: usize,
    resources: Option<Arc<exec::runtime::QueryExecutionResources>>,
) {
    let Some(sender) = senders[index].lock().ok().and_then(|mut slot| slot.take()) else {
        return;
    };
    let outcome = match resources {
        Some(resources) => executor
            .execute_streaming_with_resources(
                task,
                CancellationToken::new(),
                sender.clone(),
                resources,
            )
            .await
            .map(|_| ()),
        None => executor
            .execute_streaming(task, CancellationToken::new(), sender.clone())
            .await
            .map(|_| ()),
    };
    if let Ok(mut outcomes) = outcomes.lock()
        && let Some(slot) = outcomes.get_mut(index)
    {
        *slot = Some(outcome);
    }
    drop(sender);
}
