use super::*;
use common::statistics::QueryStatisticsContext;

mod ownership;
mod output;
mod cancellation;

fn scoped_task() -> (DefaultTaskExecutor, ExecuteTask, Arc<QueryExecutionResources>) {
    let mut task = task();
    task.statistics_context = Some(Arc::new(QueryStatisticsContext::new([])));
    let executor = DefaultTaskExecutor::new(Arc::new(MemoryFeedBuilder::new()));
    let resources = executor.create_query_resources(&task, CancellationToken::new()).unwrap();
    (executor, task, resources)
}
