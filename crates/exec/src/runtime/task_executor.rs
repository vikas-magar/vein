use async_trait::async_trait;
use arrow::record_batch::RecordBatch;
use common::Result;
use common::cancellation::CancellationToken;
use common::protocol::ExecuteTask;
use crate::operators::sink::ViewResultBatch;

use super::{DefaultTaskExecutor, TaskExecutor, TaskResult};

#[async_trait]
impl TaskExecutor for DefaultTaskExecutor {
    async fn execute(&self, task: &ExecuteTask, cancel: CancellationToken) -> Result<TaskResult> {
        self.execute_with_output(task, cancel, |builder, sink| builder.build_sink(sink))
            .await
    }

    async fn execute_views(
        &self,
        task: &ExecuteTask,
        cancel: CancellationToken,
    ) -> Result<TaskResult<ViewResultBatch>> {
        self.execute_with_output(task, cancel, |builder, sink| {
            builder.build_view_result_sink(sink, None)
        })
            .await
    }

    async fn execute_views_streaming(
        &self,
        task: &ExecuteTask,
        cancel: CancellationToken,
        output: flume::Sender<ViewResultBatch>,
    ) -> Result<TaskResult<ViewResultBatch>> {
        self.execute_with_output(task, cancel, move |builder, sink| {
            builder.build_view_result_sink(sink, Some(output))
        })
            .await
    }

    async fn execute_streaming(
        &self,
        task: &ExecuteTask,
        cancel: CancellationToken,
        output: flume::Sender<RecordBatch>,
    ) -> Result<TaskResult> {
        self.execute_with_output(task, cancel, move |builder, sink| {
            builder.build_streaming_sink(sink, output)
        })
            .await
    }
}
