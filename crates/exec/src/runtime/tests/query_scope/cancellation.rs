use super::*;
use std::future::Future;
use std::task::{Context, Poll};

struct PendingSource(Arc<AtomicUsize>);
#[async_trait]
impl Source for PendingSource {
    async fn execute(&mut self, _: &mut dyn PipelineNode) -> Result<()> {
        self.0.fetch_add(1, Ordering::SeqCst);
        futures::future::pending::<()>().await;
        unreachable!()
    }
}
struct IoBuilder {
    inner: MemoryFeedBuilder,
    calls: Arc<AtomicUsize>,
}
impl ExecutorBuilder for IoBuilder {
    fn build_source(&self, _: &DataSource, _: TaskId, _: u32) -> Result<Box<dyn Source>> {
        Ok(Box::new(PendingSource(Arc::clone(&self.calls))))
    }
    fn build_expression_executor(
        &self,
        program: &Arc<ExpressionProgram>,
        next: Box<dyn PipelineNode>,
    ) -> Result<Box<dyn Transform>> {
        self.inner.build_expression_executor(program, next)
    }
    fn build_sink(&self, sink: &DataSink) -> Result<Box<dyn Sink>> {
        self.inner.build_sink(sink)
    }
}

#[test]
fn root_cancellation_interrupts_a_source_that_never_publishes_a_batch() {
    let calls = Arc::new(AtomicUsize::new(0));
    let executor = DefaultTaskExecutor::new(Arc::new(IoBuilder {
        inner: MemoryFeedBuilder::new(),
        calls: Arc::clone(&calls),
    }));
    let task = task();
    let resources = executor
        .create_query_resources(&task, CancellationToken::new())
        .unwrap();
    let mut execution = Box::pin(executor.execute_with_resources(&task, Arc::clone(&resources)));
    let waker = futures::task::noop_waker();
    assert!(matches!(
        execution.as_mut().poll(&mut Context::from_waker(&waker)),
        Poll::Pending
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    resources.cancel();
    assert!(matches!(
        futures::executor::block_on(execution),
        Err(VeinError::Cancelled)
    ));
}

#[tokio::test]
async fn cancellation_interrupts_checkpoint_replay_blocked_on_result_delivery() {
    let (executor, mut task, resources) = scoped_task();
    let checkpoint = executor
        .execute_to_checkpoint_with_resources(&task, Arc::clone(&resources))
        .await
        .unwrap();
    let source_id = common::identifiers::IntermediateSourceId::new();
    let _registration = executor
        .register_intermediate_source(task.query_id, source_id, checkpoint.relation)
        .unwrap();
    let schema = int64_schema();
    let input = plan_node(
        Arc::clone(&schema),
        PhysicalPlanOperator::Pipeline {
            source: Arc::new(DataSource::Intermediate(source_id)),
            input_schema: schema,
            program: Arc::new(ExpressionProgram::empty()),
        },
    );
    task.plan = plan_with_input(&task, input);
    let (sender, _receiver) = flume::bounded(0);
    let mut execution = Box::pin(executor.execute_streaming_with_resources(
        &task, sender, Arc::clone(&resources),
    ));
    let waker = futures::task::noop_waker();
    assert!(execution.as_mut().poll(&mut Context::from_waker(&waker)).is_pending());
    resources.cancel();
    assert!(matches!(execution.await, Err(VeinError::Cancelled)));
}
