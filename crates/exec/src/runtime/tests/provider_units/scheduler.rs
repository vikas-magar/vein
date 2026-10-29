use super::*;
use crate::operators::source::scan::cursor::{ScanPipelineProvider, ScanWaitRegistrar};
use crate::pipeline::resumable::{self, *};
use crate::pipeline::resumable::executable::{ExecutablePipelinePlan, PipelineOperatorProvider, PipelineOperatorState};
use crate::pipeline::resumable::host::CooperativeHost;
use crate::pipeline::resumable::scheduler::{SchedulerStep, WaitRegistrar};
use common::protocol::pipeline_graph::PipelinePhase;
use std::{any::Any, num::NonZeroUsize, task::Waker};

struct Provider {
    scan: ScanPipelineProvider,
    output: Arc<Mutex<Vec<DataChunkView>>>,
}
struct SinkState {
    metadata: Arc<OperatorMetadata>,
    output: Arc<Mutex<Vec<DataChunkView>>>,
}
struct SinkStage {
    output: Arc<Mutex<Vec<DataChunkView>>>,
    ended: bool,
}
impl ResumableOperator for SinkStage {
    type WaitReason = ();
    fn push_input(&mut self, _: OperatorInputPort, event: InputEvent) -> Result<Handoff<InputEvent>> {
        match event {
            InputEvent::Batch(view) => self.output.lock().unwrap().push(view),
            InputEvent::EndOfInput => self.ended = true,
        }
        Ok(Handoff::Accepted)
    }
    fn step(&mut self) -> Result<OperatorStep<()>> {
        Ok(if self.ended { OperatorStep::Finished } else { OperatorStep::NeedsInput })
    }
}
impl LocalStateOperator for SinkStage {
    type LocalState = ();
    fn take_local_state(&mut self) -> Result<()> { Ok(()) }
}
impl PipelineOperatorState for SinkState {
    fn capabilities(&self, _: PipelinePhase) -> Result<OperatorCapabilities> {
        Ok(OperatorCapabilities::new(vec![InputProperties::any()], OrderingGuarantee::Unknown,
                                     PartitioningGuarantee::Unknown, ParallelismCapability::Independent))
    }
    fn create_stage(&self, phase: PipelinePhase) -> Result<Box<dyn ErasedResumableOperator>> {
        resumable::erase_operator(SinkStage { output: Arc::clone(&self.output), ended: false },
                                  &self.metadata, phase)
    }
    fn combine_stage(&self, _: PipelinePhase, local: Box<dyn Any + Send>) -> Result<()> {
        local.downcast::<()>().unwrap();
        Ok(())
    }
}
impl PipelineOperatorProvider for Provider {
    fn prepare(&self, node: &Arc<PhysicalPlanNode>, metadata: Arc<OperatorMetadata>,
               phases: &[PipelinePhase]) -> Result<Arc<dyn PipelineOperatorState>> {
        if metadata.role == OperatorRole::Sink {
            return Ok(Arc::new(SinkState { metadata, output: Arc::clone(&self.output) }));
        }
        self.scan.prepare(node, metadata, phases)
    }
}
struct NoOtherWaits;
impl WaitRegistrar for NoOtherWaits {
    type Registration = ();
    fn register(&self, _: ErasedWaitReason, _: Waker) -> Result<()> {
        Err(VeinError::internal("unexpected non-scan wait"))
    }
}
struct EventHost;
impl CooperativeHost for EventHost {
    async fn yield_control(&mut self) -> Result<()> {
        let mut first = true;
        futures::future::poll_fn(|cx| {
            if std::mem::take(&mut first) {
                cx.waker().wake_by_ref();
                std::task::Poll::Pending
            } else { std::task::Poll::Ready(()) }
        }).await;
        Ok(())
    }
}

pub(super) async fn validate(executor: &DefaultTaskExecutor, task: &ExecuteTask) {
    for native in [false, true] {
        let scope = executor.create_query_resources(task, CancellationToken::new()).unwrap();
        let output = Arc::new(Mutex::new(Vec::new()));
        let provider = Provider {
            scan: ScanPipelineProvider::new(Arc::clone(&executor.builder), task, Arc::clone(&scope)).unwrap(),
            output: Arc::clone(&output),
        };
        let plan = ExecutablePipelinePlan::prepare(&task.plan, &OperatorFactoryRegistry::new(), &provider).unwrap();
        let n = |value| NonZeroUsize::new(value).unwrap();
        let host = scope.host_driver(&plan, n(2), n(1), ScanWaitRegistrar(NoOtherWaits)).unwrap();
        let result = if native {
            host.run_native(n(2)).await.unwrap()
        } else {
            host.run_cooperative(&mut EventHost).await.unwrap()
        };
        assert_eq!(result, SchedulerStep::Finished);
        let mut values = output.lock().unwrap().iter().flat_map(|view| view_int64_values(view, 0)).collect::<Vec<_>>();
        values.sort_unstable();
        assert_eq!(values, [0, 1, 2, 3, 100, 101]);
    }
}

pub(super) fn validate_small_pool(executor: &DefaultTaskExecutor, task: &ExecuteTask) {
    let runtime = tokio::runtime::Builder::new_current_thread().enable_all()
        .max_blocking_threads(1).build().unwrap();
    runtime.block_on(async {
        tokio::time::timeout(std::time::Duration::from_secs(5), validate(executor, task))
            .await.expect("CPU workers must release the blocking slot for pending I/O");
    });
}
