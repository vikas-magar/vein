use super::*;
use crate::memory::ExecutionResourceLimits;
use crate::pipeline::resumable::executable::ExecutablePipelinePlan;
use crate::runtime::QueryExecutionResources;
use crate::spill::{LocalSpillStorageFactory, SpillStorageFactory};
use common::cancellation::CancellationToken;
use common::identifiers::{QueryId, RequestId, StageId, TaskId};
use common::protocol::{ExecuteTask, ExecutionMode, PhysicalPlan};
use common::statistics::QueryStatisticsContext;

mod cancellation;
mod delivery;

fn resources(plan: &ExecutablePipelinePlan) -> Arc<QueryExecutionResources> {
    let task = ExecuteTask {
        request_id: RequestId::new(),
        query_id: QueryId::new(),
        task_id: TaskId::new(),
        stage_id: StageId::new(),
        attempt: 0,
        mode: ExecutionMode::Batch,
        plan: PhysicalPlan {
            root: Arc::clone(&plan.graph().pipelines()[0].sink.node),
        },
        statistics_context: Some(Arc::new(QueryStatisticsContext::new([]))),
    };
    Arc::new(QueryExecutionResources::new(
        &task,
        ExecutionResourceLimits::default(),
        CancellationToken::new(),
        LocalSpillStorageFactory.create_for_query().unwrap(),
    ).unwrap())
}
