use super::*;
use common::cancellation::CancellationToken;
use common::identifiers::{QueryId, RequestId, StageId, TaskId};
use common::protocol::{DataSink, DataSource, ExecuteTask, ExecutionMode, ExpressionProgram,
                       PhysicalPlan, PhysicalPlanNode, PhysicalPlanOperator};
use common::{Result, VeinError};
use exec::memory::ExecutionResourceLimits;
use exec::runtime::QueryExecutionResources;
use exec::spill::{LocalSpillStorageFactory, SpillStorageFactory};
use plan::{ColumnId, ResolvedColumn, VeinDataType, VeinSchema, VeinSchemaRef};
use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::{Arc, Mutex, atomic::{AtomicBool, AtomicUsize, Ordering}};
use std::task::{Context, Poll, Waker};

mod lifecycle;
mod ownership;
mod clients;
mod writes;
mod readiness;

fn schema() -> VeinSchemaRef {
    let mut column = ResolvedColumn::new(ColumnId(1), "v", None, VeinDataType::Int64);
    column.nullable = true;
    Arc::new(VeinSchema::new(vec![Arc::new(column)]))
}
fn resources(limits: ExecutionResourceLimits) -> Arc<QueryExecutionResources> {
    let schema = schema();
    let source = Arc::new(PhysicalPlanNode {
        output_schema: Arc::clone(&schema),
        operator: PhysicalPlanOperator::Pipeline {
            source: Arc::new(DataSource::Memory),
            input_schema: Arc::clone(&schema),
            program: Arc::new(ExpressionProgram::empty()),
        },
    });
    let task = ExecuteTask {
        request_id: RequestId::new(),
        query_id: QueryId::new(),
        task_id: TaskId::new(),
        stage_id: StageId::new(),
        attempt: 0,
        mode: ExecutionMode::Batch,
        statistics_context: None,
        plan: PhysicalPlan {
            root: Arc::new(PhysicalPlanNode {
                output_schema: schema,
                operator: PhysicalPlanOperator::Sink {
                    input: source,
                    sink: Arc::new(DataSink::Discard),
                },
            })
        },
    };
    Arc::new(QueryExecutionResources::new(&task, limits, CancellationToken::new(),
                                          LocalSpillStorageFactory.create_for_query().unwrap()).unwrap())
}
fn view(values: Vec<Option<i64>>) -> exec::pipeline::DataChunkView {
    let rows = values.len();
    exec::pipeline::DataChunkView::from_arrays(
        vec![Arc::new(arrow::array::Int64Array::from(values))], rows,
        common::arrow::SelectionVector::new_dense(rows),
    ).unwrap()
}

#[derive(Clone, Default)]
struct Gate {
    ready: Arc<AtomicBool>,
    waker: Arc<Mutex<Option<Waker>>>,
}
impl Gate {
    fn release(&self) {
        self.ready.store(true, Ordering::SeqCst);
        let wake = self.waker.lock().unwrap().take();
        if let Some(wake) = wake { wake.wake(); }
    }
}
enum Action {
    Batch(exec::pipeline::DataChunkView),
    Wait(Gate),
    Fail(VeinError),
    Complete(Option<exec::operators::sink::WriteReceipt>),
}
struct Script {
    actions: VecDeque<Action>,
    resources: Arc<QueryExecutionResources>,
    calls: Arc<AtomicUsize>,
    drops: Arc<AtomicUsize>,
}
impl QueryExecution for Script {
    fn poll_event(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<QueryEvent>> {
        let this = self.get_mut();
        this.calls.fetch_add(1, Ordering::SeqCst);
        if let Some(Action::Wait(gate)) = this.actions.front() {
            let mut waker = gate.waker.lock().unwrap();
            *waker = Some(cx.waker().clone());
            if !gate.ready.load(Ordering::SeqCst) { return Poll::Pending; }
            drop(waker);
            this.actions.pop_front();
        }
        Poll::Ready(match this.actions.pop_front().expect("cursor must not poll past completion") {
            Action::Batch(view) => ResultBatch::account(view, this.resources.memory_tracker()).map(QueryEvent::Batch),
            Action::Fail(error) => Err(error),
            Action::Complete(write_receipt) => Ok(QueryEvent::Complete { write_receipt }),
            Action::Wait(_) => panic!("one gate per scripted turn"),
        })
    }
}
impl Drop for Script {
    fn drop(&mut self) { self.drops.fetch_add(1, Ordering::SeqCst); }
}

fn handle(actions: Vec<Action>) -> QueryHandle<Script> {
    typed(resources(ExecutionResourceLimits::default()), schema(), QueryKind::Rows, actions)
}
fn typed(resources: Arc<QueryExecutionResources>, schema: VeinSchemaRef, kind: QueryKind,
         actions: Vec<Action>) -> QueryHandle<Script> {
    let script = Script {
        actions: actions.into(),
        resources: Arc::clone(&resources),
        calls: Arc::new(AtomicUsize::new(0)),
        drops: Arc::new(AtomicUsize::new(0)),
    };
    QueryHandle::from_execution(resources, schema, kind, script).unwrap()
}
