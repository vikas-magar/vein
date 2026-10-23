use std::sync::Arc;

use crate::local::{
    LocalQueryEngine, test_engine,
};
use arrow::array::{Array, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use common::statistics::QueryStatisticsContext;
use futures::StreamExt;

use super::support::{NoCheckpoints, adaptive_engine};

const JOIN_QUERY: &str = "FROM a |> WHERE a_key < 4 |> JOIN b ON a_key = b_key
    |> JOIN c ON b_key = c_key |> WHERE c_key < 5
    |> SELECT a_key, b_key, c_key |> ORDER a_key, b_key, c_key";

fn join_fixture() -> tempfile::TempDir {
    let root = tempfile::tempdir().unwrap();
    write_table(root.path(), "a", vec![1, 1, 2, 3, 4]);
    write_table(root.path(), "b", vec![1, 1, 2, 4, 5]);
    write_table(root.path(), "c", vec![1, 2, 2, 4, 6]);
    root
}

fn write_table(root: &std::path::Path, name: &str, values: Vec<i64>) {
    let schema = Arc::new(Schema::new(vec![Field::new(format!("{name}_key"), DataType::Int64, false)]));
    let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(Int64Array::from(values))]).unwrap();
    super::super::support::write_parquet_table(root, name, "create", schema, &batch);
}

async fn run(engine: &LocalQueryEngine, query: &str) -> (Vec<Vec<i64>>, Arc<QueryStatisticsContext>) {
    let prepared = engine.prepare(query).await.unwrap();
    let statistics = Arc::clone(prepared.statistics_context());
    let mut stream = prepared.into_stream();
    let mut output = Vec::new();
    while let Some(batch) = stream.next().await {
        let batch = batch.unwrap();
        for row in 0..batch.num_rows() {
            let values = (0..batch.num_columns())
                .map(|column| {
                    let (array, physical_row) = batch
                        .resolve(column, row)
                        .unwrap()
                        .expect("query result is not null-extended");
                    let values = array.as_any().downcast_ref::<Int64Array>().unwrap();
                    assert!(!values.is_null(physical_row));
                    values.value(physical_row)
                })
                .collect();
            output.push(values);
        }
    }
    (output, statistics)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn resolved_join_search_stops_checkpointing_without_repeating_rows() {
    let root = join_fixture();
    let query = JOIN_QUERY;
    let adaptive = adaptive_engine(root.path());
    let fixed = test_engine(root.path()).with_adaptive_policy(Arc::new(NoCheckpoints));
    let (expected, _) = run(&fixed, query).await;
    let (actual, statistics) = run(&adaptive, query).await;
    assert_eq!(actual, expected);
    assert_eq!(actual.len(), 6);
    let snapshot = statistics.snapshot();
    let observed_rows: Vec<_> = snapshot
        .intermediates
        .values()
        .filter_map(|statistics| statistics.num_rows)
        .collect();
    assert_eq!(snapshot.intermediates.len(), 1,
               "resolved join search must not request redundant checkpoints");
    assert_eq!(observed_rows, vec![4]);
    assert!(snapshot.intermediates.values().all(|statistics| {
        statistics.column_statistics.values().all(|column| column.distinct_count.is_some())
    }));
}

struct FailsSecondCheckpoint {
    inner: Arc<dyn exec::runtime::TaskExecutor>,
    calls: std::sync::atomic::AtomicUsize,
    cancel: std::sync::Mutex<Option<common::cancellation::CancellationToken>>,
}

#[async_trait::async_trait]
impl exec::runtime::TaskExecutor for FailsSecondCheckpoint {
    fn create_query_resources(&self, cancel: common::cancellation::CancellationToken)
                              -> common::Result<Arc<exec::runtime::QueryExecutionResources>> {
        *self.cancel.lock().unwrap() = Some(cancel.clone());
        self.inner.create_query_resources(cancel)
    }
    async fn execute(&self, task: &common::protocol::ExecuteTask, cancel: common::cancellation::CancellationToken)
                     -> common::Result<exec::runtime::TaskResult> { self.inner.execute(task, cancel).await }
    async fn execute_to_checkpoint(&self, task: &common::protocol::ExecuteTask, cancel: common::cancellation::CancellationToken)
                                   -> common::Result<exec::runtime::checkpoint::CheckpointTaskResult> {
        self.inner.execute_to_checkpoint(task, cancel).await
    }
    async fn execute_to_checkpoint_with_resources(&self, task: &common::protocol::ExecuteTask,
                                                  cancel: common::cancellation::CancellationToken, resources: Arc<exec::runtime::QueryExecutionResources>)
                                                  -> common::Result<exec::runtime::checkpoint::CheckpointTaskResult> {
        if self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 1 {
            return Err(common::VeinError::internal("injected second-stage producer failure"));
        }
        self.inner.execute_to_checkpoint_with_resources(task, cancel, resources).await
    }
    fn register_intermediate_source(&self, query: common::identifiers::QueryId,
                                    source: common::identifiers::IntermediateSourceId, relation: exec::runtime::checkpoint::IntermediateRelation)
                                    -> common::Result<exec::runtime::checkpoint::IntermediateSourceRegistration> {
        self.inner.register_intermediate_source(query, source, relation)
    }
    async fn execute_streaming_with_resources(&self, task: &common::protocol::ExecuteTask,
                                              cancel: common::cancellation::CancellationToken,
                                              output: flume::Sender<exec::pipeline::DataChunkView>,
                                              resources: Arc<exec::runtime::QueryExecutionResources>) -> common::Result<exec::runtime::TaskResult> {
        self.inner.execute_streaming_with_resources(task, cancel, output, resources).await
    }
}

#[tokio::test]
async fn failed_later_stage_keeps_prior_checkpoints_and_returns_each_row_once() {
    let root = join_fixture();
    let base = adaptive_engine(root.path());
    let executor = Arc::clone(base.task_executor());
    let engine = base.with_task_executor(Arc::new(FailsSecondCheckpoint {
        inner: executor,
        calls: std::sync::atomic::AtomicUsize::new(0),
        cancel: Default::default(),
    }));
    let fixed = test_engine(root.path()).with_adaptive_policy(Arc::new(NoCheckpoints));
    let (expected, _) = run(&fixed, JOIN_QUERY).await;
    let (actual, context) = run(&engine, JOIN_QUERY).await;
    assert_eq!(actual, expected);
    assert_eq!(context.snapshot().intermediates.len(), 1,
               "the failed second stage must resume from the first valid continuation");
}

#[tokio::test]
async fn dropping_an_adaptive_result_stream_cancels_its_shared_resource_scope() {
    let root = join_fixture();
    let base = adaptive_engine(root.path());
    let executor = Arc::new(FailsSecondCheckpoint {
        inner: Arc::clone(base.task_executor()),
        calls: std::sync::atomic::AtomicUsize::new(0),
        cancel: Default::default(),
    });
    let engine = base.with_task_executor(executor.clone());
    let mut stream = engine.prepare(JOIN_QUERY).await.unwrap().into_stream();
    assert!(stream.next().await.unwrap().is_ok());
    let cancel = executor.cancel.lock().unwrap().clone().unwrap();
    assert!(!cancel.is_cancelled());
    drop(stream);
    assert!(cancel.is_cancelled(), "dropping the consumer must cancel all adaptive stages");
}

#[tokio::test]
async fn aggregate_join_input_publishes_its_own_population_and_preserves_results() {
    let root = tempfile::tempdir().unwrap();
    write_table(root.path(), "a", vec![1, 1, 2, 3]);
    write_table(root.path(), "b", vec![1, 2, 3]);
    write_table(root.path(), "c", vec![1, 2, 3]);
    let query = "FROM a |> AGGREGATE count(*) AS n GROUP BY a_key
        |> JOIN b ON a_key = b_key |> JOIN c ON b_key = c_key
        |> SELECT a_key, n |> ORDER a_key";
    let adaptive = adaptive_engine(root.path());
    let fixed = test_engine(root.path()).with_adaptive_policy(Arc::new(NoCheckpoints));
    let (expected, _) = run(&fixed, query).await;
    let (actual, context) = run(&adaptive, query).await;
    assert_eq!(actual, expected);
    assert_eq!(actual, vec![vec![1, 2], vec![2, 1], vec![3, 1]]);
    assert!(context.snapshot().intermediates.values().any(|statistics| statistics.num_rows == Some(3)));
}
