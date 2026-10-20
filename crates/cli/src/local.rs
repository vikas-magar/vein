use std::collections::HashSet;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use binder::LogicalBinder;
use catalog::AnalyzedTableStatistics;
use common::cancellation::CancellationToken;
use common::identifiers::RequestId;
use common::protocol::{DataSink, ExecuteTask, PhysicalPlanOperator};
use common::statistics::analysis::{
    StatisticsAnalysisProvider, analyze_table as analyze_statistics_table,
};
use common::statistics::{
    QuerySourceStatistics, QueryStatisticsContext, RuntimeStatisticsStore, StatisticsConfig,
    TableStatisticsLog,
};
use common::{Result, VeinError};
use exec::operators::sink::{WriteReceipt, WriteStorage};
use exec::pipeline::DataChunkView;
use exec::runtime::TaskExecutor;
use exec::udf::{FunctionRegistry, register_builtin_scalars};
use futures::Stream;
use physical::PhysicalPlanner;
use plan::{CatalogProvider, LogicalPlan, PlanTraversal};

use super::parquet_catalog::ParquetCatalog;
use super::parquet_storage::ParquetStorage;
mod adaptive;
use adaptive::{AdaptiveBoundary, AdaptiveCheckpointPolicy, StatisticsCheckpointPolicy};

const OUTPUT_BUFFER_BATCHES: usize = 2;

/// A local query engine that plans against Parquet metadata and runs tasks in-process.
pub struct LocalQueryEngine {
    binder: binder::QueryBinder,
    optimizer: Arc<optimizer::OptimizerPipeline>,
    planner: Arc<physical::DefaultPhysicalPlanner>,
    adaptive_policy: Arc<dyn AdaptiveCheckpointPolicy>,
    executor: Arc<dyn TaskExecutor>,
    write_storage: Arc<dyn WriteStorage>,
    statistics_provider: Arc<dyn StatisticsAnalysisProvider>,
    runtime_statistics: RuntimeStatisticsStore,
}

impl LocalQueryEngine {
    /// Creates an engine whose table names resolve beneath `data_root`.
    pub fn new(data_root: impl Into<std::path::PathBuf>) -> Self {
        let data_root = data_root.into();
        let runtime_statistics = RuntimeStatisticsStore::default();
        let catalog = Arc::new(
            ParquetCatalog::new(data_root.clone())
                .with_runtime_statistics_store(runtime_statistics.clone()),
        );
        let metadata_store = catalog.metadata_store();
        let statistics_provider: Arc<dyn StatisticsAnalysisProvider> = catalog.clone();
        let catalog: Arc<dyn CatalogProvider> = catalog;
        let functions = Arc::new(binder::BuiltinFunctionRegistry::new());
        let functions: Arc<dyn binder::FunctionRegistry> = functions;
        let mut runtime_functions = FunctionRegistry::new();
        register_builtin_scalars(&mut runtime_functions);
        let storage: Arc<dyn WriteStorage> = Arc::new(ParquetStorage::new(data_root));
        let executor = Arc::new(
            exec::factory::TaskExecutorBuilder::new()
                .with_metadata_store(metadata_store)
                .with_runtime_statistics_store(runtime_statistics.clone())
                .with_registry(Arc::new(runtime_functions))
                .with_write_storage(Arc::clone(&storage))
                .build(),
        );

        Self {
            binder: binder::QueryBinder::new(catalog, functions),
            optimizer: Arc::new(optimizer::OptimizerPipeline::default()),
            planner: Arc::new(physical::DefaultPhysicalPlanner::new()),
            adaptive_policy: Arc::new(StatisticsCheckpointPolicy),
            executor,
            write_storage: storage,
            statistics_provider,
            runtime_statistics,
        }
    }

    /// Analyze a table with default bounded statistics settings and publish the result.
    pub async fn analyze_table(&self, table_name: &str) -> Result<AnalyzedTableStatistics> {
        self.analyze_table_with_config(table_name, StatisticsConfig::default())
            .await
    }

    /// Analyze a table with explicit synopsis, scan, and multi-column settings.
    pub async fn analyze_table_with_config(
        &self,
        table_name: &str,
        config: StatisticsConfig,
    ) -> Result<AnalyzedTableStatistics> {
        analyze_statistics_table(self.statistics_provider.as_ref(), table_name, config).await
    }

    /// Returns the shared runtime statistics populated by completed scans.
    pub fn runtime_statistics(&self) -> &RuntimeStatisticsStore {
        &self.runtime_statistics
    }

    /// Parses, binds, optimizes, and plans a Pipe query for local execution.
    pub async fn prepare(&self, query: &str) -> Result<LocalQuery> {
        let prepare_started = Instant::now();

        let parse_started = Instant::now();
        let mut parser = parser::Parser::new(query);
        let statement = parser
            .parse_statement()
            .map_err(|error| VeinError::planning(format!("query parse failed: {error}"), None))?;
        let parse_us = parse_started.elapsed().as_micros() as u64;

        let bind_started = Instant::now();
        let bound = self.binder.bind_with_catalog_revision(&statement).await?;
        let bind_us = bind_started.elapsed().as_micros() as u64;

        let statistics_context = Arc::new(query_statistics_context(&bound.plan));
        for source in statistics_context.sources() {
            let statistics = source.statistics.as_deref();
            tracing::info!(
                target: "vein.statistics",
                event = "query_source_statistics",
                source_key = %source.source_key,
                table = %source.table_name,
                statistics_available = statistics.is_some(),
                rows = ?statistics.and_then(|value| value.num_rows),
                total_bytes = ?statistics.and_then(|value| value.total_bytes),
                file_count = ?statistics.and_then(|value| value.file_count),
                row_group_count = ?statistics.and_then(|value| value.row_group_count),
                compressed_bytes = ?statistics.and_then(|value| value.compressed_bytes),
                uncompressed_bytes = ?statistics.and_then(|value| value.uncompressed_bytes),
                statistics_summary = %statistics.map_or_else(
                    || "unavailable".to_owned(),
                    |value| TableStatisticsLog::new(value).to_string(),
                ),
                "query source statistics snapshot"
            );
        }

        let optimize_started = Instant::now();
        let optimized = self
            .optimizer
            .optimize_with_statistics_context(bound.plan, Arc::clone(&statistics_context))?
            .0;
        let optimize_us = optimize_started.elapsed().as_micros() as u64;

        if let LogicalPlan::Write {
            target, options, ..
        } = &optimized
        {
            self.write_storage.validate_target(target, options)?;
        }

        let plan_started = Instant::now();
        let request_id = RequestId::new();
        let adaptive_boundary = self.adaptive_policy.choose_boundary(&optimized, Arc::clone(&statistics_context));
        let job = self.planner.create_physical_plan_with_statistics(request_id, optimized, Arc::clone(&statistics_context))?;
        let plan_us = plan_started.elapsed().as_micros() as u64;

        let task_started = Instant::now();
        let (request_id, query_id, mode, logical_plan, stage) = job.into_parts();
        let first_sink = stage
            .tasks
            .first()
            .and_then(|task| match &task.plan.root.operator {
                PhysicalPlanOperator::Sink { sink, .. } => Some(sink.as_ref()),
                _ => None,
            });
        let is_write = first_sink.is_some_and(|sink| matches!(sink, DataSink::Storage(_)));
        if is_write && stage.tasks.len() != 1 {
            return Err(VeinError::planning(
                "a local WRITE must execute as one atomic task",
                None,
            ));
        }
        let stage_id = stage.stage_id;
        let mut tasks = Vec::with_capacity(stage.tasks.len());
        for task in stage.tasks {
            let PhysicalPlanOperator::Sink { sink, .. } = &task.plan.root.operator else {
                return Err(VeinError::planning(
                    "physical task plan must have a sink root",
                    None,
                ));
            };
            match (sink.as_ref(), is_write) {
                (DataSink::Client { .. }, false) | (DataSink::Storage(_), true) => {}
                _ => {
                    return Err(VeinError::planning(
                        "physical stage contains inconsistent sink types",
                        None,
                    ));
                }
            }
            let mut task = task.into_execute_task(request_id, query_id, stage_id, mode);
            task.statistics_context = Some(Arc::clone(&statistics_context));
            tasks.push(task);
        }
        let adaptive_scan = if is_write { None } else {
            adaptive_boundary.and_then(|boundary| adaptive::prepare_checkpoint(
                self.planner.as_ref(), boundary, Arc::clone(&statistics_context), request_id, query_id,
                &tasks, &logical_plan,
            ))
        };
        let task_build_us = task_started.elapsed().as_micros() as u64;

        tracing::info!(
            target: "vein.audit",
            event = "prepare_finished",
            parse_us,
            bind_us,
            optimize_us,
            plan_us,
            task_build_us,
            total_us = prepare_started.elapsed().as_micros() as u64,
            task_count = tasks.len(),
            is_write,
            "prepare_finished"
        );

        Ok(LocalQuery {
            tasks,
            executor: self.executor.clone(),
            statistics_context,
            logical_plan,
            is_write,
            adaptive_checkpoint: adaptive_scan,
            adaptive_policy: Arc::clone(&self.adaptive_policy),
            optimizer: Arc::clone(&self.optimizer),
            planner: Arc::clone(&self.planner),
            request_id,
            query_id,
        })
    }
}

/// A planned local query whose output can be consumed incrementally.
pub struct LocalQuery {
    tasks: Vec<ExecuteTask>,
    executor: Arc<dyn TaskExecutor>,
    statistics_context: Arc<QueryStatisticsContext>,
    logical_plan: Arc<LogicalPlan>,
    is_write: bool,
    adaptive_checkpoint: Option<PreparedAdaptiveCheckpoint>,
    adaptive_policy: Arc<dyn AdaptiveCheckpointPolicy>,
    optimizer: Arc<optimizer::OptimizerPipeline>,
    planner: Arc<physical::DefaultPhysicalPlanner>,
    request_id: RequestId,
    query_id: common::identifiers::QueryId,
}

struct PreparedAdaptiveCheckpoint {
    boundary: AdaptiveBoundary,
    producer_task: ExecuteTask,
}

impl LocalQuery {
    /// Returns the optimized logical plan retained for adaptive continuations.
    pub fn logical_plan(&self) -> &Arc<LogicalPlan> {
        &self.logical_plan
    }

    /// Returns the shared query statistics context used by planning and execution.
    pub fn statistics_context(&self) -> &Arc<QueryStatisticsContext> {
        &self.statistics_context
    }

    /// Whether this prepared query commits data to a storage target.
    pub fn is_write(&self) -> bool {
        self.is_write
    }

    /// Execute a storage write and return only after its commit is confirmed.
    pub async fn execute_write(self) -> Result<WriteReceipt> {
        if !self.is_write || self.tasks.len() != 1 {
            return Err(VeinError::planning(
                "prepared query is not a single-task WRITE",
                None,
            ));
        }
        let result = self
            .executor
            .execute(&self.tasks[0], CancellationToken::new())
            .await?;
        result
            .write_receipt
            .ok_or_else(|| VeinError::internal("WRITE task completed without a commit receipt"))
    }

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

fn tasks_from_job(
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

fn query_statistics_context(plan: &LogicalPlan) -> QueryStatisticsContext {
    fn collect(
        plan: &LogicalPlan,
        sources: &mut Vec<QuerySourceStatistics>,
        seen: &mut HashSet<String>,
    ) {
        fn register_source(
            table: &plan::TableMetadata,
            sources: &mut Vec<QuerySourceStatistics>,
            seen: &mut HashSet<String>,
        ) {
            let source_key = table
                .catalog_snapshot
                .as_ref()
                .map(common::statistics::runtime::table_statistics_key)
                .unwrap_or_else(|| table.name.clone());
            if seen.insert(source_key.clone()) {
                sources.push(QuerySourceStatistics {
                    source_key,
                    table_name: table.name.clone(),
                    statistics: table.statistics.clone().map(Arc::new),
                });
            }
        }
        match plan {
            LogicalPlan::Scan { table, .. } => register_source(table, sources, seen),
            LogicalPlan::ExternalScan {
                source_metadata: Some(table),
                ..
            } => register_source(table, sources, seen),
            _ => {}
        }
        for child in plan.children() {
            collect(child, sources, seen);
        }
        for subquery in plan.subquery_plans() {
            collect(subquery, sources, seen);
        }
    }

    let mut sources = Vec::new();
    collect(plan, &mut sources, &mut HashSet::new());
    QueryStatisticsContext::new(sources)
}

/// Executes one task into its own channel and records the outcome.
///
/// The channel's last sender is dropped only after the outcome is recorded, so
/// the consumer never observes a disconnected channel whose outcome is unset.
async fn run_task(
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

/// Reference-backed query results; no output columns are gathered by the stream.
pub type LocalResultStream = Pin<Box<dyn Stream<Item = Result<DataChunkView>> + Send>>;

#[cfg(test)]
mod tests {
    use std::fs::File;

    use futures::StreamExt;
    use arrow::array::{
        Array, FixedSizeListArray, Float32Array, Float64Array, Int64Array, StringArray,
    };
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;

    use super::*;

    fn materialized_result_column(view: &DataChunkView, column: usize) -> arrow::array::ArrayRef {
        view.materialize_column(column)
            .expect("materialize result column for assertion")
    }

    #[test]
    fn query_statistics_context_registers_catalog_resolved_external_sources() {
        let metadata = Arc::new(
            plan::TableMetadata::new("external_events", Arc::new(plan::VeinSchema::empty()))
                .with_statistics(plan::TableStatistics {
                    num_rows: Some(23),
                    ..plan::TableStatistics::default()
                }),
        );
        let external = LogicalPlan::ExternalScan {
            url: "file:///external_events".into(),
            options: vec![],
            files: vec![],
            schema: Box::new(metadata.schema.as_ref().clone()),
            source_metadata: Some(metadata),
        };

        let context = query_statistics_context(&external);
        assert_eq!(context.sources().len(), 1);
        assert_eq!(context.sources()[0].table_name, "external_events");
        assert_eq!(
            context.sources()[0]
                .statistics
                .as_deref()
                .and_then(|statistics| statistics.population_rows()),
            Some(23)
        );
    }

    fn contains_sort(node: &common::protocol::PhysicalPlanNode) -> bool {
        use common::protocol::PhysicalPlanOperator;
        match &node.operator {
            PhysicalPlanOperator::Sort { .. } => true,
            PhysicalPlanOperator::Transform { input, .. }
            | PhysicalPlanOperator::Aggregate { input, .. }
            | PhysicalPlanOperator::Distinct { input, .. }
            | PhysicalPlanOperator::Limit { input, .. }
            | PhysicalPlanOperator::Sink { input, .. } => contains_sort(input),
            PhysicalPlanOperator::Join { probe, build, .. } => {
                contains_sort(probe) || contains_sort(build)
            }
            PhysicalPlanOperator::Pipeline { .. } => false,
        }
    }

    fn find_file_scan(
        node: &common::protocol::PhysicalPlanNode,
    ) -> Option<&common::protocol::FileScanTask> {
        use common::protocol::{DataSource, PhysicalPlanOperator};
        match &node.operator {
            PhysicalPlanOperator::Pipeline { source, .. } => match source.as_ref() {
                DataSource::File(scan) => Some(scan),
                DataSource::Memory | DataSource::Stream(_) | DataSource::Intermediate(_) => None,
            },
            PhysicalPlanOperator::Transform { input, .. }
            | PhysicalPlanOperator::Aggregate { input, .. }
            | PhysicalPlanOperator::Distinct { input, .. }
            | PhysicalPlanOperator::Sort { input, .. }
            | PhysicalPlanOperator::Limit { input, .. }
            | PhysicalPlanOperator::Sink { input, .. } => find_file_scan(input),
            PhysicalPlanOperator::Join { probe, build, .. } => {
                find_file_scan(probe).or_else(|| find_file_scan(build))
            }
        }
    }

    #[tokio::test]
    async fn executes_ordered_local_parquet_query_across_files() {
        let root = tempfile::tempdir().expect("temporary data root");
        let table_dir = root.path().join("flights");
        std::fs::create_dir_all(&table_dir).expect("create table directory");

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        for (part, values) in [
            ("part-1.parquet", vec![1, 4]),
            ("part-2.parquet", vec![5, 2, 3]),
        ] {
            let batch =
                RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(values))])
                    .expect("valid batch");
            let file = File::create(table_dir.join(part)).expect("create Parquet file");
            let mut writer =
                ArrowWriter::try_new(file, schema.clone(), None).expect("create writer");
            writer.write(&batch).expect("write batch");
            writer.close().expect("close writer");
        }

        let engine = LocalQueryEngine::new(root.path());
        let query = engine
            .prepare("FROM flights |> WHERE id > 1 |> SELECT id |> ORDER id DESC")
            .await
            .expect("prepare ordered local query");
        assert_eq!(
            query.tasks.len(),
            1,
            "a global sort must read all files in one task"
        );
        assert!(contains_sort(&query.tasks[0].plan.root));
        let statistics_context = Arc::clone(query.statistics_context());
        assert_eq!(statistics_context.sources().len(), 1);
        assert_eq!(statistics_context.sources()[0].table_name, "flights");
        assert!(Arc::ptr_eq(
            &statistics_context,
            query.tasks[0]
                .statistics_context
                .as_ref()
                .expect("task shares query context"),
        ));
        let mut stream = query.into_stream();
        let mut rows = Vec::new();
        while let Some(batch) = stream.next().await {
            let batch = batch.expect("query batch");
            for row in 0..batch.num_rows() {
                let (array, index) = batch
                    .resolve(0, row)
                    .expect("resolve reference-backed value")
                    .expect("selected value is not null-extended");
                let ids = array
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("Int64 output");
                rows.push(ids.value(index));
            }
        }

        assert_eq!(rows, vec![5, 4, 3, 2]);
        assert!(
            statistics_context
                .operator_statistics()
                .iter()
                .any(|operator| operator.output_rows > 0)
        );

        let catalog = ParquetCatalog::new(root.path())
            .with_runtime_statistics_store(engine.runtime_statistics.clone());
        let table = CatalogProvider::get_table(&catalog, "flights")
            .await
            .expect("read scan table metadata");
        let runtime_key = common::statistics::runtime::table_statistics_key(
            table
                .catalog_snapshot
                .as_ref()
                .expect("catalog pins source revision"),
        );
        assert!(
            engine
                .runtime_statistics
                .complete_table_statistics(&runtime_key)
                .is_none(),
            "filtered scans must not become full-table optimizer statistics"
        );

        let query = engine
            .prepare("FROM flights |> SELECT id")
            .await
            .expect("prepare complete table scan");
        let mut stream = query.into_stream();
        let mut scanned_rows = 0;
        while let Some(batch) = stream.next().await {
            scanned_rows += batch.expect("full scan batch").num_rows();
        }
        assert_eq!(scanned_rows, 5);

        let runtime = engine
            .runtime_statistics
            .complete_table_statistics(&runtime_key)
            .expect("complete scan publishes runtime synopsis");
        assert_eq!(runtime.num_rows, Some(5));
        assert_eq!(runtime.column_statistics["id"].distinct_count, Some(5));

        let replanned_table = CatalogProvider::get_table(&catalog, "flights")
            .await
            .expect("read table metadata after runtime collection");
        let planner_statistics = replanned_table
            .statistics
            .as_ref()
            .expect("runtime synopsis is visible to binder and optimizer");
        assert_eq!(planner_statistics.num_rows, Some(5));
        assert_eq!(
            planner_statistics.column_statistics["id"].distinct_count,
            Some(5)
        );

        let query = engine
            .prepare("FROM flights |> SELECT id |> ORDER id + 1 DESC")
            .await
            .expect("prepare computed ORDER BY query");
        let mut stream = query.into_stream();
        let mut computed_order_ids = Vec::new();
        while let Some(batch) = stream.next().await {
            let batch = batch.expect("computed ORDER BY batch");
            assert_eq!(batch.num_columns(), 1, "temporary sort key must not escape");
            let ids_array = materialized_result_column(&batch, 0);
            let ids = ids_array
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("Int64 projected output");
            computed_order_ids.extend(ids.values().iter().copied());
        }
        assert_eq!(computed_order_ids, vec![5, 4, 3, 2, 1]);

        let query = engine
            .prepare(
                "FROM flights |> AGGREGATE count(id) AS flight_count GROUP BY id |> ORDER id DESC",
            )
            .await
            .expect("prepare ordered aggregate query");
        let mut stream = query.into_stream();
        let mut grouped_ids = Vec::new();
        while let Some(batch) = stream.next().await {
            let batch = batch.expect("aggregate query batch");
            let ids_array = materialized_result_column(&batch, 0);
            let ids = ids_array
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("Int64 grouping key");
            grouped_ids.extend(ids.values().iter().copied());
        }

        assert_eq!(grouped_ids, vec![5, 4, 3, 2, 1]);

        // Replacing a source in place must invalidate the reusable runtime
        // synopsis even when its table path and total row count stay unchanged.
        let replaced_path = table_dir.join("part-1.parquet");
        std::fs::remove_file(&replaced_path).expect("remove old source file");
        let replacement =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![7, 7]))])
                .expect("valid replacement batch");
        let file = File::create(&replaced_path).expect("recreate source file at same path");
        let mut writer =
            ArrowWriter::try_new(file, schema, None).expect("create replacement writer");
        writer.write(&replacement).expect("write replacement batch");
        writer.close().expect("close replacement writer");

        let changed_table = CatalogProvider::get_table(&catalog, "flights")
            .await
            .expect("read table after same-path replacement");
        let changed_snapshot = changed_table
            .catalog_snapshot
            .as_ref()
            .expect("catalog pins replacement source revision");
        assert_ne!(
            table.catalog_snapshot.as_ref(),
            Some(changed_snapshot),
            "source replacement must create a new runtime-statistics identity"
        );
        let changed_runtime_key =
            common::statistics::runtime::table_statistics_key(changed_snapshot);
        assert!(
            engine
                .runtime_statistics
                .complete_table_statistics(&changed_runtime_key)
                .is_none(),
            "a complete synopsis for the old source revision must not be reused"
        );
        assert_eq!(changed_table.statistics.as_ref().unwrap().num_rows, Some(5));
        assert_eq!(
            changed_table.statistics.as_ref().unwrap().column_statistics["id"].distinct_count,
            None,
            "the old five-distinct-value synopsis must not leak into the replacement"
        );
    }

    #[tokio::test]
    async fn skips_redundant_checkpoint_when_join_already_builds_filtered_input() {
        let root = tempfile::tempdir().expect("temporary data root");
        let left_dir = root.path().join("lefts");
        let right_dir = root.path().join("rights");
        std::fs::create_dir_all(&left_dir).expect("create left table directory");
        std::fs::create_dir_all(&right_dir).expect("create right table directory");

        let left_schema = Arc::new(Schema::new(vec![
            Field::new("left_key", DataType::Int64, false),
            Field::new("left_value", DataType::Utf8, false),
        ]));
        let left_batch = RecordBatch::try_new(
            Arc::clone(&left_schema),
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["one", "two"])),
            ],
        )
            .expect("valid left batch");
        let left_file = File::create(left_dir.join("part.parquet")).expect("create left file");
        let mut left_writer =
            ArrowWriter::try_new(left_file, left_schema, None).expect("create left writer");
        left_writer.write(&left_batch).expect("write left batch");
        left_writer.close().expect("close left writer");

        let right_schema = Arc::new(Schema::new(vec![
            Field::new("right_key", DataType::Int64, false),
            Field::new("right_value", DataType::Utf8, false),
        ]));
        let right_batch = RecordBatch::try_new(
            Arc::clone(&right_schema),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["one", "two", "three"])),
            ],
        )
            .expect("valid right batch");
        let right_file = File::create(right_dir.join("part.parquet")).expect("create right file");
        let mut right_writer =
            ArrowWriter::try_new(right_file, right_schema, None).expect("create right writer");
        right_writer.write(&right_batch).expect("write right batch");
        right_writer.close().expect("close right writer");

        let engine = LocalQueryEngine::new(root.path());
        let query = engine
            .prepare(
                "FROM lefts |> WHERE left_key > 0 |> JOIN rights ON left_key = right_key |> SELECT left_value, right_value",
            )
            .await
            .expect("prepare join with adaptive checkpoint");
        assert!(
            query.adaptive_checkpoint.is_none(),
            "the physical join already builds this filtered input before probing"
        );

        let mut stream = query.into_stream();
        let mut rows = Vec::new();
        while let Some(batch) = stream.next().await {
            let batch = batch.expect("continuation output batch");
            let left_array = materialized_result_column(&batch, 0);
            let left_values = left_array
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("left value output");
            let right_array = materialized_result_column(&batch, 1);
            let right_values = right_array
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("right value output");
            for row in 0..batch.num_rows() {
                rows.push((
                    left_values.value(row).to_owned(),
                    right_values.value(row).to_owned(),
                ));
            }
        }
        rows.sort();
        assert_eq!(
            rows,
            vec![("one".into(), "one".into()), ("two".into(), "two".into())]
        );
    }

    #[tokio::test]
    async fn parquet_projection_keeps_filter_columns_and_compacts_predicate_input() {
        let root = tempfile::tempdir().expect("temporary data root");
        let table_dir = root.path().join("wide");
        std::fs::create_dir_all(&table_dir).expect("create table directory");

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("unused", DataType::Utf8, false),
            Field::new("value", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["discard-a", "discard-b"])),
                Arc::new(Int64Array::from(vec![10, 42])),
            ],
        )
            .expect("valid batch");
        let file = File::create(table_dir.join("part.parquet")).expect("create Parquet file");
        let mut writer = ArrowWriter::try_new(file, schema, None).expect("create writer");
        writer.write(&batch).expect("write batch");
        writer.close().expect("close writer");

        let engine = LocalQueryEngine::new(root.path());
        let query = engine
            .prepare("FROM wide |> WHERE id = 2 |> SELECT value")
            .await
            .expect("prepare projected scan");
        let scan = find_file_scan(&query.tasks[0].plan.root)
            .expect("expected a Parquet file scan in the physical plan");
        assert_eq!(
            scan.projected
                .fields
                .iter()
                .map(|column| column.name.as_str())
                .collect::<Vec<_>>(),
            vec!["id", "value"],
        );

        let mut stream = query.into_stream();
        let batch = stream
            .next()
            .await
            .expect("query batch")
            .expect("successful scan");
        let values_array = materialized_result_column(&batch, 0);
        let values = values_array
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Int64 output");
        assert_eq!(values.values(), &[42]);
        assert!(stream.next().await.is_none());

        let write = engine
            .prepare("FROM wide |> WHERE id = 2 |> SELECT value |> WRITE narrowed")
            .await
            .expect("prepare projected WRITE");
        let receipt = write.execute_write().await.expect("commit projected WRITE");
        assert_eq!(receipt.row_count, 1);

        let catalog = ParquetCatalog::new(root.path());
        let metadata = CatalogProvider::get_table(&catalog, "narrowed")
            .await
            .expect("read projected output metadata");
        assert_eq!(metadata.schema.fields.len(), 1);
        assert_eq!(metadata.schema.fields[0].name, "value");
    }

    #[tokio::test]
    async fn runtime_synopsis_uses_complete_rows_with_projected_column_coverage() {
        let root = tempfile::tempdir().expect("temporary data root");
        let table_dir = root.path().join("events");
        std::fs::create_dir_all(&table_dir).expect("create table directory");
        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Int64, false),
            Field::new("payload", DataType::Utf8, false),
        ]));
        let input = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
                Arc::new(StringArray::from(vec!["a", "b", "c", "d"])),
            ],
        )
            .expect("valid input batch");
        let file = File::create(table_dir.join("part.parquet")).expect("create Parquet file");
        let mut writer = ArrowWriter::try_new(file, schema, None).expect("create writer");
        writer.write(&input).expect("write input batch");
        writer.close().expect("close writer");

        let engine = LocalQueryEngine::new(root.path());
        let query = engine
            .prepare("FROM events |> SELECT key")
            .await
            .expect("prepare query");
        let scan = find_file_scan(&query.tasks[0].plan.root).expect("find file scan");
        let statistics_scan = scan
            .runtime_statistics
            .as_ref()
            .expect("statistics context");
        assert!(statistics_scan.row_coverage_candidate);
        let table_key = statistics_scan.table_key.clone();
        assert_eq!(
            scan.projected.fields.len(),
            1,
            "only the selected column is scanned"
        );

        let mut stream = query.into_stream();
        let mut row_count = 0;
        while let Some(batch) = stream.next().await {
            row_count += batch.expect("query batch").num_rows();
        }
        assert_eq!(row_count, 4);

        let statistics = engine
            .runtime_statistics
            .complete_table_statistics(&table_key)
            .expect("background synopsis is visible to the next plan");
        assert_eq!(statistics.num_rows, Some(4));
        assert_eq!(statistics.column_statistics.len(), 1);
        assert_eq!(statistics.column_statistics["key"].distinct_count, Some(4));
    }

    #[tokio::test]
    async fn executes_lower_and_round_scalar_projections_preserving_nulls() {
        let root = tempfile::tempdir().expect("temporary data root");
        let table_dir = root.path().join("names");
        std::fs::create_dir_all(&table_dir).expect("create table directory");

        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("amount", DataType::Float64, true),
            Field::new("order_date", DataType::Date32, true),
            Field::new(
                "left_vector",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 2),
                true,
            ),
            Field::new(
                "right_vector",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 2),
                true,
            ),
        ]));
        let vector_field = Arc::new(Field::new("item", DataType::Float32, true));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec![Some("MiXeD"), None, Some("VeIn")])),
                Arc::new(Float64Array::from(vec![Some(1.2), Some(1.5), Some(-1.5)])),
                Arc::new(arrow::array::Date32Array::from(vec![
                    Some(0),
                    Some(19_723),
                    None,
                ])),
                Arc::new(
                    FixedSizeListArray::try_new(
                        vector_field.clone(),
                        2,
                        Arc::new(Float32Array::from(vec![1.0, 0.0, 0.0, 1.0, 1.0, 0.0])),
                        Some(vec![true, true, false].into()),
                    )
                        .expect("valid left vectors"),
                ),
                Arc::new(
                    FixedSizeListArray::try_new(
                        vector_field,
                        2,
                        Arc::new(Float32Array::from(vec![0.0, 1.0, 0.0, 1.0, 1.0, 0.0])),
                        None,
                    )
                        .expect("valid right vectors"),
                ),
            ],
        )
            .expect("valid batch");
        let file = File::create(table_dir.join("part.parquet")).expect("create Parquet file");
        let mut writer = ArrowWriter::try_new(file, schema, None).expect("create writer");
        writer.write(&batch).expect("write batch");
        writer.close().expect("close writer");

        let engine = LocalQueryEngine::new(root.path());
        let query = engine
            .prepare("FROM names |> SELECT LOWER(name) AS lowered")
            .await
            .expect("prepare LOWER projection");
        let mut stream = query.into_stream();
        let result = stream
            .next()
            .await
            .expect("query batch")
            .expect("successful LOWER projection");
        let values_array = materialized_result_column(&result, 0);
        let values = values_array
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("UTF-8 output");
        assert_eq!(values.value(0), "mixed");
        assert!(values.is_null(1));
        assert_eq!(values.value(2), "vein");
        assert!(stream.next().await.is_none());

        let query = engine
            .prepare("FROM names |> SELECT ROUND(amount) AS rounded")
            .await
            .expect("prepare ROUND projection");
        let mut stream = query.into_stream();
        let result = stream
            .next()
            .await
            .expect("query batch")
            .expect("successful ROUND projection");
        let values_array = materialized_result_column(&result, 0);
        let values = values_array
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("Float64 output");
        assert_eq!(values.value(0), 1.0);
        assert_eq!(values.value(1), 2.0);
        assert_eq!(values.value(2), -2.0);
        assert!(stream.next().await.is_none());

        let query = engine
            .prepare(
                "FROM names |> SELECT EXTRACT(YEAR FROM order_date) AS year, YEAR(order_date) AS year_alias",
            )
            .await
            .expect("prepare EXTRACT and YEAR projections");
        let mut stream = query.into_stream();
        let result = stream
            .next()
            .await
            .expect("query batch")
            .expect("successful temporal projections");
        let year_array = materialized_result_column(&result, 0);
        let years = year_array
            .as_any()
            .downcast_ref::<arrow::array::Int32Array>()
            .expect("Int32 output");
        assert_eq!(years.value(0), 1970);
        assert_eq!(years.value(1), 2024);
        assert!(years.is_null(2));
        let year_alias_array = materialized_result_column(&result, 1);
        let year_alias = year_alias_array
            .as_any()
            .downcast_ref::<arrow::array::Int32Array>()
            .expect("Int32 YEAR alias output");
        assert_eq!(year_alias, years);
        assert!(stream.next().await.is_none());

        let query = engine
            .prepare(
                "FROM names |> WHERE order_date = DATE '1970-01-01' |> EXTEND TRUNCATE(order_date, 'MONTH') AS month_start |> SELECT month_start, TRUNCATE(TIMESTAMP '2024-03-31 12:34:56', 'HOUR') AS hour_start",
            )
            .await
            .expect("prepare temporal EXTEND and timestamp literal");
        let mut stream = query.into_stream();
        let result = stream
            .next()
            .await
            .expect("query batch")
            .expect("successful temporal EXTEND and timestamp literal");
        let month_start_array = materialized_result_column(&result, 0);
        let month_start = month_start_array
            .as_any()
            .downcast_ref::<arrow::array::Date32Array>()
            .expect("Date32 TRUNCATE output");
        assert_eq!(month_start.value(0), 0);
        let hour_start_array = materialized_result_column(&result, 1);
        let hour_start = hour_start_array
            .as_any()
            .downcast_ref::<arrow::array::TimestampMicrosecondArray>()
            .expect("timestamp TRUNCATE output");
        assert_eq!(hour_start.value(0), 1_711_886_400_000_000);
        assert!(stream.next().await.is_none());

        let query = engine
            .prepare("FROM names |> SELECT VECTOR_DISTANCE(left_vector, right_vector) AS distance")
            .await
            .expect("prepare VECTOR_DISTANCE projection");
        let mut stream = query.into_stream();
        let result = stream
            .next()
            .await
            .expect("query batch")
            .expect("successful VECTOR_DISTANCE projection");
        let distance_array = materialized_result_column(&result, 0);
        let distances = distance_array
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("Float64 output");
        assert_eq!(distances.value(0), 1.0);
        assert_eq!(distances.value(1), 0.0);
        assert!(distances.is_null(2));
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn executes_pipe_write_and_exposes_only_committed_rows() {
        let root = tempfile::tempdir().expect("temporary data root");
        let table_dir = root.path().join("source");
        std::fs::create_dir_all(&table_dir).expect("create source directory");
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![4, 5, 6]))],
        )
            .expect("valid source batch");
        let file =
            File::create(table_dir.join("part.parquet")).expect("create source Parquet file");
        let mut writer = ArrowWriter::try_new(file, schema, None).expect("create source writer");
        writer.write(&batch).expect("write source batch");
        writer.close().expect("close source writer");

        let engine = LocalQueryEngine::new(root.path());
        let error = match engine
            .prepare("FROM source |> SELECT id |> WRITE archived (compression='zstd')")
            .await
        {
            Ok(_) => panic!("unsupported writer options should fail preparation"),
            Err(error) => error,
        };
        assert!(
            error
                .to_string()
                .contains("does not support option 'compression'")
        );

        let query = engine
            .prepare("FROM source |> WHERE id > 4 |> SELECT id |> WRITE archived")
            .await
            .expect("prepare WRITE query");
        assert!(query.is_write());
        let receipt = query.execute_write().await.expect("commit WRITE query");
        assert_eq!(receipt.destination, "archived");
        assert_eq!(receipt.row_count, 2);

        let catalog = ParquetCatalog::new(root.path());
        let metadata = CatalogProvider::get_table(&catalog, "archived")
            .await
            .expect("read committed output table");
        assert_eq!(metadata.files.len(), 1);
        assert_eq!(metadata.schema.fields[0].name, "id");
    }

    #[tokio::test]
    async fn analyzes_external_parquet_tables_and_rejects_stale_sidecars() {
        let root = tempfile::tempdir().expect("temporary data root");
        let table_dir = root.path().join("external_events");
        std::fs::create_dir_all(&table_dir).expect("create external table directory");
        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Int64, true),
            Field::new("label", DataType::Utf8, true),
        ]));
        let path = table_dir.join("part.parquet");
        let initial = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![
                    Some(1),
                    Some(1),
                    Some(2),
                    Some(2),
                    None,
                ])),
                Arc::new(StringArray::from(vec![
                    Some("A"),
                    Some("A"),
                    Some("B"),
                    Some("A"),
                    None,
                ])),
            ],
        )
            .expect("valid external batch");
        let file = File::create(&path).expect("create external Parquet file");
        let mut writer = ArrowWriter::try_new(file, Arc::clone(&schema), None)
            .expect("create external Parquet writer");
        writer.write(&initial).expect("write external batch");
        writer.close().expect("close external Parquet writer");

        let engine = LocalQueryEngine::new(root.path());
        let config = StatisticsConfig {
            scan_batch_size: 2,
            most_common_capacity: 4,
            histogram_sample_size: 8,
            histogram_bucket_count: 2,
            multi_column_sets: vec![vec!["label".to_owned(), "key".to_owned()]],
            ..StatisticsConfig::default()
        };
        let analyzed = engine
            .analyze_table_with_config("external_events", config.clone())
            .await
            .expect("analyze external Parquet table");
        assert_eq!(analyzed.statistics.num_rows, Some(5));
        assert_eq!(
            analyzed.statistics.column_groups[0].columns,
            vec!["key", "label"]
        );
        assert_eq!(analyzed.statistics.column_groups[0].distinct_count, Some(4));
        assert_eq!(
            analyzed.statistics.column_statistics["key"]
                .histogram
                .as_ref()
                .unwrap()
                .population_count,
            4
        );

        let catalog = ParquetCatalog::new(root.path());
        let loaded = CatalogProvider::get_table(&catalog, "external_events")
            .await
            .expect("load analyzed external table");
        assert_eq!(
            loaded.statistics.as_ref().unwrap().column_groups[0].distinct_count,
            Some(4)
        );
        assert!(
            loaded.statistics.as_ref().unwrap().column_statistics["key"]
                .most_common_values
                .is_some()
        );

        std::fs::remove_file(&path).expect("replace source file");
        let replacement = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![
                    Some(7),
                    Some(7),
                    Some(7),
                    Some(8),
                    Some(9),
                    Some(10),
                ])),
                Arc::new(StringArray::from(vec![
                    Some("X"),
                    Some("X"),
                    Some("X"),
                    Some("Y"),
                    Some("Z"),
                    Some("W"),
                ])),
            ],
        )
            .expect("valid replacement batch");
        let file = File::create(&path).expect("recreate external Parquet file");
        let mut writer =
            ArrowWriter::try_new(file, schema, None).expect("create replacement Parquet writer");
        writer.write(&replacement).expect("write replacement batch");
        writer.close().expect("close replacement Parquet writer");

        let changed = CatalogProvider::get_table(&catalog, "external_events")
            .await
            .expect("load changed external table");
        let changed_stats = changed.statistics.as_ref().unwrap();
        assert_eq!(changed_stats.num_rows, Some(6));
        assert_eq!(changed_stats.column_statistics["key"].distinct_count, None);
        assert!(
            changed_stats.column_statistics["key"]
                .most_common_values
                .is_none()
        );

        let refreshed = engine
            .analyze_table_with_config("external_events", config)
            .await
            .expect("reanalyze changed external table");
        assert_eq!(refreshed.statistics.num_rows, Some(6));
        assert_eq!(
            refreshed.statistics.column_statistics["key"]
                .most_common_values
                .as_ref()
                .unwrap()
                .analyzed_count,
            6
        );
    }
}
