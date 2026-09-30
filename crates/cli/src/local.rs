use std::pin::Pin;
use std::sync::Arc;
use std::time::Instant;

use arrow::record_batch::RecordBatch;
use binder::LogicalBinder;
use common::cancellation::CancellationToken;
use common::identifiers::RequestId;
use common::protocol::{DataSink, ExecuteTask};
use common::{Result, VeinError};
use exec::runtime::TaskExecutor;
use exec::operators::sink::WriteReceipt;
use futures::Stream;
use optimizer::Optimizer;
use physical::PhysicalPlanner;
use plan::CatalogProvider;

use super::parquet_catalog::ParquetCatalog;
use super::parquet_storage::ParquetStorage;

const OUTPUT_BUFFER_BATCHES: usize = 2;

/// A local query engine that plans against Parquet metadata and runs tasks in-process.
pub struct LocalQueryEngine {
    binder: binder::QueryBinder,
    optimizer: optimizer::OptimizerPipeline,
    planner: physical::DefaultPhysicalPlanner,
    executor: Arc<dyn TaskExecutor>,
}

impl LocalQueryEngine {
    /// Creates an engine whose table names resolve beneath `data_root`.
    pub fn new(data_root: impl Into<std::path::PathBuf>) -> Self {
        let data_root = data_root.into();
        let catalog = Arc::new(ParquetCatalog::new(data_root.clone()));
        let metadata_store = catalog.metadata_store();
        let catalog: Arc<dyn CatalogProvider> = catalog;
        let functions = Arc::new(binder::BuiltinFunctionRegistry::new());
        let functions: Arc<dyn binder::FunctionRegistry> = functions;
        let storage = Arc::new(ParquetStorage::new(data_root));
        let executor = Arc::new(exec::factory::TaskExecutorBuilder::new()
            .with_metadata_store(metadata_store)
            .with_write_storage(storage)
            .build());

        Self {
            binder: binder::QueryBinder::new(catalog, functions),
            optimizer: optimizer::OptimizerPipeline::default(),
            planner: physical::DefaultPhysicalPlanner::new(),
            executor,
        }
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

        let optimize_started = Instant::now();
        let optimized = self.optimizer.optimize(bound.plan)?;
        let optimize_us = optimize_started.elapsed().as_micros() as u64;

        let plan_started = Instant::now();
        let request_id = RequestId::new();
        let job = self.planner.create_physical_plan(request_id, optimized)?;
        let plan_us = plan_started.elapsed().as_micros() as u64;

        let task_started = Instant::now();
        let (request_id, query_id, mode, stage) = job.into_parts();
        let is_write = stage
            .tasks
            .first()
            .is_some_and(|task| matches!(&*task.sink, DataSink::Storage(_)));
        if is_write && stage.tasks.len() != 1 {
            return Err(VeinError::planning("a local WRITE must execute as one atomic task", None));
        }
        let stage_id = stage.stage_id;
        let mut tasks = Vec::with_capacity(stage.tasks.len());
        for task in stage.tasks {
            match (&*task.sink, is_write) {
                (DataSink::Client { .. }, false) | (DataSink::Storage(_), true) => {}
                _ => return Err(VeinError::planning("physical stage contains inconsistent sink types", None)),
            }
            tasks.push(task.into_execute_task(request_id, query_id, stage_id, mode));
        }
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
            is_write,
        })
    }
}

/// A planned local query whose output can be consumed incrementally.
pub struct LocalQuery {
    tasks: Vec<ExecuteTask>,
    executor: Arc<dyn TaskExecutor>,
    is_write: bool,
}

impl LocalQuery {
    /// Whether this prepared query commits data to a storage target.
    pub fn is_write(&self) -> bool {
        self.is_write
    }

    /// Execute a storage write and return only after its commit is confirmed.
    pub async fn execute_write(self) -> Result<WriteReceipt> {
        if !self.is_write || self.tasks.len() != 1 {
            return Err(VeinError::planning("prepared query is not a single-task WRITE", None));
        }
        let result = self.executor.execute(&self.tasks[0], CancellationToken::new()).await?;
        result.write_receipt.ok_or_else(|| VeinError::internal("WRITE task completed without a commit receipt"))
    }

    /// Runs the query and yields Arrow batches through a bounded channel.
    pub fn into_stream(self) -> LocalResultStream {
        Box::pin(async_stream::stream! {
            for task in self.tasks {
                let (sender, receiver) = flume::bounded(OUTPUT_BUFFER_BATCHES);
                let mut execution = Box::pin(self.executor.execute_streaming(
                    &task,
                    CancellationToken::new(),
                    sender,
                ));

                loop {
                    tokio::select! {
                        result = &mut execution => {
                            match result {
                                Ok(_) => {
                                    while let Ok(batch) = receiver.try_recv() {
                                        yield Ok(batch);
                                    }
                                }
                                Err(error) => yield Err(error),
                            }
                            break;
                        }
                        received = receiver.recv_async() => {
                            match received {
                                Ok(batch) => yield Ok(batch),
                                Err(_) => {
                                    if let Err(error) = execution.await {
                                        yield Err(error);
                                    }
                                    break;
                                }
                            }
                        }
                    }
                }
            }
        })
    }
}

/// A streamed sequence of local query batches.
pub type LocalResultStream = Pin<Box<dyn Stream<Item=Result<RecordBatch>>>>;

#[cfg(test)]
mod tests {
    use std::fs::File;

    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use futures::StreamExt;
    use parquet::arrow::ArrowWriter;

    use super::*;

    #[tokio::test]
    async fn executes_a_local_parquet_query_incrementally() {
        let root = tempfile::tempdir().expect("temporary data root");
        let table_dir = root.path().join("flights");
        std::fs::create_dir_all(&table_dir).expect("create table directory");

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
        )
            .expect("valid batch");
        let file = File::create(table_dir.join("part.parquet")).expect("create Parquet file");
        let mut writer = ArrowWriter::try_new(file, schema, None).expect("create writer");
        writer.write(&batch).expect("write batch");
        writer.close().expect("close writer");

        let engine = LocalQueryEngine::new(root.path());
        let query = engine
            .prepare("FROM flights |> WHERE id > 1 |> SELECT id")
            .await
            .expect("prepare local query");
        let mut stream = query.into_stream();
        let mut rows = Vec::new();
        while let Some(batch) = stream.next().await {
            let batch = batch.expect("query batch");
            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("Int64 output");
            rows.extend(ids.values().iter().copied());
        }

        assert_eq!(rows, vec![2, 3]);
    }

    #[tokio::test]
    async fn parquet_projection_keeps_filter_columns_and_compacts_vm_registers() {
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
        let common::protocol::DataSource::File(scan) = &query.tasks[0].source else {
            panic!("expected a Parquet file scan");
        };
        assert_eq!(
            scan.projected_schema
                .iter()
                .map(|field| field.name.as_str())
                .collect::<Vec<_>>(),
            vec!["id", "value"],
        );

        let mut stream = query.into_stream();
        let batch = stream
            .next()
            .await
            .expect("query batch")
            .expect("successful scan");
        let values = batch
            .column(0)
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
        let file = File::create(table_dir.join("part.parquet")).expect("create source Parquet file");
        let mut writer = ArrowWriter::try_new(file, schema, None).expect("create source writer");
        writer.write(&batch).expect("write source batch");
        writer.close().expect("close source writer");

        let engine = LocalQueryEngine::new(root.path());
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
}
