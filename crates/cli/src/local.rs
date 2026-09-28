use std::pin::Pin;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use binder::LogicalBinder;
use common::cancellation::CancellationToken;
use common::identifiers::RequestId;
use common::protocol::{DataSink, ExecuteTask};
use common::{Result, VeinError};
use exec::runtime::TaskExecutor;
use futures::Stream;
use optimizer::Optimizer;
use physical::PhysicalPlanner;

use super::parquet_catalog::ParquetCatalog;

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
        let catalog = Arc::new(ParquetCatalog::new(data_root));
        let catalog: Arc<dyn plan::CatalogProvider> = catalog;
        let functions = Arc::new(binder::BuiltinFunctionRegistry::new());
        let functions: Arc<dyn binder::FunctionRegistry> = functions;
        let executor = Arc::new(exec::factory::TaskExecutorBuilder::new().build());

        Self {
            binder: binder::QueryBinder::new(catalog, functions),
            optimizer: optimizer::OptimizerPipeline::default(),
            planner: physical::DefaultPhysicalPlanner::new(),
            executor,
        }
    }

    /// Parses, binds, optimizes, and plans a Pipe query for local execution.
    pub async fn prepare(&self, query: &str) -> Result<LocalQuery> {
        let mut parser = parser::Parser::new(query);
        let statement = parser
            .parse_statement()
            .map_err(|error| VeinError::planning(format!("query parse failed: {error}"), None))?;
        let bound = self.binder.bind_with_catalog_revision(&statement).await?;
        let optimized = self.optimizer.optimize(bound.plan)?;
        let request_id = RequestId::new();
        let job = self.planner.create_physical_plan(request_id, optimized)?;

        let stage = job.stage();
        let mut tasks = Vec::with_capacity(stage.tasks().len());
        for task in stage.tasks() {
            if !matches!(task.sink, DataSink::Client { .. }) {
                return Err(VeinError::planning(
                    "local query execution currently supports read results only",
                    None,
                ));
            }
            tasks.push(task.clone().into_execute_task(
                job.request_id,
                job.query_id,
                stage.stage_id,
                job.mode().clone(),
            ));
        }

        Ok(LocalQuery {
            tasks,
            executor: self.executor.clone(),
        })
    }
}

/// A planned local query whose output can be consumed incrementally.
pub struct LocalQuery {
    tasks: Vec<ExecuteTask>,
    executor: Arc<dyn TaskExecutor>,
}

impl LocalQuery {
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
pub type LocalResultStream = Pin<Box<dyn Stream<Item = Result<RecordBatch>>>>;

#[cfg(test)]
mod tests {
    use std::fs::File;

    use arrow::array::Int64Array;
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
}
