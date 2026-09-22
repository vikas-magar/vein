use async_trait::async_trait;
use common::{Result, ResultEvent, SendableResultStream, bounded_result_channel};
use futures::StreamExt;
use std::sync::Arc;

use binder::LogicalBinder;
use distributed::{
    ArtifactId as DistributedArtifactId, ArtifactRef as DistributedArtifactRef,
    SnapshotId as DistributedSnapshotId, TableId as DistributedTableId,
    TableSnapshot as DistributedTableSnapshot, TableWriteMode, WriteId,
};
use optimizer::Optimizer;
use parser::ast::query::QueryNode;
use physical::PhysicalPlanner;
use plan::LogicalPlan;
use plan::display;
use tracing::{Instrument, info_span};

/// The result of executing a bound query.
pub enum QueryOutput {
    /// A distributed result stream that owns its bounded execution lifecycle.
    Data {
        query_id: common::identifiers::QueryId,
        stream: SendableResultStream,
    },
    /// A rendered EXPLAIN plan.
    Plan(String),
    /// A successful write operation.
    Write {
        /// Destination table name.
        table: String,
        /// Rows written when known.
        row_count: u64,
    },
}

/// The absolute highest-level abstraction for the database engine.
#[async_trait]
pub trait QueryEngine: Send + Sync {
    /// Execute a parsed pipe query and return a typed result.
    async fn submit_query<'a>(&self, node: QueryNode<'a>) -> Result<QueryOutput>;

    /// Optional access to the executor registry (used by Master daemon to record heartbeats).
    fn registry(&self) -> Option<Arc<orchestrator::executor::ExecutorRegistry>> {
        None
    }
}

/// The concrete Coordinator that wires all the abstract traits together.
/// It does not contain any hardcoded logic—it purely orchestrates the injected trait implementations.
pub struct DistributedCoordinator {
    /// Abstract layer for binding ASTs to semantic schemas
    pub binder: Arc<dyn LogicalBinder>,
    /// Abstract layer for all logical plan optimization
    pub optimizer: Arc<dyn Optimizer>,
    /// Abstract layer for lowering Logical Plans to Physical DAGs
    pub planner: Arc<dyn PhysicalPlanner>,
    /// The cluster scheduler responsible for dispatching tasks
    pub scheduler: Arc<orchestrator::scheduler::Scheduler>,
    /// Authoritative catalog writer used to publish completed writes.
    pub catalog_writer: Option<Arc<dyn vcs::CatalogWriter>>,
    /// Authoritative atomic table-generation writer.
    pub table_write_catalog: Option<Arc<dyn vcs::TableWriteCatalog>>,
}

impl DistributedCoordinator {
    /// Creates a new builder for the DistributedCoordinator.
    pub fn builder() -> DistributedCoordinatorBuilder {
        DistributedCoordinatorBuilder {
            binder: None,
            optimizer: None,
            planner: None,
            scheduler: None,
            catalog_writer: None,
            table_write_catalog: None,
        }
    }
}

/// A builder for constructing a `DistributedCoordinator` with fluent DI configuration.
pub struct DistributedCoordinatorBuilder {
    binder: Option<Arc<dyn LogicalBinder>>,
    optimizer: Option<Arc<dyn Optimizer>>,
    planner: Option<Arc<dyn PhysicalPlanner>>,
    scheduler: Option<Arc<orchestrator::scheduler::Scheduler>>,
    catalog_writer: Option<Arc<dyn vcs::CatalogWriter>>,
    table_write_catalog: Option<Arc<dyn vcs::TableWriteCatalog>>,
}

impl DistributedCoordinatorBuilder {
    /// Injects the LogicalBinder strategy.
    pub fn with_binder(mut self, binder: Arc<dyn LogicalBinder>) -> Self {
        self.binder = Some(binder);
        self
    }

    /// Injects the Optimizer strategy.
    pub fn with_optimizer(mut self, optimizer: Arc<dyn Optimizer>) -> Self {
        self.optimizer = Some(optimizer);
        self
    }

    /// Injects the PhysicalPlanner strategy.
    pub fn with_planner(mut self, planner: Arc<dyn PhysicalPlanner>) -> Self {
        self.planner = Some(planner);
        self
    }

    /// Injects the Scheduler.
    pub fn with_scheduler(mut self, scheduler: Arc<orchestrator::scheduler::Scheduler>) -> Self {
        self.scheduler = Some(scheduler);
        self
    }

    /// Injects the authoritative catalog writer used by write queries.
    pub fn with_catalog_writer(mut self, catalog_writer: Arc<dyn vcs::CatalogWriter>) -> Self {
        self.catalog_writer = Some(catalog_writer);
        self
    }

    /// Injects the authoritative table-generation transaction service.
    pub fn with_table_write_catalog(
        mut self,
        table_write_catalog: Arc<dyn vcs::TableWriteCatalog>,
    ) -> Self {
        self.table_write_catalog = Some(table_write_catalog);
        self
    }

    /// Validates dependencies and constructs the `DistributedCoordinator`.
    pub fn build(self) -> Result<DistributedCoordinator> {
        let binder = self
            .binder
            .ok_or_else(|| error::VeinError::internal("LogicalBinder must be injected"))?;
        let optimizer = self
            .optimizer
            .ok_or_else(|| error::VeinError::internal("Optimizer must be injected"))?;
        let planner = self
            .planner
            .ok_or_else(|| error::VeinError::internal("PhysicalPlanner must be injected"))?;
        let scheduler = self
            .scheduler
            .ok_or_else(|| error::VeinError::internal("Scheduler must be injected"))?;

        Ok(DistributedCoordinator {
            binder,
            optimizer,
            planner,
            scheduler,
            catalog_writer: self.catalog_writer,
            table_write_catalog: self.table_write_catalog,
        })
    }
}

impl DistributedCoordinator {
    fn stream_job(&self, job: physical::Job) -> SendableResultStream {
        let request_id = job.request_id;
        let query_id = job.query_id;
        let scheduler = self.scheduler.clone();
        let dispatcher = scheduler.dispatcher().clone();
        let upstream = dispatcher.open_result_stream(query_id);
        let (sender, stream) = bounded_result_channel(32);
        let cancel = common::cancellation::CancellationToken::new();
        let execution_cancel = cancel.clone();

        let query_span = info_span!(
            target: "vein.audit",
            "query_execution",
            request_id = %request_id,
            query_id = %query_id
        );
        tokio::spawn(
            async move {
                let mut upstream = upstream;
                let mut execution = Box::pin(scheduler.execute(job, execution_cancel));
                let mut execution_result = None;
                let mut upstream_done = false;

                while execution_result.is_none() || !upstream_done {
                    tokio::select! {
                        result = &mut execution, if execution_result.is_none() => {
                            dispatcher.close_result_stream(query_id);
                            execution_result = Some(result);
                        }
                        event = upstream.next(), if !upstream_done => {
                            match event {
                                Some(event) => {
                                    if sender.send(event).await.is_err() {
                                        cancel.cancel();
                                        dispatcher.close_result_stream(query_id);
                                        return;
                                    }
                                }
                                None => upstream_done = true,
                            }
                        }
                    }
                }

                if let Some(Err(error)) = execution_result {
                    tracing::error!(
                        target: "vein.audit",
                        event = "result_stream_failed",
                        error = %error,
                        "result_stream_failed"
                    );
                    let _ = sender
                        .send(Ok(ResultEvent::Error {
                            message: error.to_string(),
                        }))
                        .await;
                } else {
                    tracing::info!(
                        target: "vein.audit",
                        event = "result_stream_completed",
                        "result_stream_completed"
                    );
                }
            }
            .instrument(query_span),
        );

        stream
    }
}

#[async_trait]
impl QueryEngine for DistributedCoordinator {
    fn registry(&self) -> Option<Arc<orchestrator::executor::ExecutorRegistry>> {
        Some(self.scheduler.registry().clone())
    }

    async fn submit_query<'a>(&self, node: QueryNode<'a>) -> Result<QueryOutput> {
        let request_id = common::identifiers::RequestId::new();
        let query_span = info_span!(
            target: "vein.audit",
            "query_request",
            request_id = %request_id
        );
        async move {
        tracing::info!(
            target: "vein.audit",
            event = "query_lifecycle_started",
            "query_lifecycle_started"
        );
        tracing::debug!(target: "vein_coordinator", "Binding SQL AST to semantic logical plan");
        let bound_query = self.binder.bind_with_catalog_revision(&node).await?;
        let logical_plan = bound_query.plan;

        tracing::debug!(target: "vein_coordinator", "Applying rule-based plan optimizations");
        let mut optimized_plan = self.optimizer.optimize(logical_plan)?;

        if let LogicalPlan::Explain { .. } = &optimized_plan {
            let text = display::format_plan(&optimized_plan);
            tracing::info!(target: "vein_coordinator", "EXPLAIN statement evaluated; returning rendered plan");
            return Ok(QueryOutput::Plan(text));
        }

        let write_info = match &mut optimized_plan {
            LogicalPlan::Write {
                target,
                options,
                schema,
                ..
            } => {
                let mode = match options
                    .iter()
                    .find(|(key, _)| key.eq_ignore_ascii_case("mode"))
                    .map(|(_, value)| value.as_str())
                    .unwrap_or("overwrite")
                    .to_ascii_lowercase()
                    .as_str()
                {
                    "overwrite" => TableWriteMode::Overwrite,
                    "append" => TableWriteMode::Append,
                    value => {
                        return Err(error::VeinError::semantic(
                            format!("unsupported WRITE mode '{value}'"),
                            None,
                        ));
                    }
                };
                let write_id = WriteId::from(format!("write:{request_id}"));
                let staging_table = format!("__vein_stage_{}", request_id);
                options.push(("write_id".to_string(), write_id.to_string()));
                options.push(("staging_table".to_string(), staging_table.clone()));
                options.push((
                    "write_mode".to_string(),
                    match mode {
                        TableWriteMode::Overwrite => "overwrite".to_string(),
                        TableWriteMode::Append => "append".to_string(),
                    },
                ));
                Some((
                    target.clone(),
                    schema.clone(),
                    write_id,
                    staging_table,
                    mode,
                ))
            }
            _ => None,
        };

        tracing::debug!(target: "vein_coordinator", "Lowering optimized logical plan to physical DAG");
        let job = self
            .planner
            .create_physical_plan(request_id, optimized_plan)?;
        let query_id = job.query_id;
        let output_stage = job.output_stage_id();
        tracing::info!(target: "vein_coordinator", ?output_stage, "Dispatching physical job to cluster scheduler");

        if let Some((target, schema, write_id, staging_table, mode)) = write_info {
            if self.table_write_catalog.is_none() {
                let dispatcher = self.scheduler.dispatcher().clone();
                let _result_stream = dispatcher.open_result_stream(query_id);
                let execution_result = self
                    .scheduler
                    .execute(job, common::cancellation::CancellationToken::new())
                    .await;
                dispatcher.close_result_stream(query_id);
                let completion = execution_result?;
                let vfs_store = vfs::VfsTableStore::shared_or_default()
                    .map_err(|error| error::VeinError::internal(error.to_string()))?;
                vfs_store
                    .publish_staged_table(
                        &staging_table,
                        &target,
                        mode == TableWriteMode::Overwrite,
                    )
                    .await
                    .map_err(|error| error::VeinError::internal(error.to_string()))?;

             let location = if target.starts_with('/')
                    || target.starts_with("./")
                    || target.starts_with("../")
                    || target.starts_with("vfs://")
                {
                    target.clone()
                } else {
                    format!("vfs://{target}")
                };
                let catalog = self.catalog_writer.as_ref().ok_or_else(|| {
                    error::VeinError::internal("catalog writer is required for local WRITE queries")
                })?;
                let metadata = plan::TableMetadata::new(&target, Arc::new((*schema).clone()))
                    .with_location(location)
                    .with_format(plan::TableFormat::Exo);
                catalog.register_table(metadata).await?;
                return Ok(QueryOutput::Write {
                    table: target,
                    row_count: completion.row_count,
                });
            }
            let table_write_catalog = self
                .table_write_catalog
                .as_ref()
                .expect("table write catalog checked above");
            let expected_parts = job
                .get_stage(output_stage)
                .map(|stage| stage.tasks().len() as u32)
                .filter(|count| *count > 0)
                .ok_or_else(|| error::VeinError::internal("WRITE output stage has no tasks"))?;
            let prepared = table_write_catalog
                .prepare_table_write(
                    write_id.clone(),
                    DistributedTableId::from(target.clone()),
                    mode,
                    expected_parts,
                )
                .await?;
            let dispatcher = self.scheduler.dispatcher().clone();
            let _result_stream = dispatcher.open_result_stream(query_id);
            let execution_result = self
                .scheduler
                .execute(job, common::cancellation::CancellationToken::new())
                .await;
            dispatcher.close_result_stream(query_id);
            let completion = match execution_result {
                Ok(completion) => completion,
                Err(error) => {
                    let _ = table_write_catalog
                        .abort_table_write(prepared.write_id.clone())
                        .await;
                    return Err(error);
                }
            };

            let staged_parts = completion.write_parts.clone();

            if staged_parts.len() != expected_parts as usize {
                let _ = table_write_catalog
                    .abort_table_write(prepared.write_id.clone())
                    .await;
                return Err(error::VeinError::internal(format!(
                    "WRITE produced {} staged partitions, expected {}",
                    staged_parts.len(),
                    expected_parts
                )));
            }

            for part in staged_parts {
                table_write_catalog
                    .publish_table_write_part(prepared.write_id.clone(), part)
                    .await?;
            }


            let location = format!("vfs://{staging_table}");
            let metadata = plan::TableMetadata::new(&target, Arc::new((*schema).clone()))
                .with_location(location)
                .with_format(plan::TableFormat::Exo);
            let published_bytes = completion
                .write_parts
                .iter()
                .map(|part| part.artifact.size_bytes)
                .sum::<u64>();
            let schema_fingerprint = metadata
                .schema
                .fields
                .iter()
                .map(|field| format!("{}:{:?}", field.name, field.data_type))
                .collect::<Vec<_>>()
                .join("|");
            let snapshot = DistributedTableSnapshot {
                id: DistributedSnapshotId::from(format!("{}@{}", target, prepared.generation)),
                manifest: DistributedArtifactRef {
                    id: DistributedArtifactId::from(format!("{}:manifest", prepared.write_id)),
                    uri: format!("vfs://{staging_table}/generation/{}", prepared.generation),
                    checksum: schema_fingerprint.clone(),
                    size_bytes: published_bytes,
                },
                schema_fingerprint,
                metadata_payload: vcs::state::encode_table_metadata(&metadata)?,
            };
            table_write_catalog
                .commit_table_write(prepared.write_id, snapshot)
                .await?;
            return Ok(QueryOutput::Write {
                table: target,
                row_count: completion.row_count,
            });
        }

        Ok(QueryOutput::Data {
            query_id,
            stream: self.stream_job(job),
        })
        }
        .instrument(query_span)
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use binder::QueryBinder;
    use binder::testing::MockFunctionRegistry;
    use exec::ExecutorDaemon;
    use orchestrator::ExecutorRegistry;
    use orchestrator::Scheduler;
    use orchestrator::scheduler::quic_dispatcher::QuicTaskDispatcher;
    use parser::Parser;
    use physical::DefaultPhysicalPlanner;
    use std::net::SocketAddr;
    use std::time::Duration;
    use vcs::{InMemoryCatalog, TableCatalog};

    fn local_test_executor(
        exec_addr: SocketAddr,
        master_addr: &str,
        cert: &str,
        key: &str,
    ) -> ExecutorDaemon {
        ExecutorDaemon::builder()
            .bind(exec_addr)
            .certificates(cert, key)
            .workers(2)
            .master(master_addr)
            .local_development()
            .build()
            .expect("executor daemon should build")
    }

    /// Builds a fully-wired coordinator over a specified catalog.
    fn coordinator_with_catalog(catalog: Arc<dyn vcs::CatalogReader>) -> DistributedCoordinator {
        DistributedCoordinator::builder()
            .with_binder(Arc::new(QueryBinder::new(
                catalog,
                Arc::new(MockFunctionRegistry::new()),
            )))
            .with_optimizer(Arc::new(optimizer::OptimizerPipeline::production()))
            .with_planner(Arc::new(DefaultPhysicalPlanner::new()))
            .with_scheduler(Arc::new(Scheduler::new(
                Arc::new(ExecutorRegistry::new()),
                Arc::new(QuicTaskDispatcher::new()),
                3,
                Duration::from_secs(10),
            )))
            .build()
            .expect("coordinator should build")
    }

    /// Builds a fully-wired coordinator over the default in-memory catalog.
    fn coordinator() -> DistributedCoordinator {
        coordinator_with_catalog(Arc::new(InMemoryCatalog::new()))
    }

    #[test]
    fn test_builder_fails_without_dependencies() {
        let builder = DistributedCoordinator::builder();
        let result = builder.build();
        assert!(
            result.is_err(),
            "Builder should fail if dependencies are not injected"
        );
    }

    #[tokio::test]
    async fn explain_returns_plan_text_without_executing() {
        let engine = coordinator();
        let node = Parser::new("EXPLAIN FROM users |> SELECT id, name")
            .parse_statement()
            .expect("query should parse");
        let QueryOutput::Plan(text) = engine
            .submit_query(node)
            .await
            .expect("explain should succeed")
        else {
            panic!("EXPLAIN should return a plan");
        };
        assert!(text.contains("Explain"), "got:\n{text}");
        assert!(text.contains("Scan [table: users"), "got:\n{text}");
        assert!(text.contains("Project"), "got:\n{text}");
    }

    /// EXPLAIN returns the *optimized* plan: constant folding must have run
    /// inside the wrapped query by the time it is rendered.
    #[tokio::test]
    async fn explain_renders_optimized_plan() {
        let engine = coordinator();
        let node = Parser::new("EXPLAIN FROM users |> WHERE age > 18 + 2 |> SELECT id")
            .parse_statement()
            .expect("query should parse");
        let QueryOutput::Plan(text) = engine
            .submit_query(node)
            .await
            .expect("explain should succeed")
        else {
            panic!("EXPLAIN should return a plan");
        };
        // Scan-level predicate pushdown renders the filter as scan metadata.
        assert!(
            text.contains("filters: 1"),
            "expected scan filter, got:\n{text}"
        );
        assert!(!text.contains("18 + 2"), "expected folding, got:\n{text}");
    }

    #[tokio::test]
    async fn explain_native_exo_table_path_query() {
        use exo::PartWriter;
        use tempfile::tempdir;

        let dir = tempdir().expect("tempdir");
        let table_path = dir.path();

        // Write an exo partition with id, amount, and status
        {
            let mut writer = PartWriter::create(table_path, 0, 256).expect("create writer");
            let ids: Vec<i64> = vec![1, 2, 3];
            let amounts: Vec<f64> = vec![10.5, 60.0, 99.99];
            let statuses: Vec<String> = vec!["A".into(), "B".into(), "C".into()];

            writer.write_primitive_column("id", &ids).expect("write id");
            writer
                .write_primitive_column("amount", &amounts)
                .expect("write amount");
            writer
                .write_string_column("status", &statuses)
                .expect("write status");
            writer.finish().expect("finish part");
        }

        let catalog = Arc::new(InMemoryCatalog::new());
        let table_str = table_path.to_string_lossy().to_string();
        catalog.add_table(
            &table_str,
            Arc::new(plan::VeinSchema {
                fields: vec![
                    plan::ResolvedColumn::new(
                        plan::ColumnId(0),
                        "id",
                        None,
                        plan::VeinDataType::Int64,
                    ),
                    plan::ResolvedColumn::new(
                        plan::ColumnId(1),
                        "amount",
                        None,
                        plan::VeinDataType::Float64,
                    ),
                    plan::ResolvedColumn::new(
                        plan::ColumnId(2),
                        "status",
                        None,
                        plan::VeinDataType::Utf8,
                    ),
                ],
            }),
        );
        let engine = coordinator_with_catalog(catalog);
        let query_str = format!(
            "EXPLAIN FROM '{}' |> WHERE amount > 50.0 |> SELECT id, amount",
            table_str
        );

        let node = Parser::new(&query_str)
            .parse_statement()
            .expect("query should parse");
        let QueryOutput::Plan(text) = engine
            .submit_query(node)
            .await
            .expect("explain should succeed")
        else {
            panic!("EXPLAIN should return a plan");
        };

        assert!(text.contains("Explain"), "got:\n{text}");
        assert!(
            text.contains("ExternalScan") || text.contains("Scan"),
            "got:\n{text}"
        );
        assert!(text.contains("Project"), "got:\n{text}");
    }

    #[tokio::test]
    async fn explain_write_query() {
        let engine = coordinator();
        let node = Parser::new("EXPLAIN FROM users |> WRITE out_users")
            .parse_statement()
            .expect("query should parse");
        let QueryOutput::Plan(text) = engine
            .submit_query(node)
            .await
            .expect("explain should succeed")
        else {
            panic!("EXPLAIN should return a plan");
        };
        assert!(text.contains("Explain"), "got:\n{text}");
        assert!(text.contains("Write"), "got:\n{text}");
    }

    /// Non-EXPLAIN queries return a lazy stream; scheduling errors are delivered
    /// through that stream so callers can consume one uniform result protocol.
    #[tokio::test]
    async fn plain_query_reports_scheduler_error_through_stream() {
        let engine = coordinator();
        let node = Parser::new("FROM 'users' WITH (format = 'parquet') |> SELECT id, name")
            .parse_statement()
            .expect("query should parse");
        let QueryOutput::Data { mut stream, .. } = engine
            .submit_query(node)
            .await
            .expect("planning should return a result stream")
        else {
            panic!("plain query should return data");
        };

        let mut saw_scheduler_error = false;
        while let Some(event) = stream.next().await {
            match event.expect("result stream should remain transport-valid") {
                ResultEvent::Error { message } => {
                    saw_scheduler_error = message.contains("No executors available");
                }
                ResultEvent::Start { .. } | ResultEvent::Batch(_) | ResultEvent::End { .. } => {}
            }
        }
        assert!(saw_scheduler_error, "scheduler error was not streamed");
    }

    #[tokio::test]
    async fn test_end_to_end_quic_task_dispatch_and_stream_return() {
        use common::identifiers::{NodeId, QueryId, StageId, TaskId};
        use common::protocol::{
            DataSink, DataSource, ExecuteTask, ExecutionMode, MicroProgram, NodeKind, NodeLoad,
        };
        use orchestrator::executor::NodeInfo;
        use orchestrator::traits::TaskDispatcher;
        use std::net::SocketAddr;

        let cert = format!("{}/../../certs/server.crt", env!("CARGO_MANIFEST_DIR"));
        let key = format!("{}/../../certs/server.key", env!("CARGO_MANIFEST_DIR"));

        let exec_addr: SocketAddr = "127.0.0.1:9185".parse().unwrap();
        let daemon = local_test_executor(exec_addr, "127.0.0.1:9180", &cert, &key);

        let cert_c = cert.clone();
        let key_c = key.clone();
        tokio::spawn(async move {
            let _ = daemon.run_control_plane(exec_addr, &cert_c, &key_c).await;
        });

        // Give the listener a moment to bind
        tokio::time::sleep(Duration::from_millis(150)).await;

        let dispatcher = QuicTaskDispatcher::with_cert(&cert);
        let node_id = NodeId::new();
        let executor_info = NodeInfo {
            node_id,
            addr: exec_addr,
            kind: NodeKind::Executor,
            load: NodeLoad {
                active_tasks: 0,
                cpu_percent: 0.0,
                memory_bytes: 0,
            },
        };

        let request_id = common::identifiers::RequestId::new();
        let query_id = QueryId::new();
        let task_id = TaskId::new();
        let task = ExecuteTask {
            request_id,
            mode: ExecutionMode::Batch,
            task_id,
            attempt: 0,
            query_id,
            stage_id: StageId::new(),
            source: DataSource::Memory,
            program: MicroProgram {
                register_count: 1,
                instructions: vec![],
            },
            sink: DataSink::Client { schema: vec![] },
        };

        let result_stream = dispatcher.open_result_stream(query_id);

        // 1. Dispatch over real QUIC
        dispatcher
            .dispatch(&executor_info, task)
            .await
            .expect("dispatch over QUIC should succeed");

        // 2. Wait for completion over real QUIC
        dispatcher
            .wait_for_completion(query_id, task_id)
            .await
            .expect("wait for completion over QUIC should succeed");
        dispatcher.close_result_stream(query_id);

        // 3. Consume the bounded result stream without materializing a response buffer.
        let mut result_stream = result_stream;
        let mut total_rows = 0;
        while let Some(event) = result_stream.next().await {
            match event.expect("valid result event") {
                ResultEvent::Batch(batch) => total_rows += batch.row_count(),
                ResultEvent::Error { message } => panic!("query failed: {message}"),
                ResultEvent::Start { .. } | ResultEvent::End { .. } => {}
            }
        }
        assert_eq!(total_rows, 5, "stream should account for all rows");
        assert_eq!(total_rows, 5, "MemorySource produces 5 rows");
    }

    #[tokio::test]
    async fn test_master_daemon_records_heartbeat_and_acks() {
        use common::identifiers::NodeId;
        use common::protocol::{ArchivedVeinControlEvent, NodeKind, NodeLoad, VeinControlEvent};
        use common::transport::build_quic_client;
        use std::net::SocketAddr;

        let cert = format!("{}/../../certs/server.crt", env!("CARGO_MANIFEST_DIR"));
        let key = format!("{}/../../certs/server.key", env!("CARGO_MANIFEST_DIR"));

        let master_addr: SocketAddr = "127.0.0.1:9190".parse().unwrap();
        let registry = Arc::new(ExecutorRegistry::new());
        let dispatcher = Arc::new(QuicTaskDispatcher::with_cert(&cert));
        let scheduler = Arc::new(Scheduler::new(
            registry.clone(),
            dispatcher,
            3,
            Duration::from_secs(10),
        ));

        let catalog = Arc::new(InMemoryCatalog::new());
        let coordinator = Arc::new(
            DistributedCoordinator::builder()
                .with_binder(Arc::new(QueryBinder::new(
                    catalog.clone(),
                    Arc::new(MockFunctionRegistry::new()),
                )))
                .with_catalog_writer(catalog.clone())
                .with_optimizer(Arc::new(optimizer::OptimizerPipeline::production()))
                .with_planner(Arc::new(DefaultPhysicalPlanner::new()))
                .with_scheduler(scheduler)
                .build()
                .expect("coordinator should build"),
        );

        let master = crate::daemon::MasterDaemon::new(coordinator);
        let cert_m = cert.clone();
        let key_m = key.clone();
        tokio::spawn(async move {
            let _ = master.run(master_addr, &cert_m, &key_m).await;
        });

        tokio::time::sleep(Duration::from_millis(150)).await;

        let client = build_quic_client(&cert).expect("client should build");
        let connect = s2n_quic::client::Connect::new(master_addr).with_server_name("localhost");
        let mut conn = client.connect(connect).await.expect("connect to master");
        let mut stream = conn.open_bidirectional_stream().await.expect("open stream");

        let node_id = NodeId::new();
        let hb = VeinControlEvent::Heartbeat {
            node_id,
            node_kind: NodeKind::Executor,
            timestamp_ms: 1000,
            load: NodeLoad {
                active_tasks: 1,
                cpu_percent: 5.0,
                memory_bytes: 0,
            },
            advertise_addr: "127.0.0.1:9191".to_string(),
        };

        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&hb).expect("serialize hb");
        let wire = common::transport::frame_control_message(&bytes);
        stream.send(wire).await.expect("send hb");
        stream.finish().expect("finish send");

        let mut resp = Vec::new();
        while let Ok(Some(chunk)) = stream.receive().await {
            resp.extend_from_slice(&chunk);
        }

        assert!(!resp.is_empty(), "master should send back ack");
        assert_eq!(
            resp[0],
            common::transport::CONTROL_STREAM_OPCODE,
            "ack should have control opcode"
        );

        let payload =
            common::transport::extract_control_payload(&resp).expect("extract ack payload");

        let mut aligned = rkyv::util::AlignedVec::<16>::new();
        aligned.extend_from_slice(payload);
        let archived = rkyv::access::<ArchivedVeinControlEvent, rkyv::rancor::Error>(&aligned)
            .expect("archived ack");
        assert!(matches!(archived, ArchivedVeinControlEvent::HeartbeatAck));

        // Verify registry received heartbeat
        let nodes = registry.available(Duration::from_secs(5)).await;
        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[0].node_id, node_id);
        assert_eq!(
            nodes[0].addr,
            "127.0.0.1:9191".parse::<SocketAddr>().unwrap()
        );
    }

    #[tokio::test]
    async fn test_end_to_end_vfs_native_write_and_read() {
        use arrow::array::{Int64Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use common::identifiers::NodeId;
        use common::protocol::{NodeKind, NodeLoad};
        use std::net::SocketAddr;

        let cert = format!("{}/../../certs/server.crt", env!("CARGO_MANIFEST_DIR"));
        let key = format!("{}/../../certs/server.key", env!("CARGO_MANIFEST_DIR"));

        // 0. Set up an isolated VFS store in a fresh temp directory so stale
        //    partition state from previous test runs never bleeds in.
        let tmp = tempfile::tempdir().expect("tempdir");
        let vfs_cfg = vfs::VfsConfig::from_data_dir(tmp.path());
        let vfs_store = Arc::new(
            vfs::VfsTableStore::init(&vfs_cfg)
                .await
                .expect("vfs store init"),
        );
        vfs::VfsTableStore::set_shared(vfs_store.clone());

        // 1. Start Executor Daemon
        let exec_addr: SocketAddr = "127.0.0.1:9195".parse().unwrap();
        let daemon = local_test_executor(exec_addr, "127.0.0.1:9190", &cert, &key);

        let cert_c = cert.clone();
        let key_c = key.clone();
        tokio::spawn(async move {
            let _ = daemon.run_control_plane(exec_addr, &cert_c, &key_c).await;
        });

        tokio::time::sleep(Duration::from_millis(150)).await;

        // 2. Set up Coordinator with registered Executor in scheduler
        let registry = Arc::new(ExecutorRegistry::new());
        let node_id = NodeId::new();
        registry
            .record_heartbeat(
                node_id,
                exec_addr,
                NodeKind::Executor,
                NodeLoad {
                    active_tasks: 0,
                    cpu_percent: 0.0,
                    memory_bytes: 0,
                },
            )
            .await;

        let dispatcher = Arc::new(QuicTaskDispatcher::with_cert(&cert));
        let scheduler = Arc::new(Scheduler::new(
            registry,
            dispatcher,
            3,
            Duration::from_secs(10),
        ));

        let catalog = Arc::new(InMemoryCatalog::new());
        let coordinator = Arc::new(
            DistributedCoordinator::builder()
                .with_binder(Arc::new(QueryBinder::new(
                    catalog.clone(),
                    Arc::new(MockFunctionRegistry::new()),
                )))
                .with_catalog_writer(catalog.clone())
                .with_optimizer(Arc::new(optimizer::OptimizerPipeline::production()))
                .with_planner(Arc::new(DefaultPhysicalPlanner::new()))
                .with_scheduler(scheduler)
                .build()
                .expect("coordinator should build"),
        );

        // 3. Pre-seed a source table directly into VFS blocks (zero loose files)
        let source_table_name = "src_vfs_items";
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let ids = Arc::new(Int64Array::from(vec![1, 2, 3]));
        let names = Arc::new(StringArray::from(vec!["laptop", "phone", "tablet"]));
        let batch = RecordBatch::try_new(schema.clone(), vec![ids, names]).unwrap();

        vfs_store
            .append_partition(source_table_name, &batch)
            .await
            .expect("seed vfs table");
        catalog
            .register_table(
                plan::TableMetadata::new(
                    source_table_name,
                    Arc::new(plan::VeinSchema::from(schema.as_ref())),
                )
                .with_location(format!(
                    "vfs://tbl_{}",
                    vfs::VfsTableStore::resolve_table_id(source_table_name)
                ))
                .with_format(plan::TableFormat::Exo)
                .with_files(vec![format!(
                    "vfs://tbl_{}/part_000000",
                    vfs::VfsTableStore::resolve_table_id(source_table_name)
                )]),
            )
            .await
            .expect("register VFS source metadata");

        // 4. Execute WRITE query: FROM src_vfs_items |> WRITE dst_vfs_items
        let write_query = format!("FROM {source_table_name} |> WRITE dst_vfs_items");
        let node = Parser::new(&write_query)
            .parse_statement()
            .expect("query should parse");
        let write_res = coordinator
            .submit_query(node)
            .await
            .expect("write query should succeed");
        let QueryOutput::Write { table, .. } = write_res else {
            panic!("WRITE should return a write result");
        };
        assert_eq!(table, "dst_vfs_items");

        // 5. Verify NO loose filesystem directory exists!
        assert!(
            !std::path::Path::new("dst_vfs_items").exists(),
            "No directory dst_vfs_items on disk!"
        );
        assert!(
            !std::path::Path::new("data/dst_vfs_items").exists(),
            "No directory data/dst_vfs_items on disk!"
        );

        // 6. Execute READ query: FROM dst_vfs_items |> SELECT id, name
        let read_query = "FROM dst_vfs_items |> SELECT id, name";
        let node = Parser::new(read_query)
            .parse_statement()
            .expect("query should parse");
        let read_output = coordinator
            .submit_query(node)
            .await
            .expect("read query should succeed");
        let QueryOutput::Data { mut stream, .. } = read_output else {
            panic!("read query should return a data stream");
        };

        let mut total_rows = 0;
        while let Some(event) = stream.next().await {
            match event.expect("valid result event") {
                ResultEvent::Batch(batch) => {
                    total_rows += batch.row_count();
                    assert_eq!(batch.batch.num_columns(), 2);
                    let id_col = batch
                        .batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap();
                    let name_col = batch
                        .batch
                        .column(1)
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap();
                    assert_eq!(id_col.value(0), 1);
                    assert_eq!(name_col.value(0), "laptop");
                }
                ResultEvent::Error { message } => panic!("query failed: {message}"),
                ResultEvent::Start { .. } | ResultEvent::End { .. } => {}
            }
        }
        assert_eq!(
            total_rows, 3,
            "Must scan all 3 rows from VFS memory-mapped blocks"
        );

        // Clean up: reset shared store so other tests get their own default.
        vfs::VfsTableStore::reset_shared();
    }
}
