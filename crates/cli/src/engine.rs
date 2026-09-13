use async_trait::async_trait;
use bytes::Bytes;
use common::Result;
use std::sync::Arc;

use optimizer::Optimizer;
use physical::PhysicalPlanner;
use parser::ast::query::QueryNode;
use plan::display;
use plan::LogicalPlan;
use binder::LogicalBinder;

/// The absolute highest-level abstraction for the Database Engine.
/// Represents the complete end-to-end lifecycle from query submission to result retrieval.
#[async_trait]
pub trait QueryEngine: Send + Sync {
    /// Submits a parsed statement and returns the result bytes. Executed
    /// queries return Arrow IPC; `EXPLAIN` statements short-circuit and
    /// return the rendered (optimized) plan text as UTF-8.
    async fn submit_query<'a>(&self, node: QueryNode<'a>) -> Result<Bytes>;

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
}

impl DistributedCoordinator {
    /// Creates a new builder for the DistributedCoordinator.
    pub fn builder() -> DistributedCoordinatorBuilder {
        DistributedCoordinatorBuilder {
            binder: None,
            optimizer: None,
            planner: None,
            scheduler: None,
        }
    }
}

/// A builder for constructing a `DistributedCoordinator` with fluent DI configuration.
pub struct DistributedCoordinatorBuilder {
    binder: Option<Arc<dyn LogicalBinder>>,
    optimizer: Option<Arc<dyn Optimizer>>,
    planner: Option<Arc<dyn PhysicalPlanner>>,
    scheduler: Option<Arc<orchestrator::scheduler::Scheduler>>,
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
        })
    }
}

#[async_trait]
impl QueryEngine for DistributedCoordinator {
    fn registry(&self) -> Option<Arc<orchestrator::executor::ExecutorRegistry>> {
        Some(self.scheduler.registry().clone())
    }

    async fn submit_query<'a>(&self, node: QueryNode<'a>) -> Result<Bytes> {
        let start_time = std::time::Instant::now();

        // 1. BIND: Convert raw SQL AST to a Semantic Logical Plan
        tracing::debug!(target: "vein_coordinator", "Binding SQL AST to semantic logical plan");
        let logical_plan = self.binder.bind(&node).await?;

        // 2. OPTIMIZE: Apply rule-based rewrites (constant folding, ...)
        tracing::debug!(target: "vein_coordinator", "Applying rule-based plan optimizations");
        let optimized_plan = self.optimizer.optimize(logical_plan)?;

        // 2.5 EXPLAIN: render the optimized plan and return it directly —
        //             EXPLAIN never executes. `format_plan` already emits a
        //             trailing newline.
        if let LogicalPlan::Explain { .. } = &optimized_plan {
            let text = display::format_plan(&optimized_plan);
            tracing::info!(target: "vein_coordinator", "EXPLAIN statement evaluated; returning rendered plan");
            tracing::debug!(target: "vein_coordinator", "Rendered plan:\n{text}");
            return Ok(Bytes::from(text));
        }

        // 3. PLAN: Lower the optimized logical plan into a distributed physical Job.
        tracing::debug!(target: "vein_coordinator", "Lowering optimized logical plan to physical DAG");
        let job = self.planner.create_physical_plan(optimized_plan)?;
        let query_id = job.query_id;

        // 4. DISPATCH: The Scheduler breaks the Job into stages/tasks and dispatches them.
        let output_stage = job.output_stage_id();
        tracing::info!(target: "vein_coordinator", ?output_stage, "Dispatching physical job to cluster scheduler");

        let cancel_token = common::cancellation::CancellationToken::new();

        self.scheduler.execute(job, cancel_token).await?;

        // 5. RECEIVE: Extract the collected Arrow IPC bytes from the dispatcher.
        let output_bytes = self
            .scheduler
            .dispatcher()
            .take_query_output(query_id)
            .unwrap_or_default();

        // 6. FEEDBACK: Report execution telemetry back to the Reinforcement Learning Optimizer
        let telemetry = common::protocol::QueryTelemetry {
            query_id,
            execution_time_ms: start_time.elapsed().as_millis() as u64,
            rows_produced: 0,
        };
        tracing::debug!(
            target: "vein_coordinator",
            query_id = %telemetry.query_id,
            elapsed_ms = telemetry.execution_time_ms,
            "Query execution telemetry recorded"
        );

        Ok(output_bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use binder::QueryBinder;
    use binder::testing::MockFunctionRegistry;
    use orchestrator::scheduler::quic_dispatcher::QuicTaskDispatcher;
    use orchestrator::ExecutorRegistry;
    use orchestrator::Scheduler;
    use parser::Parser;
    use physical::DefaultPhysicalPlanner;
    use std::time::Duration;
    use vcs::InMemoryCatalog;

    /// Builds a fully-wired coordinator over the in-memory catalog.
    fn coordinator() -> DistributedCoordinator {
        DistributedCoordinator::builder()
            .with_binder(Arc::new(QueryBinder::new(
                Arc::new(InMemoryCatalog::new()),
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
        let bytes = engine.submit_query(node).await.expect("explain should succeed");
        let text = String::from_utf8(bytes.to_vec()).expect("explain output should be utf-8");
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
        let bytes = engine.submit_query(node).await.expect("explain should succeed");
        let text = String::from_utf8(bytes.to_vec()).expect("explain output should be utf-8");
        assert!(text.contains("> 20"), "expected folded literal, got:\n{text}");
        assert!(!text.contains("18 + 2"), "expected folding, got:\n{text}");
    }

    /// Non-EXPLAIN queries proceed through physical planning into the scheduler.
    /// In an isolated test harness without remote executor daemons registered,
    /// scheduling fails with `No executors available`.
    #[tokio::test]
    async fn plain_query_reaches_physical_planning() {
        let engine = coordinator();
        let node = Parser::new("FROM users |> SELECT id, name")
            .parse_statement()
            .expect("query should parse");
        let err = engine.submit_query(node).await.expect_err("no executors registered");
        assert!(
            err.to_string().contains("No executors available"),
            "got {err}"
        );
    }

    #[tokio::test]
    async fn test_end_to_end_quic_task_dispatch_and_stream_return() {
        use common::identifiers::{NodeId, QueryId, StageId, TaskId};
        use common::protocol::{DataSink, DataSource, ExecuteTask, ExecutionMode, MicroProgram, NodeKind, NodeLoad};
        use orchestrator::executor::NodeInfo;
        use orchestrator::traits::TaskDispatcher;
        use exec::ExecutorDaemon;
        use std::net::SocketAddr;

        let cert = format!("{}/../../certs/server.crt", env!("CARGO_MANIFEST_DIR"));
        let key = format!("{}/../../certs/server.key", env!("CARGO_MANIFEST_DIR"));

        let exec_addr: SocketAddr = "127.0.0.1:9185".parse().unwrap();
        let daemon = ExecutorDaemon::builder()
            .bind(exec_addr)
            .certificates(&cert, &key)
            .workers(2)
            .master("127.0.0.1:9180")
            .build()
            .expect("executor daemon should build");

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

        let query_id = QueryId::new();
        let task_id = TaskId::new();
        let task = ExecuteTask {
            mode: ExecutionMode::Batch,
            task_id,
            query_id,
            stage_id: StageId::new(),
            source: DataSource::Memory,
            program: MicroProgram {
                register_count: 1,
                instructions: vec![],
            },
            sink: DataSink::Client { schema: vec![] },
        };

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

        // 3. Take query output
        let output = dispatcher
            .take_query_output(query_id)
            .expect("should have query output");

        // 4. Verify Arrow IPC stream
        assert!(!output.is_empty(), "output bytes should not be empty");
        let cursor = std::io::Cursor::new(output.as_ref());
        let reader = arrow::ipc::reader::StreamReader::try_new(cursor, None)
            .expect("valid arrow ipc stream");
        let mut total_rows = 0;
        for batch in reader {
            let b = batch.expect("valid batch");
            total_rows += b.num_rows();
        }
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
        let registry = Arc::new(orchestrator::executor::registry::ExecutorRegistry::new());
        let dispatcher = Arc::new(orchestrator::scheduler::quic_dispatcher::QuicTaskDispatcher::with_cert(&cert));
        let scheduler = Arc::new(orchestrator::scheduler::Scheduler::new(
            registry.clone(),
            dispatcher,
            3,
            Duration::from_secs(10),
        ));

        let coordinator = Arc::new(
            DistributedCoordinator::builder()
                .with_binder(Arc::new(QueryBinder::new(
                    Arc::new(InMemoryCatalog::new()),
                    Arc::new(MockFunctionRegistry::new()),
                )))
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
        assert_eq!(resp[0], common::transport::CONTROL_STREAM_OPCODE, "ack should have control opcode");

        let payload = common::transport::extract_control_payload(&resp).expect("extract ack payload");

        let mut aligned = rkyv::util::AlignedVec::<16>::new();
        aligned.extend_from_slice(payload);
        let archived = rkyv::access::<ArchivedVeinControlEvent, rkyv::rancor::Error>(&aligned)
            .expect("archived ack");
        assert!(matches!(archived, ArchivedVeinControlEvent::HeartbeatAck));

        // Verify registry received heartbeat
        let nodes = registry.available(Duration::from_secs(5)).await;
        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[0].node_id, node_id);
        assert_eq!(nodes[0].addr, "127.0.0.1:9191".parse::<SocketAddr>().unwrap());
    }
}
