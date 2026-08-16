use async_trait::async_trait;
use bytes::Bytes;
use common::Result;
use std::sync::Arc;

use optimizer::Optimizer;
use orchestrator::traits::PhysicalPlanner;
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

    /// Validates and constructs the final DistributedCoordinator.
    pub fn build(self) -> Result<DistributedCoordinator> {
        let binder = self
            .binder
            .ok_or_else(|| error::VeinError::internal("Missing LogicalBinder configuration"))?;
        let optimizer = self
            .optimizer
            .ok_or_else(|| error::VeinError::internal("Missing Optimizer configuration"))?;
        let planner = self
            .planner
            .ok_or_else(|| error::VeinError::internal("Missing PhysicalPlanner configuration"))?;
        let scheduler = self
            .scheduler
            .ok_or_else(|| error::VeinError::internal("Missing Scheduler configuration"))?;

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
    async fn submit_query<'a>(&self, node: QueryNode<'a>) -> Result<Bytes> {
        // 1. BIND: Convert raw SQL AST to a Semantic Logical Plan
        let _query_id_str = format!("{:?}", node); // Simplified for telemetry
        let start_time = std::time::Instant::now();

        let logical_plan = self.binder.bind(&node).await?;

        // 2. OPTIMIZE: Apply rule-based rewrites (constant folding, ...)
        let optimized_plan = self.optimizer.optimize(logical_plan)?;

        // 2.5 EXPLAIN: render the optimized plan and return it directly —
        //             EXPLAIN never executes. `format_plan` already emits a
        //             trailing newline.
        if let LogicalPlan::Explain { .. } = &optimized_plan {
            let text = display::format_plan(&optimized_plan);
            tracing::debug!("EXPLAIN rendered plan:\n{text}");
            return Ok(Bytes::from(text));
        }

        // 3. PLAN: Lower the optimized logical plan into a distributed Physical DAG
        let physical_dag = self.planner.create_physical_plan(optimized_plan)?;

        // 4. DISPATCH: The Scheduler breaks the DAG into MorselTasks and dispatches them efficiently.
        let output_stage = physical_dag.output_stage_id();
        tracing::info!("Dispatched DAG with output stage {:?}", output_stage);

        let query_id = common::identifiers::QueryId::new();
        let cancel_token = common::cancellation::CancellationToken::new();

        self.scheduler
            .execute(query_id, physical_dag, cancel_token)
            .await?;

        // 5. RECEIVE: The Master node waits for the final DataTransport stream from the Executors.
        // TODO: Actually open the Arrow IPC stream receiver.
        let final_rows_produced = 0; // Mocked for now until data transport is complete

        // 6. FEEDBACK: Report execution telemetry back to the Reinforcement Learning Optimizer
        let telemetry = common::protocol::QueryTelemetry {
            query_id: common::identifiers::QueryId::new(), // In reality we'd parse or generate this
            execution_time_ms: start_time.elapsed().as_millis() as u64,
            rows_produced: final_rows_produced,
        };
        tracing::debug!(?telemetry, "query execution telemetry");

        Ok(Bytes::new())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use binder::QueryBinder;
    use binder::testing::MockFunctionRegistry;
    use orchestrator::scheduler::quic_dispatcher::QuicTaskDispatcher;
    use orchestrator::ExecutorRegistry;
    use orchestrator::planner::dag_planner::DagPhysicalPlanner;
    use orchestrator::Scheduler;
    use parser::Parser;
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
            .with_planner(Arc::new(DagPhysicalPlanner::new()))
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

    /// Non-EXPLAIN queries proceed into physical planning; that stage is a
    /// stub today, which is what the error pins here.
    #[tokio::test]
    async fn plain_query_reaches_physical_planning() {
        let engine = coordinator();
        let node = Parser::new("FROM users |> SELECT id, name")
            .parse_statement()
            .expect("query should parse");
        let err = engine.submit_query(node).await.expect_err("physical planning is stubbed");
        assert!(
            err.to_string().contains("create_physical_plan"),
            "got {err}"
        );
    }
}
