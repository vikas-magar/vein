use common::Result;
use async_trait::async_trait;
use bytes::Bytes;
use std::sync::Arc;

use orchestrator::traits::{TaskDispatcher, PhysicalPlanner};
use orchestrator::executor::registry::ExecutorRegistry;
use planner::traits::{LogicalBinder, JoinOptimizer};
use parser::ast::query::Query;
use common::identifiers::TaskId;
use common::protocol::MorselTask;

/// The absolute highest-level abstraction for the Database Engine.
/// Represents the complete end-to-end lifecycle from query submission to result retrieval.
#[async_trait]
pub trait QueryEngine: Send + Sync {
    /// Submits a query AST and returns the final result as Arrow IPC bytes.
    async fn submit_query(&self, query: Query) -> Result<Bytes>;
}

/// The concrete Coordinator that wires all the abstract traits together.
/// It does not contain any hardcoded logic—it purely orchestrates the injected trait implementations.
pub struct DistributedCoordinator {
    /// Abstract layer for binding ASTs to semantic schemas
    pub binder: Arc<dyn LogicalBinder>,
    /// Abstract layer for dynamic Join ordering (ADOPT/CBO)
    pub optimizer: Arc<dyn JoinOptimizer>,
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
    optimizer: Option<Arc<dyn JoinOptimizer>>,
    planner: Option<Arc<dyn PhysicalPlanner>>,
    scheduler: Option<Arc<orchestrator::scheduler::Scheduler>>,
}

impl DistributedCoordinatorBuilder {
    /// Injects the LogicalBinder strategy.
    pub fn with_binder(mut self, binder: Arc<dyn LogicalBinder>) -> Self {
        self.binder = Some(binder);
        self
    }
    
    /// Injects the JoinOptimizer strategy.
    pub fn with_optimizer(mut self, optimizer: Arc<dyn JoinOptimizer>) -> Self {
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
        let binder = self.binder.ok_or_else(|| error::VeinError::internal("Missing LogicalBinder configuration"))?;
        let optimizer = self.optimizer.ok_or_else(|| error::VeinError::internal("Missing JoinOptimizer configuration"))?;
        let planner = self.planner.ok_or_else(|| error::VeinError::internal("Missing PhysicalPlanner configuration"))?;
        let scheduler = self.scheduler.ok_or_else(|| error::VeinError::internal("Missing Scheduler configuration"))?;
        
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
    async fn submit_query(&self, query: Query) -> Result<Bytes> {
        // 1. BIND: Convert raw SQL AST to a Semantic Logical Plan
        let query_id_str = format!("{:?}", query); // Simplified for telemetry
        let start_time = std::time::Instant::now();
        
        let logical_plan = self.binder.bind(&query).await?;
        
        // 2. OPTIMIZE: Apply cost-based or RL-based Join reordering
        let optimized_plan = self.optimizer.optimize_join(&logical_plan)?;
        
        // 3. PLAN: Lower the optimized logical plan into a distributed Physical DAG
        let physical_dag = self.planner.create_physical_plan(optimized_plan)?;
        
        // 4. DISPATCH: The Scheduler breaks the DAG into MorselTasks and dispatches them efficiently.
        let output_stage = physical_dag.output_stage_id();
        tracing::info!("Dispatched DAG with output stage {:?}", output_stage);
        
        let query_id = common::identifiers::QueryId::new();
        let cancel_token = common::cancellation::CancellationToken::new();
        
        self.scheduler.execute(query_id, physical_dag, cancel_token).await?;
        
        // 5. RECEIVE: The Master node waits for the final DataTransport stream from the Executors.
        // TODO: Actually open the Arrow IPC stream receiver.
        let final_rows_produced = 0; // Mocked for now until data transport is complete
        
        // 6. FEEDBACK: Report execution telemetry back to the Reinforcement Learning Optimizer
        let telemetry = common::protocol::QueryTelemetry {
            query_id: common::identifiers::QueryId::new(), // In reality we'd parse or generate this
            execution_time_ms: start_time.elapsed().as_millis() as u64,
            rows_produced: final_rows_produced,
        };
        let _ = self.optimizer.report_telemetry();
        
        Ok(Bytes::new())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_builder_fails_without_dependencies() {
        let builder = DistributedCoordinator::builder();
        let result = builder.build();
        assert!(result.is_err(), "Builder should fail if dependencies are not injected");
    }
}
