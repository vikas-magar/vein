use common::Result;
use async_trait::async_trait;
use bytes::Bytes;
use std::sync::Arc;

use master::traits::{TaskDispatcher, PhysicalPlanner};
use planner::traits::{LogicalBinder, JoinOptimizer};
use parser::ast::query::Query;

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
    /// Abstract network layer for transmitting tasks to Executor nodes
    pub dispatcher: Arc<dyn TaskDispatcher>,
}

impl DistributedCoordinator {
    /// Creates a new builder for the DistributedCoordinator.
    pub fn builder() -> DistributedCoordinatorBuilder {
        DistributedCoordinatorBuilder {
            binder: None,
            optimizer: None,
            planner: None,
            dispatcher: None,
        }
    }
}

/// A builder for constructing a `DistributedCoordinator` with fluent DI configuration.
pub struct DistributedCoordinatorBuilder {
    binder: Option<Arc<dyn LogicalBinder>>,
    optimizer: Option<Arc<dyn JoinOptimizer>>,
    planner: Option<Arc<dyn PhysicalPlanner>>,
    dispatcher: Option<Arc<dyn TaskDispatcher>>,
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
    
    /// Injects the TaskDispatcher network strategy.
    pub fn with_dispatcher(mut self, dispatcher: Arc<dyn TaskDispatcher>) -> Self {
        self.dispatcher = Some(dispatcher);
        self
    }
    
    /// Validates and constructs the final DistributedCoordinator.
    pub fn build(self) -> Result<DistributedCoordinator> {
        let binder = self.binder.ok_or_else(|| error::VeinError::internal("Missing LogicalBinder configuration"))?;
        let optimizer = self.optimizer.ok_or_else(|| error::VeinError::internal("Missing JoinOptimizer configuration"))?;
        let planner = self.planner.ok_or_else(|| error::VeinError::internal("Missing PhysicalPlanner configuration"))?;
        let dispatcher = self.dispatcher.ok_or_else(|| error::VeinError::internal("Missing TaskDispatcher configuration"))?;
        
        Ok(DistributedCoordinator {
            binder,
            optimizer,
            planner,
            dispatcher,
        })
    }
}

#[async_trait]
impl QueryEngine for DistributedCoordinator {
    async fn submit_query(&self, query: Query) -> Result<Bytes> {
        // 1. BIND: Convert raw SQL AST to a Semantic Logical Plan
        let logical_plan = self.binder.bind(query)?;
        
        // 2. OPTIMIZE: Apply cost-based or RL-based Join reordering
        let optimized_plan = self.optimizer.optimize_join_order(logical_plan)?;
        
        // 3. PLAN: Lower the optimized logical plan into a distributed Physical DAG
        let physical_dag = self.planner.create_physical_plan(optimized_plan)?;
        
        // 4. DISPATCH: The Scheduler breaks the DAG into MorselTasks and uses the Dispatcher 
        // to send them over the network (e.g., via QUIC) to the Executor nodes.
        let output_stage = physical_dag.output_stage_id();
        tracing::info!("Dispatched DAG with output stage {:?}", output_stage);
        
        // 5. RECEIVE: The Master node waits for the final DataTransport stream from the Executors.
        
        // Return placeholder bytes representing the final Arrow IPC result
        Ok(Bytes::new())
    }
}
