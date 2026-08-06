use common::Result;
use async_trait::async_trait;
use bytes::Bytes;
use std::sync::Arc;

use master::traits::{TaskDispatcher, PhysicalPlanner};
use master::executor::registry::ExecutorRegistry;
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
    /// Abstract network layer for transmitting tasks to Executor nodes
    pub dispatcher: Arc<dyn TaskDispatcher>,
    /// Cluster state for task distribution
    pub registry: Arc<ExecutorRegistry>,
}

impl DistributedCoordinator {
    /// Creates a new builder for the DistributedCoordinator.
    pub fn builder() -> DistributedCoordinatorBuilder {
        DistributedCoordinatorBuilder {
            binder: None,
            optimizer: None,
            planner: None,
            dispatcher: None,
            registry: None,
        }
    }
}

/// A builder for constructing a `DistributedCoordinator` with fluent DI configuration.
pub struct DistributedCoordinatorBuilder {
    binder: Option<Arc<dyn LogicalBinder>>,
    optimizer: Option<Arc<dyn JoinOptimizer>>,
    planner: Option<Arc<dyn PhysicalPlanner>>,
    dispatcher: Option<Arc<dyn TaskDispatcher>>,
    registry: Option<Arc<ExecutorRegistry>>,
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

    /// Injects the ExecutorRegistry state.
    pub fn with_registry(mut self, registry: Arc<ExecutorRegistry>) -> Self {
        self.registry = Some(registry);
        self
    }
    
    /// Validates and constructs the final DistributedCoordinator.
    pub fn build(self) -> Result<DistributedCoordinator> {
        let binder = self.binder.ok_or_else(|| error::VeinError::internal("Missing LogicalBinder configuration"))?;
        let optimizer = self.optimizer.ok_or_else(|| error::VeinError::internal("Missing JoinOptimizer configuration"))?;
        let planner = self.planner.ok_or_else(|| error::VeinError::internal("Missing PhysicalPlanner configuration"))?;
        let dispatcher = self.dispatcher.ok_or_else(|| error::VeinError::internal("Missing TaskDispatcher configuration"))?;
        let registry = self.registry.ok_or_else(|| error::VeinError::internal("Missing ExecutorRegistry configuration"))?;
        
        Ok(DistributedCoordinator {
            binder,
            optimizer,
            planner,
            dispatcher,
            registry,
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
        
        let available_nodes = self.registry.available(std::time::Duration::from_secs(10)).await;
        if available_nodes.is_empty() {
             return Err(error::VeinError::internal("No available executor nodes to dispatch tasks to"));
        }
        
        // Track simulated load during this dispatch loop to ensure we distribute fairly
        // based on the telemetry feedback loop (heartbeat active_tasks).
        let mut current_loads: Vec<(usize, usize)> = available_nodes.iter()
            .enumerate()
            .map(|(idx, n)| (idx, n.load.active_tasks as usize))
            .collect();

        // For each stage in the DAG, dispatch it.
        for stage_id in physical_dag.topological_order() {
            if let Some(stage) = physical_dag.get_stage(stage_id) {
                let task = MorselTask {
                    task_id: TaskId::new(),
                    source: stage.source.clone(),
                    plan: stage.operators.clone(),
                    sink: stage.sink.clone(),
                };
                
                // Feedback-Loop Routing: Always pick the node with the absolute lowest load
                current_loads.sort_by_key(|&(_, load)| load);
                let best_idx = current_loads[0].0;
                current_loads[0].1 += 1; // Simulate the added load so the next task goes elsewhere if needed
                
                let target_node = &available_nodes[best_idx];
                self.dispatcher.dispatch(target_node.node_id, task).await?;
            }
        }
        
        // 5. RECEIVE: The Master node waits for the final DataTransport stream from the Executors.
        // TODO: Actually open the Arrow IPC stream receiver.
        
        Ok(Bytes::new())
    }
}
