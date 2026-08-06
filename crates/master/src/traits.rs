use common::Result;
use async_trait::async_trait;
use common::protocol::MorselTask;

/// Highly abstract Task Dispatcher.
/// Allows swapping our QUIC network layer with TCP, gRPC, or an in-memory mock for local testing.
#[async_trait]
pub trait TaskDispatcher: Send + Sync {
    /// Dispatch a compiled physical Morsel to a specific Executor node over the network.
    async fn dispatch(&self, node_id: u32, task: MorselTask) -> Result<()>;
}

/// Abstract Query Optimizer.
/// Allows us to seamlessly swap a traditional Cost-Based Optimizer (Cascades) 
/// with our cutting-edge Reinforcement Learning engine (ADOPT).
pub trait JoinOptimizer: Send + Sync {
    /// Takes a Logical AST and returns an optimized Physical DAG string (to be strongly typed later).
    fn optimize_join_order(&self, logical_plan: String) -> Result<String>;
}
