//! Query-scoped, low-overhead physical execution tracing.
//!
//! Node identifiers are assigned by deterministic preorder over the canonical
//! plan. Batch observations count reference-backed source arrays without
//! materializing them; repeated bytes across nodes therefore describe data flow,
//! not unique resident memory.

use common::time::Instant;
use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

use common::protocol::{DataSink, PhysicalPlanNode, PhysicalPlanOperator};
use common::statistics::{
    OperatorCounter, OperatorStatisticsHandle, OperatorStatisticsIdentity, QueryStatisticsContext,
};

use crate::memory::{OperatorResourceMetrics, QueryMemoryTracker};
use crate::pipeline::{DataChunkView, PipelineNode};

#[derive(Debug)]
struct NodeMetrics {
    node: Arc<PhysicalPlanNode>,
    id: u32,
    operator: &'static str,
    operator_config: String,
    child_ids: OnceLock<Vec<u32>>,
    statistics: Arc<OperatorStatisticsHandle>,
    source_array_count_seen: OperatorCounter,
    source_array_bytes_seen: OperatorCounter,
    downstream_push_ns: OperatorCounter,
    execution_completed: OperatorCounter,
    execution_cancelled: OperatorCounter,
    execution_failed: OperatorCounter,
    execution_dropped: OperatorCounter,
    resources: Arc<OperatorResourceMetrics>,
}

/// Stable identifiers and aggregate flow observations for one canonical plan.
#[derive(Debug)]
pub(crate) struct ExecutionTrace {
    nodes: Vec<Arc<NodeMetrics>>,
    node_ids: HashMap<usize, u32>,
    query_id: String,
    task_id: String,
    task_identity: u128,
    statistics_context: Arc<QueryStatisticsContext>,
    memory: Option<QueryMemoryTracker>,
    completed: bool,
}

impl ExecutionTrace {
    #[cfg(test)]
    pub(crate) fn new(root: &Arc<PhysicalPlanNode>, query_id: String, task_id: String) -> Self {
        Self::new_with_query_context(
            root,
            query_id,
            task_id,
            0,
            Arc::new(QueryStatisticsContext::new([])),
        )
    }

    pub(crate) fn new_with_query_context(
        root: &Arc<PhysicalPlanNode>,
        query_id: String,
        task_id: String,
        task_identity: u128,
        statistics_context: Arc<QueryStatisticsContext>,
    ) -> Self {
        let mut trace = Self {
            nodes: Vec::new(),
            node_ids: HashMap::new(),
            query_id,
            task_id,
            task_identity,
            statistics_context,
            memory: None,
            completed: false,
        };
        trace.visit(root);
        trace
    }

    pub(crate) fn node_id(&self, node: &PhysicalPlanNode) -> u32 {
        self.node_ids
            .get(&(node as *const PhysicalPlanNode as usize))
            .copied()
            .unwrap_or(u32::MAX)
    }

    pub(crate) fn query_statistics_context(&self) -> Arc<QueryStatisticsContext> {
        Arc::clone(&self.statistics_context)
    }

    /// Returns the shared statistics handle registered for a physical node.
    pub(crate) fn operator_statistics_handle(
        &self,
        node_id: u32,
    ) -> Option<Arc<OperatorStatisticsHandle>> {
        self.nodes
            .get(node_id as usize)
            .map(|node| Arc::clone(&node.statistics))
    }

    pub(crate) fn record_source_bytes(&self, node_id: u32, bytes: u64) {
        if let Some(node) = self.nodes.get(node_id as usize) {
            node.statistics.record_source_bytes(bytes);
        }
    }

    pub(crate) fn attach_memory_tracker(&mut self, memory: QueryMemoryTracker) {
        self.memory = Some(memory);
    }

    pub(crate) fn operator_memory(
        &self,
        node_id: u32,
        query_memory: &QueryMemoryTracker,
    ) -> QueryMemoryTracker {
        self.nodes.get(node_id as usize).map_or_else(
            || query_memory.clone(),
            |node| query_memory.for_operator(Arc::clone(&node.resources)),
        )
    }

    pub(crate) fn execution_guard(&self, node_id: u32) -> NodeExecutionGuard {
        NodeExecutionGuard {
            metrics: self.nodes.get(node_id as usize).cloned(),
            started: Instant::now(),
            outcome: 4,
        }
    }

    pub(crate) fn observer(
        &self,
        node_id: u32,
        next: Box<dyn PipelineNode>,
        query_id: String,
        task_id: String,
    ) -> Box<dyn PipelineNode> {
        let Some(metrics) = self.nodes.get(node_id as usize).cloned() else {
            return next;
        };
        Box::new(ObservedOutputNode {
            next,
            metrics,
            query_id,
            task_id,
        })
    }
}

mod plan;
mod summary;
mod guard;
mod observer;
mod describe;
use describe::{operator_config, operator_name};
pub(crate) use guard::NodeExecutionGuard;
use guard::execution_outcome;
use observer::ObservedOutputNode;

#[cfg(test)]
mod tests;
