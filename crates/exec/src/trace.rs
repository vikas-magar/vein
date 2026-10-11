//! Query-scoped, low-overhead physical execution tracing.
//!
//! Node identifiers are assigned by deterministic preorder over the canonical
//! plan. Batch observations count reference-backed source arrays without
//! materializing them; repeated bytes across nodes therefore describe data flow,
//! not unique resident memory.

use std::collections::HashMap;
use std::sync::{Arc, OnceLock};
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use common::protocol::{PhysicalPlanNode, PhysicalPlanOperator};

use crate::memory::{OperatorResourceMetrics, QueryMemoryTracker};
use crate::pipeline::{DataChunkView, MaterializationReport, PipelineNode};

#[derive(Debug)]
struct NodeMetrics {
    node: Arc<PhysicalPlanNode>,
    id: u32,
    operator: &'static str,
    operator_config: String,
    child_ids: OnceLock<Vec<u32>>,
    output_rows: AtomicU64,
    output_batches: AtomicU64,
    source_bytes_read: AtomicU64,
    source_array_count_seen: AtomicU64,
    source_array_bytes_seen: AtomicU64,
    downstream_push_ns: AtomicU64,
    inclusive_elapsed_ns: AtomicU64,
    execution_outcome: AtomicU8,
    materialized_batches: AtomicU64,
    materialized_rows: AtomicU64,
    materialized_shared_columns: AtomicU64,
    materialized_gathered_columns: AtomicU64,
    materialized_empty_columns: AtomicU64,
    materialized_output_buffer_bytes: AtomicU64,
    materialization_ns: AtomicU64,
    resources: Arc<OperatorResourceMetrics>,
}

/// Stable identifiers and aggregate flow observations for one canonical plan.
#[derive(Debug, Default)]
pub(crate) struct ExecutionTrace {
    nodes: Vec<Arc<NodeMetrics>>,
    node_ids: HashMap<usize, u32>,
    query_id: String,
    task_id: String,
    memory: Option<QueryMemoryTracker>,
    completed: bool,
}

impl ExecutionTrace {
    pub(crate) fn new(
        root: &Arc<PhysicalPlanNode>,
        query_id: String,
        task_id: String,
    ) -> Self {
        let mut trace = Self {
            nodes: Vec::new(),
            node_ids: HashMap::new(),
            query_id,
            task_id,
            memory: None,
            completed: false,
        };
        trace.visit(root);
        trace
    }

    fn visit(&mut self, node: &Arc<PhysicalPlanNode>) -> u32 {
        let identity = Arc::as_ptr(node) as usize;
        if let Some(id) = self.node_ids.get(&identity) {
            return *id;
        }

        let id = u32::try_from(self.nodes.len()).unwrap_or(u32::MAX);
        let metrics = Arc::new(NodeMetrics {
            node: Arc::clone(node),
            id,
            operator: operator_name(&node.operator),
            operator_config: operator_config(&node.operator),
            child_ids: OnceLock::new(),
            output_rows: AtomicU64::new(0),
            output_batches: AtomicU64::new(0),
            source_bytes_read: AtomicU64::new(0),
            source_array_count_seen: AtomicU64::new(0),
            source_array_bytes_seen: AtomicU64::new(0),
            downstream_push_ns: AtomicU64::new(0),
            inclusive_elapsed_ns: AtomicU64::new(0),
            execution_outcome: AtomicU8::new(0),
            materialized_batches: AtomicU64::new(0),
            materialized_rows: AtomicU64::new(0),
            materialized_shared_columns: AtomicU64::new(0),
            materialized_gathered_columns: AtomicU64::new(0),
            materialized_empty_columns: AtomicU64::new(0),
            materialized_output_buffer_bytes: AtomicU64::new(0),
            materialization_ns: AtomicU64::new(0),
            resources: Arc::new(OperatorResourceMetrics::default()),
        });
        self.node_ids.insert(identity, id);
        self.nodes.push(Arc::clone(&metrics));

        let child_ids = children(&node.operator)
            .iter()
            .map(|child| self.visit(child))
            .collect();
        // This is initialized before recursive traversal only to make shared
        // nodes and a malformed cycle safe; no other observer can see it yet.
        let _ = metrics.child_ids.set(child_ids);
        id
    }

    pub(crate) fn node_id(&self, node: &PhysicalPlanNode) -> u32 {
        self.node_ids
            .get(&(node as *const PhysicalPlanNode as usize))
            .copied()
            .unwrap_or(u32::MAX)
    }

    pub(crate) fn record_source_bytes(&self, node_id: u32, bytes: u64) {
        if let Some(node) = self.nodes.get(node_id as usize) {
            node.source_bytes_read.fetch_add(bytes, Ordering::Relaxed);
        }
    }

    pub(crate) fn attach_memory_tracker(&mut self, memory: QueryMemoryTracker) {
        self.memory = Some(memory);
    }

    pub(crate) fn operator_memory(&self, node_id: u32, query_memory: &QueryMemoryTracker) -> QueryMemoryTracker {
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

    pub(crate) fn materialization_handle(&self, node_id: u32) -> Option<MaterializationTrace> {
        self.nodes.get(node_id as usize).map(|node| MaterializationTrace {
            metrics: Arc::clone(node),
            query_id: self.query_id.clone(),
            task_id: self.task_id.clone(),
        })
    }

    pub(crate) fn observer(
        &self,
        node_id: u32,
        next: Box<dyn PipelineNode<DataChunkView>>,
        query_id: String,
        task_id: String,
    ) -> Box<dyn PipelineNode<DataChunkView>> {
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

    pub(crate) fn log_plan(&self) {
        tracing::debug!(
            target: "vein.audit",
            event = "physical_plan_explain",
            query_id = %self.query_id,
            task_id = %self.task_id,
            node_count = self.nodes.len(),
            explanation = %self.format_explain(),
            "physical_plan_explain"
        );
        for metrics in &self.nodes {
            let schema = metrics.node
                .output_schema
                .fields
                .iter()
                .map(|field| format!("{}:{:?}{}", field.name, field.data_type, if field.nullable { "?" } else { "" }))
                .collect::<Vec<_>>();
            tracing::debug!(
                target: "vein.audit",
                event = "physical_plan_node",
                query_id = %self.query_id,
                task_id = %self.task_id,
                node_id = metrics.id,
                operator = metrics.operator,
                operator_config = %metrics.operator_config,
                child_node_ids = ?metrics.child_ids.get().map(Vec::as_slice).unwrap_or_default(),
                output_schema = ?schema,
                input_materialization = ?metrics.node.operator.data_flow().input_materialization,
                output_representation = ?metrics.node.operator.data_flow().output,
                "physical_plan_node"
            );
        }
    }

    /// Deterministic compact explain text over the canonical plan-node graph.
    /// The stable preorder IDs and child references preserve DAG sharing.
    fn format_explain(&self) -> String {
        self.nodes.iter().map(|metrics| {
            let children = metrics.child_ids.get().map(Vec::as_slice).unwrap_or_default();
            let schema = metrics.node.output_schema.fields.iter().map(|field| {
                let qualifier = field.qualifier.as_deref().map(|name| format!("{name}.")).unwrap_or_default();
                format!("{qualifier}{}:{:?}{}", field.name, field.data_type, if field.nullable { "?" } else { "" })
            }).collect::<Vec<_>>().join(", ");
            let flow = metrics.node.operator.data_flow();
            let boundary = match flow.input_materialization {
                common::protocol::InputMaterialization::WholeBatch => "materialize-input",
                common::protocol::InputMaterialization::ConsumerBoundary => "consumer-boundary",
                common::protocol::InputMaterialization::KernelColumns => "kernel-columns",
                common::protocol::InputMaterialization::None => "reference-input",
            };
            format!(
                "#{} {} children={:?} [{}] flow={}->{:?} schema=[{}]",
                metrics.id,
                metrics.operator,
                children,
                metrics.operator_config,
                boundary,
                flow.output,
                schema,
            )
        }).collect::<Vec<_>>().join("\n")
    }

    fn log_summary(&self) {
        for node in &self.nodes {
            let output_rows = node.output_rows.load(Ordering::Relaxed);
            let input_rows = node
                .child_ids.get().into_iter().flatten()
                .filter_map(|id| self.nodes.get(*id as usize))
                .map(|child| child.output_rows.load(Ordering::Relaxed))
                .fold(0u64, u64::saturating_add);
            let input_array_count_seen = node.child_ids.get().into_iter().flatten()
                .filter_map(|id| self.nodes.get(*id as usize))
                .map(|child| child.source_array_count_seen.load(Ordering::Relaxed))
                .fold(0u64, u64::saturating_add);
            let input_array_footprint_bytes_seen = node.child_ids.get().into_iter().flatten()
                .filter_map(|id| self.nodes.get(*id as usize))
                .map(|child| child.source_array_bytes_seen.load(Ordering::Relaxed))
                .fold(0u64, u64::saturating_add);
            let execution_outcome = match node.execution_outcome.load(Ordering::Relaxed) {
                1 => "complete",
                2 => "cancelled",
                3 => "failed",
                4 => "cancelled_or_dropped",
                _ => "not_started",
            };
            tracing::debug!(
                target: "vein.audit",
                event = "physical_operator_metrics",
                query_id = %self.query_id,
                task_id = %self.task_id,
                node_id = node.id,
                operator = node.operator,
                child_node_ids = ?node.child_ids.get().map(Vec::as_slice).unwrap_or_default(),
                input_rows,
                input_array_count_seen,
                input_array_footprint_bytes_seen,
                output_rows,
                output_batches = node.output_batches.load(Ordering::Relaxed),
                source_bytes_read = node.source_bytes_read.load(Ordering::Relaxed),
                output_array_count_seen = node.source_array_count_seen.load(Ordering::Relaxed),
                output_array_footprint_bytes_seen = node.source_array_bytes_seen.load(Ordering::Relaxed),
                array_footprint_scope = "arrow_reported_sizes_seen_at_operator_boundaries; shared_buffers_and_repeated_flow_are_counted_again",
                inclusive_elapsed_ns = node.inclusive_elapsed_ns.load(Ordering::Relaxed),
                execution_outcome,
                downstream_push_ns = node.downstream_push_ns.load(Ordering::Relaxed),
                operator_memory_reserved_bytes = node.resources.memory_used(),
                operator_memory_peak_reserved_bytes = node.resources.memory_peak(),
                operator_spill_disk_bytes = node.resources.spill_used(),
                operator_spill_disk_peak_bytes = node.resources.spill_peak(),
                operator_spill_bytes_written = node.resources.spill_written(),
                materialized_batches = node.materialized_batches.load(Ordering::Relaxed),
                materialized_rows = node.materialized_rows.load(Ordering::Relaxed),
                materialized_shared_columns = node.materialized_shared_columns.load(Ordering::Relaxed),
                materialized_gathered_columns = node.materialized_gathered_columns.load(Ordering::Relaxed),
                materialized_empty_columns = node.materialized_empty_columns.load(Ordering::Relaxed),
                materialized_output_buffer_bytes_estimated = node.materialized_output_buffer_bytes.load(Ordering::Relaxed),
                materialization_ns = node.materialization_ns.load(Ordering::Relaxed),
                "physical_operator_metrics"
            );
        }
        tracing::debug!(
            target: "vein.audit",
            event = "execution_trace_summary",
            query_id = %self.query_id,
            task_id = %self.task_id,
            outcome = if self.completed { "complete" } else { "incomplete_or_failed" },
            operator_count = self.nodes.len(),
            "execution_trace_summary"
        );
        if let Some(memory) = &self.memory {
            let (source_bytes_read, source_array_count_seen, source_array_footprint_bytes_seen) =
                self.nodes.iter().fold((0u64, 0u64, 0u64), |totals, node| {
                    (
                        totals.0.saturating_add(node.source_bytes_read.load(Ordering::Relaxed)),
                        totals.1.saturating_add(node.source_array_count_seen.load(Ordering::Relaxed)),
                        totals.2.saturating_add(node.source_array_bytes_seen.load(Ordering::Relaxed)),
                    )
                });
            tracing::debug!(
                target: "vein.audit",
                event = "execution_resource_summary",
                query_id = %self.query_id,
                task_id = %self.task_id,
                outcome = if self.completed { "complete" } else { "incomplete_or_failed" },
                source_bytes_read,
                source_array_count_seen,
                source_array_footprint_bytes_seen,
                source_array_footprint_scope = "arrow_reported_sizes_seen_at_operator_outputs; repeated_across_nodes_and_batches",
                operator_memory_reserved_bytes = memory.used(),
                operator_memory_peak_reserved_bytes = memory.peak(),
                operator_memory_limit_bytes = memory.limit(),
                operator_memory_accounting = "explicit_query_reservations_including_arrow_array_footprint_estimates",
                spill_disk_reserved_bytes = memory.spill_bytes_used(),
                spill_disk_peak_reserved_bytes = memory.spill_bytes_peak(),
                spill_disk_limit_bytes = memory.spill_bytes_limit(),
                process_memory_bytes_measured = false,
                "execution_resource_summary"
            );
        }
    }

    pub(crate) fn mark_completed(&mut self) {
        self.completed = true;
    }
}

pub(crate) struct MaterializationTrace {
    metrics: Arc<NodeMetrics>,
    query_id: String,
    task_id: String,
}

impl MaterializationTrace {
    pub(crate) fn node_id(&self) -> u32 {
        self.metrics.id
    }

    pub(crate) fn query_id(&self) -> &str {
        &self.query_id
    }

    pub(crate) fn task_id(&self) -> &str {
        &self.task_id
    }
}

/// Records inclusive wall time even when a runtime future is cancelled by its caller.
pub(crate) struct NodeExecutionGuard {
    metrics: Option<Arc<NodeMetrics>>,
    started: Instant,
    outcome: u8,
}

impl NodeExecutionGuard {
    pub(crate) fn finish(&mut self, result: &common::Result<u64>) {
        self.outcome = match result {
            Ok(_) => 1,
            Err(error) if matches!(error, error::VeinError::Cancelled) => 2,
            Err(_) => 3,
        };
    }
}

impl Drop for NodeExecutionGuard {
    fn drop(&mut self) {
        if let Some(metrics) = &self.metrics {
            metrics.inclusive_elapsed_ns.fetch_add(
                u64::try_from(self.started.elapsed().as_nanos()).unwrap_or(u64::MAX),
                Ordering::Relaxed,
            );
            metrics.execution_outcome.store(self.outcome, Ordering::Relaxed);
        }
    }
}

impl MaterializationTrace {
    pub(crate) fn record(&self, rows: usize, report: MaterializationReport, elapsed: Duration) {
        self.metrics.materialized_batches.fetch_add(1, Ordering::Relaxed);
        self.metrics.materialized_rows.fetch_add(rows as u64, Ordering::Relaxed);
        self.metrics.materialized_shared_columns.fetch_add(report.shared_columns as u64, Ordering::Relaxed);
        self.metrics.materialized_gathered_columns.fetch_add(report.gathered_columns as u64, Ordering::Relaxed);
        self.metrics.materialized_empty_columns.fetch_add(report.empty_columns as u64, Ordering::Relaxed);
        self.metrics.materialized_output_buffer_bytes.fetch_add(report.gathered_output_buffer_bytes as u64, Ordering::Relaxed);
        let elapsed_ns = u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX);
        self.metrics.materialization_ns.fetch_add(elapsed_ns, Ordering::Relaxed);
        tracing::debug!(
            target: "vein.audit",
            event = "physical_operator_materialization",
            query_id = %self.query_id,
            task_id = %self.task_id,
            node_id = self.metrics.id,
            operator = self.metrics.operator,
            rows,
            shared_columns = report.shared_columns,
            gathered_columns = report.gathered_columns,
            empty_columns = report.empty_columns,
            materialized_output_buffer_bytes_estimated = report.gathered_output_buffer_bytes,
            materialization_ns = elapsed_ns,
            "physical_operator_materialization"
        );
    }
}

impl Drop for ExecutionTrace {
    fn drop(&mut self) {
        if tracing::enabled!(target: "vein.audit", tracing::Level::DEBUG) {
            self.log_summary();
        }
    }
}

struct ObservedOutputNode {
    next: Box<dyn PipelineNode<DataChunkView>>,
    metrics: Arc<NodeMetrics>,
    query_id: String,
    task_id: String,
}

impl PipelineNode<DataChunkView> for ObservedOutputNode {
    fn push(&mut self, view: DataChunkView) -> common::Result<()> {
        let rows = view.num_rows() as u64;
        self.metrics.output_rows.fetch_add(rows, Ordering::Relaxed);
        self.metrics.output_batches.fetch_add(1, Ordering::Relaxed);

        if tracing::enabled!(target: "vein.audit", tracing::Level::DEBUG) {
            let (array_count, array_bytes) = view.source_array_footprint();
            self.metrics
                .source_array_count_seen
                .fetch_add(array_count as u64, Ordering::Relaxed);
            self.metrics
                .source_array_bytes_seen
                .fetch_add(array_bytes as u64, Ordering::Relaxed);
            tracing::debug!(
                target: "vein.audit",
                event = "physical_operator_batch",
                query_id = %self.query_id,
                task_id = %self.task_id,
                node_id = self.metrics.id,
                operator = self.metrics.operator,
                rows,
                columns = view.num_columns(),
                source_array_count = array_count,
                source_array_footprint_bytes = array_bytes,
                "physical_operator_batch"
            );
        }

        let started = Instant::now();
        let result = self.next.push(view);
        self.metrics.downstream_push_ns.fetch_add(
            u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        result
    }

    fn flush(&mut self) -> common::Result<()> {
        self.next.flush()
    }

    fn finished(&self) -> bool {
        self.next.finished()
    }
}

fn operator_name(operator: &PhysicalPlanOperator) -> &'static str {
    match operator {
        PhysicalPlanOperator::Pipeline { .. } => "pipeline",
        PhysicalPlanOperator::Transform { .. } => "transform",
        PhysicalPlanOperator::Join { .. } => "join",
        PhysicalPlanOperator::Aggregate { .. } => "aggregate",
        PhysicalPlanOperator::Distinct { .. } => "distinct",
        PhysicalPlanOperator::Sort { .. } => "sort",
        PhysicalPlanOperator::Limit { .. } => "limit",
        PhysicalPlanOperator::Sink { .. } => "sink",
    }
}

fn operator_config(operator: &PhysicalPlanOperator) -> String {
    match operator {
        PhysicalPlanOperator::Pipeline { source, program, .. } => {
            let source_kind = match source.as_ref() {
                common::protocol::DataSource::File(_) => "file",
                common::protocol::DataSource::Stream(_) => "stream",
                common::protocol::DataSource::Memory => "memory",
            };
            format!("source={source_kind}, program_instructions={}", program.instructions.len())
        }
        PhysicalPlanOperator::Transform { program, .. } => {
            format!("program_instructions={}", program.instructions.len())
        }
        PhysicalPlanOperator::Join { plan, .. } => format!(
            "join_type={:?}, probe_keys={:?}, build_keys={:?}, outputs={:?}",
            plan.join_type, plan.probe_keys, plan.build_keys, plan.outputs
        ),
        PhysicalPlanOperator::Aggregate { plan, .. }
        | PhysicalPlanOperator::Distinct { plan, .. } => format!(
            "groups={}, aggregate_count={}, grouping_sets={}",
            plan.groups,
            plan.aggs.len(),
            plan.grouping_sets.as_ref().map_or(0, Vec::len)
        ),
        PhysicalPlanOperator::Sort { plan, key_program, output_columns, .. } => format!(
            "keys={:?}, computed_key_instructions={}, output_columns={output_columns}",
            plan.keys,
            key_program.as_ref().map_or(0, |program| program.instructions.len())
        ),
        PhysicalPlanOperator::Limit { spec, .. } => {
            format!("offset={}, count={}", spec.offset, spec.count)
        }
        PhysicalPlanOperator::Sink { sink, .. } => format!("sink={sink:?}"),
    }
}

fn children(operator: &PhysicalPlanOperator) -> Vec<&Arc<PhysicalPlanNode>> {
    match operator {
        PhysicalPlanOperator::Pipeline { .. } => Vec::new(),
        PhysicalPlanOperator::Transform { input, .. }
        | PhysicalPlanOperator::Aggregate { input, .. }
        | PhysicalPlanOperator::Distinct { input, .. }
        | PhysicalPlanOperator::Sort { input, .. }
        | PhysicalPlanOperator::Limit { input, .. }
        | PhysicalPlanOperator::Sink { input, .. } => vec![input],
        PhysicalPlanOperator::Join { probe, build, .. } => vec![probe, build],
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, Int64Array};
    use common::Result;
    use common::protocol::{
        AggOpCode, AggregatePlan, DataSource, JoinKind, JoinOutput, JoinPlan, LimitSpec,
        MicroProgram, SortKey, SortPlan,
    };
    use std::io::Write;
    use std::sync::Arc;
    use types::{VeinDataType, VeinSchema};

    fn node(operator: PhysicalPlanOperator) -> Arc<PhysicalPlanNode> {
        Arc::new(PhysicalPlanNode {
            output_schema: Arc::new(VeinSchema::empty()),
            operator,
        })
    }

    fn source() -> Arc<PhysicalPlanNode> {
        node(PhysicalPlanOperator::Pipeline {
            source: Arc::new(DataSource::Memory),
            input_schema: Arc::new(VeinSchema::empty()),
            program: Arc::new(MicroProgram::empty()),
        })
    }

    struct Discard {
        expected: ArrayRef,
    }

    impl PipelineNode<DataChunkView> for Discard {
        fn push(&mut self, batch: DataChunkView) -> Result<()> {
            let (array, _) = batch.resolve(0, 0)?.expect("source row remains present");
            assert!(Arc::ptr_eq(array, &self.expected), "observer must preserve source arrays");
            Ok(())
        }

        fn flush(&mut self) -> Result<()> {
            Ok(())
        }
    }

    #[test]
    fn canonical_node_ids_are_deterministic_preorder_ids() {
        let leaf = source();
        let root = node(PhysicalPlanOperator::Limit {
            input: Arc::clone(&leaf),
            spec: LimitSpec { offset: 1, count: 3 },
        });

        let first = ExecutionTrace::new(&root, "q".into(), "t".into());
        let second = ExecutionTrace::new(&root, "q".into(), "t".into());

        assert_eq!(first.node_id(&root), 0);
        assert_eq!(first.node_id(&leaf), 1);
        assert_eq!(first.node_id(&root), second.node_id(&root));
        assert_eq!(first.nodes[0].child_ids.get().unwrap(), &[1]);
        let explain = first.format_explain();
        assert_eq!(explain, second.format_explain(), "explain IDs and ordering are stable");
        assert!(explain.contains("#0 limit children=[1] [offset=1, count=3]"));
        assert!(explain.contains("#1 pipeline children=[] [source=memory, program_instructions=0]"));
        assert!(explain.contains("flow=reference-input->References"));
    }

    #[test]
    fn stable_explain_describes_chained_joins_aggregate_sort_and_boundaries() {
        let filtered = node(PhysicalPlanOperator::Transform {
            input: source(),
            program: Arc::new(MicroProgram::empty()),
        });
        let join_plan = || JoinPlan {
            join_type: JoinKind::Inner,
            probe_keys: vec![0],
            build_keys: vec![0],
            residual: None,
            outputs: vec![JoinOutput::Probe(0), JoinOutput::Build(0)],
            probe_types: vec![VeinDataType::Int64],
            build_types: vec![VeinDataType::Int64],
        };
        let first_join = node(PhysicalPlanOperator::Join {
            probe: filtered,
            build: source(),
            plan: join_plan(),
        });
        let second_join = node(PhysicalPlanOperator::Join {
            probe: first_join,
            build: source(),
            plan: join_plan(),
        });
        let aggregate = node(PhysicalPlanOperator::Aggregate {
            input: second_join,
            plan: AggregatePlan {
                groups: 0,
                estimated_groups: None,
                grouping_sets: Some(vec![Vec::new()]),
                emit_empty_group_on_empty_input: true,
                aggs: vec![AggOpCode::Count],
                aggregate_input_types: vec![VeinDataType::Int64],
                types: vec![VeinDataType::Int64],
            },
        });
        let root = node(PhysicalPlanOperator::Sort {
            input: aggregate,
            plan: SortPlan {
                keys: vec![SortKey {
                    column: 0,
                    ascending: false,
                    nulls_first: false,
                }],
            },
            key_program: None,
            output_columns: 1,
        });

        let trace = ExecutionTrace::new(&root, "query".into(), "task".into());
        let explain = trace.format_explain();

        assert_eq!(trace.nodes.len(), 8, "shared canonical nodes are visited once");
        assert_eq!(explain.matches(" join children=").count(), 2);
        assert!(explain.contains("probe_keys=[0], build_keys=[0]"));
        assert!(explain.contains("aggregate children="));
        assert!(explain.contains("sort children="));
        assert!(explain.contains("flow=materialize-input->ResultArrays"));
        assert!(explain.contains("keys=[SortKey { column: 0, ascending: false, nulls_first: false }]"));
    }

    #[test]
    fn output_observer_counts_batches_and_rows_without_materializing_the_view() {
        let source = source();
        let trace = ExecutionTrace::new(&source, "q".into(), "t".into());
        let node_id = trace.node_id(&source);
        let values: ArrayRef = Arc::new(Int64Array::from(vec![10, 20, 30]));
        let view = DataChunkView::from_chunk(crate::pipeline::DataChunk::from_vectors(vec![values]))
            .expect("reference-backed source view");
        let (array, _) = view.resolve(0, 0).expect("resolve source array").unwrap();
        let original = Arc::clone(array);
        let mut observed = trace.observer(
            node_id,
            Box::new(Discard { expected: original }),
            "q".into(),
            "t".into(),
        );

        observed.push(view).expect("observe output batch");

        let metrics = &trace.nodes[node_id as usize];
        assert_eq!(metrics.output_batches.load(Ordering::Relaxed), 1);
        assert_eq!(metrics.output_rows.load(Ordering::Relaxed), 3);
        trace.record_source_bytes(node_id, 128);
        assert_eq!(metrics.source_bytes_read.load(Ordering::Relaxed), 128);
    }

    #[test]
    fn trace_retains_the_query_resource_tracker_for_summary_emission() {
        let source = source();
        let mut trace = ExecutionTrace::new(&source, "q".into(), "t".into());
        let tracker = QueryMemoryTracker::unlimited(common::cancellation::CancellationToken::new());
        trace.attach_memory_tracker(tracker.clone());

        let attached = trace.memory.as_ref().expect("query tracker attached");
        assert_eq!(attached.identity(), tracker.identity());
        assert_eq!(attached.limit(), usize::MAX);
        assert_eq!(attached.used(), 0);
        assert_eq!(attached.peak(), 0);
        assert_eq!(attached.spill_bytes_used(), 0);
    }

    #[test]
    fn node_trace_attributes_time_memory_spill_and_materialization() {
        let root = source();
        let trace = ExecutionTrace::new(&root, "query".into(), "task".into());
        let node_id = trace.node_id(&root);
        let metrics = &trace.nodes[node_id as usize];
        let query_memory = QueryMemoryTracker::unlimited(common::cancellation::CancellationToken::new());
        let operator_memory = trace.operator_memory(node_id, &query_memory);

        let reservation = operator_memory.reserve(64, "trace test state").expect("reserve operator state");
        assert_eq!(query_memory.used(), 64, "query-wide limit still accounts scoped reservations");
        assert_eq!(metrics.resources.memory_used(), 64);
        assert_eq!(metrics.resources.memory_peak(), 64);

        let manager = operator_memory.spill_manager();
        let mut pending = manager.create_run().expect("create scoped spill run");
        let mut writer = pending.writer().expect("open scoped spill writer");
        writer.write_all(b"spill").expect("write scoped spill bytes");
        let run = pending.commit(writer).expect("commit scoped spill run");
        assert_eq!(metrics.resources.spill_used(), 5);
        assert_eq!(metrics.resources.spill_peak(), 5);
        assert_eq!(metrics.resources.spill_written(), 5);

        let report = MaterializationReport {
            shared_columns: 1,
            gathered_columns: 2,
            empty_columns: 0,
            gathered_output_buffer_bytes: 128,
        };
        trace.materialization_handle(node_id).expect("node trace handle")
            .record(3, report, Duration::from_nanos(20));
        assert_eq!(metrics.materialized_rows.load(Ordering::Relaxed), 3);
        assert_eq!(metrics.materialized_gathered_columns.load(Ordering::Relaxed), 2);
        assert_eq!(metrics.materialized_output_buffer_bytes.load(Ordering::Relaxed), 128);
        assert_eq!(metrics.materialization_ns.load(Ordering::Relaxed), 20);

        let mut guard = trace.execution_guard(node_id);
        // An immediately dropped timer may share the same clock tick. Give the
        // guard a known start interval without sleeping or changing runtime time.
        guard.started = Instant::now().checked_sub(Duration::from_millis(1))
            .expect("clock supports the test interval");
        guard.finish(&Ok(1));
        drop(guard);
        assert!(metrics.inclusive_elapsed_ns.load(Ordering::Relaxed) > 0);
        assert_eq!(metrics.execution_outcome.load(Ordering::Relaxed), 1);

        drop(run);
        drop(reservation);
        assert_eq!(metrics.resources.spill_used(), 0);
        assert_eq!(metrics.resources.spill_written(), 5, "cumulative writes survive run cleanup");
        assert_eq!(metrics.resources.memory_used(), 0);
        assert_eq!(query_memory.used(), 0);
    }

    #[test]
    fn dropped_node_execution_guard_records_cancel_or_drop_outcome_and_elapsed_time() {
        let root = source();
        let trace = ExecutionTrace::new(&root, "query".into(), "task".into());
        let node_id = trace.node_id(&root);
        let guard = trace.execution_guard(node_id);
        std::thread::yield_now();
        drop(guard);

        let metrics = &trace.nodes[node_id as usize];
        assert!(metrics.inclusive_elapsed_ns.load(Ordering::Relaxed) > 0);
        assert_eq!(metrics.execution_outcome.load(Ordering::Relaxed), 4);
    }

    #[test]
    fn canceled_node_execution_is_distinguished_from_failed_execution() {
        let root = source();
        let trace = ExecutionTrace::new(&root, "query".into(), "task".into());
        let node_id = trace.node_id(&root);
        let mut guard = trace.execution_guard(node_id);
        guard.finish(&Err(error::VeinError::cancelled()));
        drop(guard);

        let metrics = &trace.nodes[node_id as usize];
        assert_eq!(metrics.execution_outcome.load(Ordering::Relaxed), 2);
    }
}
