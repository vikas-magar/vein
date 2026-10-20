//! Query-scoped, low-overhead physical execution tracing.
//!
//! Node identifiers are assigned by deterministic preorder over the canonical
//! plan. Batch observations count reference-backed source arrays without
//! materializing them; repeated bytes across nodes therefore describe data flow,
//! not unique resident memory.

use std::collections::HashMap;
use std::sync::{Arc, OnceLock};
use std::time::Instant;

use common::protocol::{PhysicalPlanNode, PhysicalPlanOperator};
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

    fn visit(&mut self, node: &Arc<PhysicalPlanNode>) -> u32 {
        let identity = Arc::as_ptr(node) as usize;
        if let Some(id) = self.node_ids.get(&identity) {
            return *id;
        }

        let id = u32::try_from(self.nodes.len()).unwrap_or(u32::MAX);
        let statistics = self.statistics_context.register_operator(
            OperatorStatisticsIdentity {
                node_id: id,
                task_id: self.task_identity,
            },
            operator_name(&node.operator),
        );
        let metrics = Arc::new(NodeMetrics {
            node: Arc::clone(node),
            id,
            operator: operator_name(&node.operator),
            operator_config: operator_config(&node.operator),
            statistics: Arc::clone(&statistics),
            child_ids: OnceLock::new(),
            source_array_count_seen: statistics.register_counter("source_array_count_seen"),
            source_array_bytes_seen: statistics.register_counter("source_array_bytes_seen"),
            downstream_push_ns: statistics.register_counter("downstream_push_ns"),
            execution_completed: statistics.register_counter("execution_completed"),
            execution_cancelled: statistics.register_counter("execution_cancelled"),
            execution_failed: statistics.register_counter("execution_failed"),
            execution_dropped: statistics.register_counter("execution_dropped"),
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
            let schema = metrics
                .node
                .output_schema
                .fields
                .iter()
                .map(|field| {
                    format!(
                        "{}:{:?}{}",
                        field.name,
                        field.data_type,
                        if field.nullable { "?" } else { "" }
                    )
                })
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
        self.nodes
            .iter()
            .map(|metrics| {
                let children = metrics
                    .child_ids
                    .get()
                    .map(Vec::as_slice)
                    .unwrap_or_default();
                let schema = metrics
                    .node
                    .output_schema
                    .fields
                    .iter()
                    .map(|field| {
                        let qualifier = field
                            .qualifier
                            .as_deref()
                            .map(|name| format!("{name}."))
                            .unwrap_or_default();
                        format!(
                            "{qualifier}{}:{:?}{}",
                            field.name,
                            field.data_type,
                            if field.nullable { "?" } else { "" }
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
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
            })
            .collect::<Vec<_>>()
            .join("\n")
    }

    fn log_summary(&self) {
        for node in &self.nodes {
            let output_rows = node.statistics.output_rows();
            let input_rows = node
                .child_ids
                .get()
                .into_iter()
                .flatten()
                .filter_map(|id| self.nodes.get(*id as usize))
                .map(|child| child.statistics.output_rows())
                .fold(0u64, u64::saturating_add);
            let input_array_count_seen = node
                .child_ids
                .get()
                .into_iter()
                .flatten()
                .filter_map(|id| self.nodes.get(*id as usize))
                .map(|child| child.source_array_count_seen.value())
                .fold(0u64, u64::saturating_add);
            let input_array_footprint_bytes_seen = node
                .child_ids
                .get()
                .into_iter()
                .flatten()
                .filter_map(|id| self.nodes.get(*id as usize))
                .map(|child| child.source_array_bytes_seen.value())
                .fold(0u64, u64::saturating_add);
            let execution_outcome = execution_outcome(node);
            let operator_statistics = node.statistics.snapshot(OperatorStatisticsIdentity {
                node_id: node.id,
                task_id: self.task_identity,
            });
            let row_change = i128::from(output_rows) - i128::from(input_rows);
            let output_input_ratio =
                (input_rows > 0).then_some(output_rows as f64 / input_rows as f64);
            tracing::info!(
                target: "vein.statistics",
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
                row_change,
                output_input_ratio = ?output_input_ratio,
                output_batches = node.statistics.output_batches(),
                operator_counters = ?operator_statistics.metrics,
                source_bytes_read = node.statistics.source_bytes_read(),
                output_array_count_seen = node.source_array_count_seen.value(),
                output_array_footprint_bytes_seen = node.source_array_bytes_seen.value(),
                array_footprint_scope = "arrow_reported_sizes_seen_at_operator_boundaries; shared_buffers_and_repeated_flow_are_counted_again",
                inclusive_elapsed_ns = node.statistics.elapsed_ns(),
                execution_outcome,
                downstream_push_ns = node.downstream_push_ns.value(),
                operator_memory_reserved_bytes = node.resources.memory_used(),
                operator_memory_peak_reserved_bytes = node.resources.memory_peak(),
                operator_spill_disk_bytes = node.resources.spill_used(),
                operator_spill_disk_peak_bytes = node.resources.spill_peak(),
                operator_spill_bytes_written = node.resources.spill_written(),
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
                        totals.0.saturating_add(node.statistics.source_bytes_read()),
                        totals
                            .1
                            .saturating_add(node.source_array_count_seen.value()),
                        totals
                            .2
                            .saturating_add(node.source_array_bytes_seen.value()),
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
            Err(error::VeinError::Cancelled) => 2,
            Err(_) => 3,
        };
    }
}

impl Drop for NodeExecutionGuard {
    fn drop(&mut self) {
        if let Some(metrics) = &self.metrics {
            metrics.statistics.record_elapsed_ns(
                u64::try_from(self.started.elapsed().as_nanos()).unwrap_or(u64::MAX),
            );
            match self.outcome {
                1 => metrics.execution_completed.add(1),
                2 => metrics.execution_cancelled.add(1),
                3 => metrics.execution_failed.add(1),
                _ => metrics.execution_dropped.add(1),
            }
        }
    }
}

fn execution_outcome(metrics: &NodeMetrics) -> &'static str {
    if metrics.execution_completed.value() > 0 {
        "complete"
    } else if metrics.execution_cancelled.value() > 0 {
        "cancelled"
    } else if metrics.execution_failed.value() > 0 {
        "failed"
    } else if metrics.execution_dropped.value() > 0 {
        "cancelled_or_dropped"
    } else {
        "not_started"
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
        self.metrics.statistics.record_output(rows);

        if tracing::enabled!(target: "vein.audit", tracing::Level::DEBUG) {
            let (array_count, array_bytes) = view.source_array_footprint();
            self.metrics.source_array_count_seen.add(array_count as u64);
            self.metrics.source_array_bytes_seen.add(array_bytes as u64);
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
        self.metrics
            .downstream_push_ns
            .add(u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX));
        result
    }

    fn flush(&mut self) -> common::Result<()> {
        self.next.flush()
    }

    fn finished(&self) -> bool {
        self.next.finished()
    }

    fn fork_local(&self) -> common::Result<Option<Box<dyn PipelineNode<DataChunkView>>>> {
        let Some(next) = self.next.fork_local()? else {
            return Ok(None);
        };
        Ok(Some(Box::new(Self {
            next,
            metrics: Arc::clone(&self.metrics),
            query_id: self.query_id.clone(),
            task_id: self.task_id.clone(),
        })))
    }

    fn combine_local(
        self: Box<Self>,
        canonical: &mut dyn PipelineNode<DataChunkView>,
    ) -> common::Result<()> {
        let target = canonical
            .as_any_mut()
            .downcast_mut::<ObservedOutputNode>()
            .ok_or_else(|| {
                error::VeinError::internal("trace local state has a mismatched target")
            })?;
        if !Arc::ptr_eq(&self.metrics, &target.metrics)
            || self.query_id != target.query_id
            || self.task_id != target.task_id
        {
            return Err(error::VeinError::internal(
                "cannot combine trace nodes from different executions",
            ));
        }
        self.next.combine_local(target.next.as_mut())
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
        PhysicalPlanOperator::Pipeline {
            source, program, ..
        } => {
            let source_kind = match source.as_ref() {
                common::protocol::DataSource::File(_) => "file",
                common::protocol::DataSource::Stream(_) => "stream",
                common::protocol::DataSource::Memory => "memory",
                common::protocol::DataSource::Intermediate(_) => "intermediate",
            };
            format!(
                "source={source_kind}, expression_steps={}",
                program.steps.len()
            )
        }
        PhysicalPlanOperator::Transform { program, .. } => {
            format!("expression_steps={}", program.steps.len())
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
        PhysicalPlanOperator::Sort {
            plan,
            key_program,
            output_columns,
            ..
        } => format!(
            "keys={:?}, computed_key_steps={}, output_columns={output_columns}",
            plan.keys,
            key_program
                .as_ref()
                .map_or(0, |program| program.steps.len())
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
        AggOpCode, AggregatePlan, DataSource, ExpressionProgram, JoinKind, JoinOutput, JoinPlan,
        LimitSpec, SortKey, SortPlan,
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
            program: Arc::new(ExpressionProgram::empty()),
        })
    }

    struct Discard {
        expected: ArrayRef,
    }

    impl PipelineNode<DataChunkView> for Discard {
        fn push(&mut self, batch: DataChunkView) -> Result<()> {
            let (array, _) = batch.resolve(0, 0)?.expect("source row remains present");
            assert!(
                Arc::ptr_eq(array, &self.expected),
                "observer must preserve source arrays"
            );
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
            spec: LimitSpec {
                offset: 1,
                count: 3,
            },
        });

        let first = ExecutionTrace::new(&root, "q".into(), "t".into());
        let second = ExecutionTrace::new(&root, "q".into(), "t".into());

        assert_eq!(first.node_id(&root), 0);
        assert_eq!(first.node_id(&leaf), 1);
        assert_eq!(first.node_id(&root), second.node_id(&root));
        assert_eq!(first.nodes[0].child_ids.get().unwrap(), &[1]);
        let explain = first.format_explain();
        assert_eq!(
            explain,
            second.format_explain(),
            "explain IDs and ordering are stable"
        );
        assert!(explain.contains("#0 limit children=[1] [offset=1, count=3]"));
        assert!(
            explain.contains("#1 pipeline children=[] [source=memory, program_instructions=0]")
        );
        assert!(explain.contains("flow=reference-input->References"));
    }

    #[test]
    fn stable_explain_describes_chained_joins_aggregate_sort_and_boundaries() {
        let filtered = node(PhysicalPlanOperator::Transform {
            input: source(),
            program: Arc::new(ExpressionProgram::empty()),
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

        assert_eq!(
            trace.nodes.len(),
            8,
            "shared canonical nodes are visited once"
        );
        assert_eq!(explain.matches(" join children=").count(), 2);
        assert!(explain.contains("probe_keys=[0], build_keys=[0]"));
        assert!(explain.contains("aggregate children="));
        assert!(explain.contains("sort children="));
        assert!(explain.contains("flow=kernel-columns->ResultArrays"));
        assert!(
            explain.contains("keys=[SortKey { column: 0, ascending: false, nulls_first: false }]")
        );
    }

    #[test]
    fn output_observer_counts_batches_and_rows_without_materializing_the_view() {
        let source = source();
        let trace = ExecutionTrace::new(&source, "q".into(), "t".into());
        let node_id = trace.node_id(&source);
        let values: ArrayRef = Arc::new(Int64Array::from(vec![10, 20, 30]));
        let view = crate::pipeline::test_support::dense_view(vec![values]);
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
        assert_eq!(metrics.statistics.output_batches(), 1);
        assert_eq!(metrics.statistics.output_rows(), 3);
        trace.record_source_bytes(node_id, 128);
        assert_eq!(metrics.statistics.source_bytes_read(), 128);
        assert_eq!(
            trace.statistics_context.operator_statistics()[0].output_rows,
            3
        );
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
    fn node_trace_attributes_time_memory_and_spill() {
        let root = source();
        let trace = ExecutionTrace::new(&root, "query".into(), "task".into());
        let node_id = trace.node_id(&root);
        let metrics = &trace.nodes[node_id as usize];
        let query_memory =
            QueryMemoryTracker::unlimited(common::cancellation::CancellationToken::new());
        let operator_memory = trace.operator_memory(node_id, &query_memory);

        let reservation = operator_memory
            .reserve(64, "trace test state")
            .expect("reserve operator state");
        assert_eq!(
            query_memory.used(),
            64,
            "query-wide limit still accounts scoped reservations"
        );
        assert_eq!(metrics.resources.memory_used(), 64);
        assert_eq!(metrics.resources.memory_peak(), 64);

        let manager = operator_memory.spill_manager();
        let mut pending = manager.create_run().expect("create scoped spill run");
        let mut writer = pending.writer().expect("open scoped spill writer");
        writer
            .write_all(b"spill")
            .expect("write scoped spill bytes");
        let run = pending.commit(writer).expect("commit scoped spill run");
        assert_eq!(metrics.resources.spill_used(), 5);
        assert_eq!(metrics.resources.spill_peak(), 5);
        assert_eq!(metrics.resources.spill_written(), 5);

        let mut guard = trace.execution_guard(node_id);
        // An immediately dropped timer may share the same clock tick. Give the
        // guard a known start interval without sleeping or changing runtime time.
        guard.started = Instant::now()
            .checked_sub(std::time::Duration::from_millis(1))
            .expect("clock supports the test interval");
        guard.finish(&Ok(1));
        drop(guard);
        assert!(metrics.statistics.elapsed_ns() > 0);
        assert_eq!(metrics.execution_completed.value(), 1);
        let snapshot = trace.statistics_context.operator_statistics();
        assert_eq!(snapshot[0].metrics["execution_completed"], 1);

        drop(run);
        drop(reservation);
        assert_eq!(metrics.resources.spill_used(), 0);
        assert_eq!(
            metrics.resources.spill_written(),
            5,
            "cumulative writes survive run cleanup"
        );
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
        assert!(metrics.statistics.elapsed_ns() > 0);
        assert_eq!(metrics.execution_dropped.value(), 1);
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
        assert_eq!(metrics.execution_cancelled.value(), 1);
    }
}
