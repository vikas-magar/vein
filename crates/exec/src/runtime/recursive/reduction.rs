use super::*;
use common::protocol::AggregatePlan;

impl DefaultTaskExecutor {
    pub(super) async fn execute_reduction(
        &self, child: &Arc<PhysicalPlanNode>, plan: &AggregatePlan,
        mut next: Box<dyn PipelineNode>, state: NodeExecution<'_>,
    ) -> Result<u64> {
        let NodeExecution { node: input, context, depth } = state;
        let RecursiveExecutionContext { memory, trace, schedule, .. } = *context;
        let node_id = trace.node_id(input);
        let operator_memory = trace.operator_memory(node_id, memory);
        let operator =
            if matches!(&input.operator, PhysicalPlanOperator::Distinct { .. }) {
                "distinct"
            } else {
                "aggregate"
            };
        let registry = self.builder.function_registry()
            .unwrap_or_else(|| Arc::new(functions::FunctionRegistry::with_builtins()));
        let (accumulator, handle) = aggregate::shared_with_registry_and_memory_tracker(
            plan.clone(),
            operator_memory,
            registry,
        )?;
        let bytes = self
            .execute_input(
                child,
                handle,
                context,
                depth + 1,
            )
            .await?;
        let boundary =
            if matches!(&input.operator, PhysicalPlanOperator::Distinct { .. }) {
                PipelineBoundaryKind::DistinctInput
            } else {
                PipelineBoundaryKind::AggregateInput
            };
        schedule.complete_boundary(input, boundary)?;
        let (metrics, group_count) = {
            let mut accumulator = Arc::try_unwrap(accumulator)
                .map_err(|_| VeinError::internal("aggregate input still has active owners"))?
                .into_inner()
                .map_err(|_| VeinError::internal("aggregate mutex poisoned"))?;
            accumulator.finish_with(
                aggregate::AGGREGATE_BATCH_ROWS,
                crate::pipeline::delivery::PipelineConsumer(next.as_mut()),
            ).await?;
            (accumulator.metrics(), accumulator.group_count())
        };
        next.flush().await?;
        tracing::info!(
target: "vein.audit",
event = "recursive_aggregate_finished",
operator,
depth,
input_bytes = bytes,
input_batches = metrics.input_batches,
input_rows = metrics.input_rows,
grouping_sets = metrics.grouping_sets,
grouping_probes = metrics.grouping_probes,
group_index_candidate_checks = metrics.group_index_candidate_checks,
group_index_reserve_ns = metrics.group_index_reserve_ns,
group_index_capacity_growth_events = metrics.group_index_capacity_growth_events,
group_index_entries_at_capacity_growth = metrics.group_index_entries_at_capacity_growth,
group_index_capacity = metrics.group_index_capacity,
group_index_entries_ns = metrics.group_index_entries_ns,
full_hash_matches = metrics.full_hash_matches,
group_key_comparisons = metrics.group_key_comparisons,
group_collision_retries = metrics.group_collision_retries,
group_index_hits = metrics.group_index_hits,
group_index_misses = metrics.group_index_misses,
new_groups = metrics.new_groups,
groups = group_count,
retained_key_slots_initialized = metrics.retained_key_slots_initialized,
string_key_bytes_copied = metrics.string_key_bytes_copied,
hasher_setup_ns = metrics.hasher_setup_ns,
key_hash_ns = metrics.key_hash_ns,
hash_finalize_ns = metrics.hash_finalize_ns,
hash_generation_ns = metrics.hash_generation_ns,
group_resolution_ns = metrics.group_resolution_ns,
group_materialize_ns = metrics.group_materialize_ns,
group_ids_distribution_ns = metrics.group_ids_distribution_ns,
state_update_ns = metrics.state_update_ns,
output_materialize_ns = metrics.output_materialize_ns,
"recursive_aggregate_finished"
                    );
        Ok(bytes)
    }
}
