use super::super::*;
use super::join_runtime_filter_context::JoinRuntimeFilterContext;

impl DefaultTaskExecutor {
    /// Build probe filters from the completed join and current probe metadata.
    /// Build probe-side runtime filters only when the join key is still a direct
    /// projected scan column. Unknown lineage is a reason to keep exact execution.
    pub(in crate::runtime) fn runtime_filters_for_join(
        &self,
        probe: &PhysicalPlanNode,
        plan: &JoinPlan,
        build: &BuildResult,
        memory: &QueryMemoryTracker,
        trace: &ExecutionTrace,
        node_id: u32,
    ) -> Result<Option<Arc<RuntimeFilterSet>>> {
        if !plan.join_type.allows_probe_filtering() {
            return Ok(None);
        }
        let BuildResult::InMemory(table) = build else {
            tracing::debug!(
                target: "vein.audit",
                event = "join_runtime_filter_skipped",
                node_id,
                reason = "spilled_build",
                "join runtime filter skipped"
            );
            return Ok(None);
        };
        let Some((source, columns)) = direct_scan_probe_columns(probe, plan) else {
            tracing::debug!(
                target: "vein.audit",
                event = "join_runtime_filter_skipped",
                node_id,
                reason = "probe_lineage_or_scan_unsupported",
                "join runtime filter skipped"
            );
            return Ok(None);
        };
        if !self
            .builder
            .supports_runtime_filter_pushdown(source, &columns)
        {
            tracing::debug!(
                target: "vein.audit",
                event = "join_runtime_filter_skipped",
                node_id,
                reason = "source_adapter_unsupported",
                "join runtime filter skipped"
            );
            return Ok(None);
        }
        let Some(factory) = self.builder.runtime_filter_factory() else {
            tracing::debug!(
                target: "vein.audit",
                event = "join_runtime_filter_skipped",
                node_id,
                reason = "factory_unavailable",
                "join runtime filter skipped"
            );
            return Ok(None);
        };
        let build_hashes = table.runtime_filter_hashes();
        let JoinRuntimeFilterContext {
            context,
            effective_probe_rows,
            probe_rows_source,
            source_statistics_count,
            runtime_scan_observation_count,
            runtime_operators_with_output,
            runtime_operator_output_rows,
            runtime_operator_output_batches,
            runtime_source_bytes_observed,
        } = JoinRuntimeFilterContext::collect(
            source,
            table.len() as u64,
            build_hashes.len(),
            memory,
            trace,
        );
        let policy = self.builder.runtime_filter_decision_policy();
        if !policy.should_build_candidates(&context) {
            tracing::info!(
                target: "vein.statistics",
                event = "join_runtime_filter_decision",
                node_id,
                build_rows = context.build_rows,
                build_keys = context.build_keys,
                probe_rows_estimate = ?context.probe_rows_estimate,
                effective_probe_rows,
                probe_rows_source,
                available_memory_bytes = context.available_memory_bytes,
                source_statistics_count,
                runtime_scan_observation_count,
                runtime_operators_with_output,
                runtime_operator_output_rows,
                runtime_operator_output_batches,
                runtime_source_bytes_observed,
                candidate_construction = "skipped",
                selected_filters = 0usize,
                reason = "policy_declined_setup",
                "join runtime filter decision"
            );
            return Ok(None);
        }

        let build_key_bounds = table.runtime_key_bounds()?;
        let bounded_key_columns = build_key_bounds
            .iter()
            .filter(|bounds| bounds.is_some())
            .count();
        let filters = factory.build_filters(RuntimeFilterBuildInput {
            probe_columns: &columns,
            build_hashes,
            build_key_bounds,
            memory,
        })?;
        if filters.is_empty() {
            tracing::debug!(
                target: "vein.audit",
                event = "join_runtime_filter_skipped",
                node_id,
                reason = "factory_declined",
                build_hashes = build_hashes.len(),
                "join runtime filter skipped"
            );
            return Ok(None);
        }

        let candidate_names = filters
            .iter()
            .map(|filter| filter.name())
            .collect::<Vec<_>>();
        let selected_indices = policy.select_filters(&context, &filters)?;
        let statistics = trace
            .operator_statistics_handle(node_id)
            .ok_or_else(|| VeinError::internal("join node is missing query statistics"))?;
        let selected_filters = RuntimeFilterSet::from_selection_with_statistics(
            &filters,
            &selected_indices,
            statistics,
        )?;
        if selected_filters.is_empty() {
            tracing::info!(
                target: "vein.statistics",
                event = "join_runtime_filter_decision",
                node_id,
                build_rows = context.build_rows,
                build_keys = context.build_keys,
                probe_rows_estimate = ?context.probe_rows_estimate,
                effective_probe_rows,
                probe_rows_source,
                available_memory_bytes = context.available_memory_bytes,
                source_statistics_count,
                runtime_scan_observation_count,
                runtime_operators_with_output,
                runtime_operator_output_rows,
                runtime_operator_output_batches,
                runtime_source_bytes_observed,
                candidate_construction = "completed",
                candidate_filters = ?candidate_names,
                selected_filters = 0usize,
                reason = "policy_selected_no_filters",
                "join runtime filter decision"
            );
            return Ok(None);
        }

        let filters = Arc::new(selected_filters);
        tracing::info!(
            target: "vein.statistics",
            event = "join_runtime_filter_decision",
            node_id,
            build_rows = context.build_rows,
            build_keys = context.build_keys,
            probe_rows_estimate = ?context.probe_rows_estimate,
            effective_probe_rows,
            probe_rows_source,
            available_memory_bytes = context.available_memory_bytes,
            source_statistics_count,
            runtime_scan_observation_count,
            runtime_operators_with_output,
            runtime_operator_output_rows,
            runtime_operator_output_batches,
            runtime_source_bytes_observed,
            candidate_construction = "completed",
            candidate_filters = ?candidate_names,
            filter_names = ?filters.names(),
            selected_filters = filters.len(),
            key_columns = columns.len(),
            indexed_build_keys = build_hashes.len(),
            bounded_key_columns,
            filter_bytes = filters.memory_bytes(),
            reason = "policy_selected_candidates",
            "join runtime filter decision"
        );
        Ok(Some(filters))
    }
}
