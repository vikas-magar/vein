use super::*;

impl ExecutionTrace {
    pub(super) fn log_summary(&self) {
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
            tracing::debug!(
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

impl Drop for ExecutionTrace {
    fn drop(&mut self) {
        if tracing::enabled!(target: "vein.statistics", tracing::Level::DEBUG) {
            self.log_summary();
        }
    }
}
