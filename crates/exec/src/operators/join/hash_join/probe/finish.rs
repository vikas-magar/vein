use super::*;

impl HashJoin {
    pub(super) fn finish(&mut self) -> Result<()> {
        // An outer join owes the build rows nothing matched. They are emitted
        // here, once, with the probe side null.
        if self.emit_unmatched_build && self.join_type.tracks_build_matches() {
            let table = Arc::clone(&self.table);
            // Retain only one bounded row-mapping batch, never all build rows.
            let mut rows = Vec::with_capacity(self.build_batch_rows);
            let mut budget = super::super::super::hash_table::probe::control::WorkBudget::for_batch(self.build_batch_rows);
            for build in table.build_rows() {
                if budget.should_poll() {
                    table.memory_tracker().check_cancelled()?;
                    if self.next.finished() { break; }
                }
                if self.build_row_matched(build.0, build.1)? == (self.join_type == JoinType::RightSemi) {
                    rows.push(OutputRow { probe: None, build: Some(build) });
                    if rows.len() == self.build_batch_rows {
                        self.forward_rows(None, rows.drain(..))?;
                    }
                }
            }
            self.forward_rows(None, rows)?;
        }
        tracing::info!(
            target: "vein.audit",
            event = "join_finished",
            probed_rows = self.probed_rows,
            matched_rows = self.matched_rows,
            candidate_rows_examined = self.candidate_rows_examined,
            probe_hash_rows = self.probe_metrics.hash_rows,
            bucket_lookups = self.probe_metrics.bucket_lookups,
            hash_rejections = self.probe_metrics.hash_rejections,
            key_comparisons = self.probe_metrics.key_comparisons,
            key_rejections = self.probe_metrics.key_rejections,
            control_polls = self.probe_metrics.control_polls,
            hash_windows = self.probe_metrics.hash_windows,
            hash_us = self.probe_metrics.hash_ns / 1000,
            bucket_lookup_us = self.probe_metrics.bucket_ns / 1000,
            candidate_scan_us = self.probe_metrics.lookup_ns / 1000,
            max_candidate_batch_rows = self.max_candidate_batch_rows,
            output_batches = self.output_batches,
            output_rows = self.output_rows,
            max_output_batch_rows = self.max_output_batch_rows,
            probe_us = self.probe_us,
            view_compose_us = self.view_compose_us,
            downstream_push_us = self.downstream_push_us,
            timing_scope = "sum_of_worker_elapsed_times",
            residual_us = self.residual_us,
            key_shared_columns = self.probe_key_materialization.shared_columns,
            key_gathered_columns = self.probe_key_materialization.gathered_columns,
            key_empty_columns = self.probe_key_materialization.empty_columns,
            key_gathered_output_buffer_bytes = self.probe_key_materialization.gathered_output_buffer_bytes,
            key_materialize_us = self.probe_key_materialize_us,
            "join_finished"
        );
        self.next.flush()
    }
}
