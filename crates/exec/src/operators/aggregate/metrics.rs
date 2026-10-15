/// Low-overhead counters and phase timings for one aggregate sink.
#[derive(Clone, Copy, Default)]
pub(crate) struct AggregateMetrics {
    pub(crate) input_batches: u64,
    pub(crate) input_rows: u64,
    pub(crate) grouping_sets: u64,
    pub(crate) grouping_probes: u64,
    pub(crate) group_index_candidate_checks: u64,
    pub(crate) group_index_reserve_ns: u64,
    pub(crate) group_index_capacity_growth_events: u64,
    pub(crate) group_index_entries_at_capacity_growth: u64,
    pub(crate) group_index_capacity: u64,
    pub(crate) group_index_entries_ns: u64,
    pub(crate) full_hash_matches: u64,
    pub(crate) group_key_comparisons: u64,
    pub(crate) group_collision_retries: u64,
    pub(crate) group_index_hits: u64,
    pub(crate) group_index_misses: u64,
    pub(crate) new_groups: u64,
    pub(crate) retained_key_slots_initialized: u64,
    pub(crate) string_key_bytes_copied: u64,
    pub(crate) hasher_setup_ns: u64,
    pub(crate) key_hash_ns: u64,
    pub(crate) hash_finalize_ns: u64,
    pub(crate) hash_generation_ns: u64,
    /// Resolving candidates to group slots, including collision checks and new groups.
    pub(crate) group_resolution_ns: u64,
    /// Copying each batch's newly discovered key rows into owned group state.
    pub(crate) group_materialize_ns: u64,
    pub(crate) group_ids_distribution_ns: u64,
    pub(crate) state_update_ns: u64,
    pub(crate) output_materialize_ns: u64,
}

impl AggregateMetrics {
    pub(super) fn accumulate(&mut self, other: Self) {
        macro_rules! add_counters {
            ($($field:ident),+ $(,)?) => {
                $(self.$field = self.$field.saturating_add(other.$field);)+
            };
        }
        add_counters!(
            input_batches,
            input_rows,
            grouping_sets,
            grouping_probes,
            group_index_candidate_checks,
            group_index_reserve_ns,
            group_index_capacity_growth_events,
            group_index_entries_at_capacity_growth,
            group_index_capacity,
            group_index_entries_ns,
            full_hash_matches,
            group_key_comparisons,
            group_collision_retries,
            group_index_hits,
            group_index_misses,
            new_groups,
            retained_key_slots_initialized,
            string_key_bytes_copied,
            hasher_setup_ns,
            key_hash_ns,
            hash_finalize_ns,
            hash_generation_ns,
            group_resolution_ns,
            group_materialize_ns,
            group_ids_distribution_ns,
            state_update_ns,
            output_materialize_ns,
        );
    }
}
