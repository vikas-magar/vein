use super::*;

impl AggregateSink {
    pub(super) fn release_group_state(&mut self) {
        self.index = GroupIndex::default();
        self.groups = Vec::new();
        self.group_key_batches = Vec::new();
        self.group_key_batch_arrays = Vec::new();
        self.group_key_batch_leases = Vec::new();
        self.aggregate_states = self
            .plan
            .aggs
            .iter()
            .zip(self.plan.aggregate_input_types.iter())
            .zip(self.plan.types.iter().skip(self.plan.groups as usize))
            .map(|((op, input_type), result_type)| {
                AggregateStateColumn::new(*op, input_type, result_type)
            })
            .collect::<Result<Vec<_>>>()
            .expect("validated aggregate plan can recreate empty state columns");
        for group_ids in &mut self.group_ids_by_set {
            *group_ids = Vec::new();
        }
        self.batch_group_ids = Vec::new();
        self.batch_new_groups = Vec::new();
        let reserved = self.memory.bytes();
        self.memory.shrink(reserved);
    }

    pub(crate) fn metrics(&self) -> AggregateMetrics {
        self.metrics
    }

    /// Collects the finished groups for callers that need an owned result list.
    /// Runtime operators should use `finish_with` so output batches are forwarded
    /// and released one at a time.
    pub fn finish(&mut self) -> Result<Vec<DataChunk>> {
        let mut output = Vec::new();
        self.finish_with(AGGREGATE_BATCH_ROWS, |batch| {
            output.push(batch);
            Ok(())
        })?;
        Ok(output)
    }

    /// Emits bounded output chunks while retaining only the aggregate state.
    /// The callback runs synchronously for each chunk and can stop emission by
    /// returning an error.
    pub fn finish_with(
        &mut self,
        batch_rows: NonZeroUsize,
        mut emit: impl FnMut(DataChunk) -> Result<()>,
    ) -> Result<()> {
        self.finish_view_with(batch_rows, |view| emit(view.to_data_chunk()?))
    }

    /// Emits aggregate batches as views over retained grouping keys and newly
    /// computed aggregate result arrays.
    pub fn finish_view_with(
        &mut self,
        batch_rows: NonZeroUsize,
        mut emit: impl FnMut(DataChunkView) -> Result<()>,
    ) -> Result<()> {
        if !self.distinct_collectors.is_empty() {
            self.finish_distinct_counts(batch_rows)?;
        }
        if self.spill_runs.is_empty() {
            return self.finish_in_memory_view_with(batch_rows, &mut emit);
        }
        self.flush_current_run()?;
        let runs = std::mem::take(&mut self.spill_runs);
        Self::finish_spill_partition_view(
            &self.plan,
            &self.tracker,
            &self.spill_schema,
            runs,
            0,
            batch_rows,
            &mut emit,
        )
    }

    pub(super) fn ensure_empty_groups(&mut self) -> Result<()> {
        if self.groups.is_empty() && self.plan.emit_empty_group_on_empty_input {
            let empty_sets: Vec<usize> = self
                .grouping_sets
                .iter()
                .enumerate()
                .filter_map(|(set_id, grouping_set)| grouping_set.is_empty().then_some(set_id))
                .collect();
            for set_id in empty_sets {
                let hash = self.hash_builder.hash_one(set_id);
                let exists = self.index.find(hash, |entry| {
                    self.groups[entry.slot].grouping_set_id == set_id
                });
                if exists.is_none() {
                    let slot = Self::push_group(
                        &mut self.groups,
                        &mut self.aggregate_states,
                        hash,
                        set_id,
                        None,
                    )?;
                    self.index.insert_unique(GroupIndexEntry { hash, slot })?;
                    self.metrics.retained_key_slots_initialized = self
                        .metrics
                        .retained_key_slots_initialized
                        .saturating_add(self.plan.groups as u64);
                }
            }
        }
        Ok(())
    }
}
