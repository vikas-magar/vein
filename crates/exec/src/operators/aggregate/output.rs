use super::*;

impl AggregateSink {
    pub(super) fn release_group_state(&mut self) {
        self.index = GroupIndex::default();
        self.groups = Vec::new();
        self.group_key_batches = Vec::new();
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
        if self.spill_runs.is_empty() {
            return self.finish_in_memory_with(batch_rows, &mut emit);
        }
        self.flush_current_run()?;
        let runs = std::mem::take(&mut self.spill_runs);
        Self::finish_spill_partition(
            &self.plan,
            &self.tracker,
            &self.spill_schema,
            runs,
            0,
            batch_rows,
            &mut emit,
        )
    }

    pub(super) fn finish_in_memory_with(
        &mut self,
        batch_rows: NonZeroUsize,
        emit: &mut impl FnMut(DataChunk) -> Result<()>,
    ) -> Result<()> {
        if self.groups.is_empty() && self.plan.emit_empty_group_on_empty_input {
            let empty_sets: Vec<usize> = self
                .grouping_sets
                .iter()
                .enumerate()
                .filter_map(|(set_id, grouping_set)| grouping_set.is_empty().then_some(set_id))
                .collect();
            for set_id in empty_sets {
                let mut hasher = self.hash_builder.build_hasher();
                set_id.hash(&mut hasher);
                let hash = hasher.finish();
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
        if self.groups.is_empty() {
            return Ok(());
        }
        let mut start = 0;
        while start < self.groups.len() {
            let end = start.saturating_add(batch_rows.get()).min(self.groups.len());
            let output_materialize_started = Instant::now();
            let range = start..end;
            let mut arrays: Vec<ArrayRef> = Vec::with_capacity(self.plan.types.len());
            for (column, data_type) in self.plan.types.iter().enumerate() {
                arrays.push(self.column_range(column, data_type, range.clone())?);
            }
            self.metrics.output_materialize_ns = self
                .metrics
                .output_materialize_ns
                .saturating_add(output_materialize_started.elapsed().as_nanos() as u64);
            emit(DataChunk::new(
                arrays,
                SelectionVector::new_dense(end - start),
            ))?;
            start = end;
        }
        Ok(())
    }
}
