use super::*;
use super::init::create_aggregate_states;

impl AggregateSink {
    pub(super) fn release_group_state(&mut self) {
        self.index = GroupIndex::default();
        self.groups = Vec::new();
        self.group_key_batches = Vec::new();
        self.group_key_batch_arrays = Vec::new();
        self.group_key_batch_leases = Vec::new();
        self.aggregate_states = create_aggregate_states(&self.plan, &self.registry)
            .expect("validated aggregate plan can recreate empty function states");
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

    /// Collects finished output views for callers that need an owned result list.
    pub async fn finish(&mut self) -> Result<Vec<DataChunkView>> {
        let mut output = Vec::new();
        self.finish_with(AGGREGATE_BATCH_ROWS, |batch| {
            output.push(batch);
            Ok(())
        }).await?;
        Ok(output)
    }

    /// Emits bounded output views while retaining only the aggregate state.
    /// Delivery may suspend between batches and stops on the first error.
    pub async fn finish_with(
        &mut self,
        batch_rows: NonZeroUsize,
        mut emit: impl crate::pipeline::delivery::BatchConsumer,
    ) -> Result<()> {
        if !self.distinct_collectors.is_empty() {
            Box::pin(self.finish_distinct_counts(batch_rows)).await?;
        }
        if self.spill_runs.is_empty() {
            return self.finish_in_memory_view_with(batch_rows, &mut emit).await;
        }
        self.flush_current_run()?;
        let runs = std::mem::take(&mut self.spill_runs);
        let context = super::spill_finish::SpillFinishContext {
            plan: &self.plan,
            tracker: &self.tracker,
            registry: &self.registry,
            schema: &self.spill_schema,
            batch_rows,
        };
        Self::finish_spill_partition_view(&context, runs, 0, &mut emit).await
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
