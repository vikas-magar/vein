use super::*;

impl AggregateSink {
    pub(super) fn finish_spill_partition_view(
        plan: &AggregatePlan,
        tracker: &QueryMemoryTracker,
        registry: &Arc<FunctionRegistry>,
        schema: &AggregateSpillSchema,
        runs: Vec<SpillRun>,
        bit: u32,
        batch_rows: NonZeroUsize,
        emit: &mut impl FnMut(DataChunkView) -> Result<()>,
    ) -> Result<()> {
        tracker.check_cancelled()?;
        let mut partition_plan = plan.clone();
        // Planner estimates describe the original full input and must not
        // preallocate a partition's hash table beyond its actual memory budget.
        partition_plan.estimated_groups = None;
        let mut sink = AggregateSink::with_registry_and_memory_tracker(
            partition_plan,
            tracker.clone(),
            Arc::clone(registry),
        )?;
        let merge_result = (|| {
            for run in &runs {
                tracker.check_cancelled()?;
                let mut reader = schema.open_reader(run.open()?)?;
                while let Some(mut batch) = reader.next_batch_charged(tracker)? {
                    tracker.check_cancelled()?;
                    for group in std::mem::take(&mut batch.groups) {
                        sink.merge_spilled_group(group)?;
                    }
                }
            }
            Ok(())
        })();

        match merge_result {
            Ok(()) => {
                let finish_result = sink.finish_in_memory_view_with(batch_rows, emit);
                drop(sink);
                finish_result?;
                drop(runs);
                Ok(())
            }
            Err(error @ VeinError::ResourceExhausted { .. }) => {
                drop(sink);
                if bit >= u64::BITS {
                    return Err(error);
                }
                let children = schema.partition_runs(&runs, tracker, bit)?;
                tracing::info!(
                    target: "vein.audit",
                    event = "aggregate_spill_partitioned",
                    hash_bit = bit,
                    child_count = children.iter().flatten().count(),
                    "aggregate spill partition exceeded memory and was repartitioned"
                );
                for child in children.into_iter().flatten() {
                    Self::finish_spill_partition_view(
                        plan,
                        tracker,
                        registry,
                        schema,
                        vec![child],
                        bit + 1,
                        batch_rows,
                        emit,
                    )?;
                }
                Ok(())
            }
            Err(error) => Err(error),
        }
    }

    /// One output column for a contiguous group range.
    pub(super) fn column_range(
        &self,
        column: usize,
        data_type: &VeinDataType,
        range: Range<usize>,
    ) -> Result<ArrayRef> {
        let groups = self.plan.groups as usize;
        if column < groups {
            let keys = self
                .groups
                .get(range.clone())
                .ok_or_else(|| VeinError::internal("aggregate output range is out of bounds"))?
                .iter()
                .map(|group| {
                    if !self.grouping_sets_by_column[column].contains(&group.grouping_set_id) {
                        return KeyValueRef::Null;
                    }
                    let Some(location) = group.key_location else {
                        return KeyValueRef::Null;
                    };
                    self.group_key_batches[location.batch][column]
                        .as_ref()
                        .expect("group source batch retains every grouping column")
                        .value(location.row)
                })
                .collect::<Vec<_>>();
            return key_column(&keys, data_type);
        }
        let offset = column - groups;
        self.aggregate_states[offset].finalize(range)
    }
}
