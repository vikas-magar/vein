use super::*;

impl AggregateSink {
    pub(super) fn flush_current_run(&mut self) -> Result<()> {
        if self.groups.is_empty() {
            return Ok(());
        }
        self.tracker.check_cancelled()?;
        self.spill_runs.try_reserve(1).map_err(|error| {
            VeinError::internal(format!("cannot reserve aggregate spill run metadata: {error}"))
        })?;
        let mut pending = self.tracker.spill_manager().create_run()?;
        let output = self.spill_schema.write_run_with(
            pending.writer()?,
            self.groups.len(),
            |slot| self.intermediate_group_state(slot),
        )?;
        let run = pending.commit(output)?;
        let group_count = self.groups.len();
        self.spill_runs.push(run);
        self.release_group_state();
        tracing::info!(
            target: "vein.audit",
            event = "aggregate_spill_run_written",
            group_count,
            run_count = self.spill_runs.len(),
            "aggregate groups were externalized"
        );
        Ok(())
    }

    /// Persists a suffix of worker-local groups without materializing a second
    /// key representation. This is the bounded-memory combine fallback when
    /// the canonical sink cannot retain another group's state.
    fn write_group_suffix_to_run(&mut self, first_group: usize) -> Result<()> {
        let group_count = self.groups.len().saturating_sub(first_group);
        if group_count == 0 {
            return Ok(());
        }
        self.tracker.check_cancelled()?;
        self.spill_runs.try_reserve(1).map_err(|error| {
            VeinError::internal(format!("cannot reserve aggregate spill run metadata: {error}"))
        })?;
        let mut pending = self.tracker.spill_manager().create_run()?;
        let output = self.spill_schema.write_run_with(
            pending.writer()?,
            group_count,
            |offset| self.intermediate_group_state(first_group + offset),
        )?;
        let run = pending.commit(output)?;
        self.spill_runs.push(run);
        Ok(())
    }

    /// Combines an independent worker sink. It first uses the normal in-memory
    /// group index; if the query memory budget is reached, the unmerged suffix
    /// is externalized and consumed by the existing bounded spill merge.
    pub(super) fn combine_local_state(&mut self, mut local: AggregateSink) -> Result<()> {
        if self.plan.groups != local.plan.groups
            || self.plan.aggs != local.plan.aggs
            || self.plan.aggregate_input_types != local.plan.aggregate_input_types
            || self.plan.types != local.plan.types
            || self.grouping_sets != local.grouping_sets
            || self.tracker.identity() != local.tracker.identity()
            || !self.distinct_layout_matches(&local)
        {
            return Err(VeinError::internal(
                "cannot combine aggregate states from different plans or memory trackers",
            ));
        }

        let local_collectors = std::mem::take(&mut local.distinct_collectors);
        for (target, source) in self.distinct_collectors.iter_mut().zip(local_collectors) {
            target.sink.combine_local_state(*source.sink)?;
        }

        self.spill_runs
            .try_reserve(local.spill_runs.len())
            .map_err(|error| {
                VeinError::internal(format!("cannot reserve aggregate spill run metadata: {error}"))
            })?;
        self.spill_runs.append(&mut local.spill_runs);

        for slot in 0..local.groups.len() {
            let group = local.intermediate_group_state(slot)?;
            match self.merge_spilled_group(group) {
                Ok(()) => {}
                Err(VeinError::ResourceExhausted { .. }) => {
                    self.flush_current_run()?;
                    local.write_group_suffix_to_run(slot)?;
                    self.spill_runs
                        .try_reserve(local.spill_runs.len())
                        .map_err(|error| {
                            VeinError::internal(format!(
                                "cannot reserve aggregate spill run metadata: {error}"
                            ))
                        })?;
                    self.spill_runs.append(&mut local.spill_runs);
                    break;
                }
                Err(error) => return Err(error),
            }
        }
        self.metrics.accumulate(local.metrics);
        Ok(())
    }
}
