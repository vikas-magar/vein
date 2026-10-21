use super::*;

impl AggregateSink {
    /// Folds a reference-backed input while retaining grouping columns as views.
    pub fn accumulate(&mut self, view: DataChunkView) -> Result<()> {
        if !self.distinct_collectors.is_empty() {
            self.accumulate_distinct(&view)?;
        }
        let batch = AggregateBatch::from_view(
            &view,
            self.plan.groups as usize,
            self.plan.aggs.len(),
            &self.grouping_sets,
        )?;
        self.accumulate_with_spill(&batch)
    }

    /// Retries only resource failures, splitting an oversized selection while
    /// keeping every retained Arrow array shared with the input batch.
    fn accumulate_with_spill(&mut self, batch: &AggregateBatch) -> Result<()> {
        match self.accumulate_batch(batch) {
            Ok(()) => Ok(()),
            Err(VeinError::ResourceExhausted { .. }) if !self.groups.is_empty() => {
                self.flush_current_run()?;
                self.accumulate_with_spill(batch)
            }
            Err(error @ VeinError::ResourceExhausted { .. }) => {
                let rows = batch.selection.size();
                if rows <= 1 {
                    return Err(error);
                }
                let midpoint = rows / 2;
                let left = batch.with_selection(batch.selection.slice(0, midpoint));
                let right = batch.with_selection(batch.selection.slice(midpoint, rows - midpoint));
                self.accumulate_with_spill(&left)?;
                self.accumulate_with_spill(&right)
            }
            Err(error) => Err(error),
        }
    }

    fn accumulate_batch(&mut self, batch: &AggregateBatch) -> Result<()> {
        let prepared = prepare::prepare_batch(self, batch)?;
        let reserved_bytes_before_batch = resolve::resolve_candidates(self, batch, prepared)?;
        retain::retain_new_groups(self, batch, prepared, reserved_bytes_before_batch)?;
        update::update_aggregate_states(self, batch, prepared)
    }
}
