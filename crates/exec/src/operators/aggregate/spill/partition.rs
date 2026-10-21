use super::*;

impl AggregateSpillSchema {
    /// Repartitions existing runs by one hash bit using two bounded IPC writers.
    ///
    /// Each source file is opened and consumed before the next is opened. The
    /// input files remain intact until both output runs have finished, so a
    /// failed partition can be discarded without losing aggregate state.
    pub(in crate::operators::aggregate) fn partition_runs(
        &self,
        runs: &[SpillRun],
        tracker: &QueryMemoryTracker,
        bit: u32,
    ) -> Result<[Option<SpillRun>; 2]> {
        if bit >= u64::BITS {
            return Err(VeinError::internal(
                "aggregate spill partition bit is out of range",
            ));
        }
        tracker.check_cancelled()?;
        let manager = tracker.spill_manager();
        let mut zero_pending = manager.create_run()?;
        let mut one_pending = manager.create_run()?;
        let target_bytes = self.spill_workspace_target(tracker);
        let mut zero = AggregateSpillWriter::new(self, zero_pending.writer()?, target_bytes)?;
        let mut one = AggregateSpillWriter::new(self, one_pending.writer()?, target_bytes)?;
        let mut has_zero = false;
        let mut has_one = false;

        for run in runs {
            tracker.check_cancelled()?;
            let mut reader = self.open_reader(run.open()?)?;
            while let Some(mut batch) = reader.next_batch_charged(tracker)? {
                tracker.check_cancelled()?;
                for group in std::mem::take(&mut batch.groups) {
                    if group.hash & (1_u64 << bit) == 0 {
                        zero.push_retained(group, &batch._leases)?;
                        has_zero = true;
                    } else {
                        one.push_retained(group, &batch._leases)?;
                        has_one = true;
                    }
                }
                // Do not let pending key references outlive the lease owned by
                // this decoded batch. The writer retains the lease until flush.
                zero.flush()?;
                one.flush()?;
            }
        }

        let zero_output = zero.finish()?;
        let one_output = one.finish()?;
        let zero_run = zero_pending.commit(zero_output)?;
        let one_run = one_pending.commit(one_output)?;
        Ok([has_zero.then_some(zero_run), has_one.then_some(one_run)])
    }
}
