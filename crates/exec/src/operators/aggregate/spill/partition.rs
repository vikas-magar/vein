use super::*;
use crate::spill::{PendingSpillRun, SpillStream, SpillWriteHandle};

/// One repartition pass, retaining parent runs until both children commit.
pub(in crate::operators::aggregate) struct AggregatePartitionCursor {
    layout: AggregateSpillSchema,
    tracker: QueryMemoryTracker,
    runs: Vec<SpillRun>,
    run: usize,
    reader: Option<AggregateSpillReader<Box<dyn SpillStream>>>,
    writers: [Option<AggregateSpillWriter<SpillWriteHandle>>; 2],
    pending: [Option<PendingSpillRun>; 2],
    has_rows: [bool; 2],
    bit: u32,
    finished: bool,
}
impl AggregatePartitionCursor {
    pub(in crate::operators::aggregate) fn new(layout: &AggregateSpillSchema,
        runs: Vec<SpillRun>, tracker: QueryMemoryTracker, bit: u32,
    ) -> Result<Self> {
        if bit >= u64::BITS { return Err(VeinError::internal("aggregate spill partition bit is out of range")); }
        tracker.check_cancelled()?;
        let manager = tracker.spill_manager();
        let mut zero = manager.create_run()?;
        let mut one = manager.create_run()?;
        let target = layout.spill_workspace_target(&tracker);
        let writers = [Some(AggregateSpillWriter::new(layout, zero.writer()?, target)?),
            Some(AggregateSpillWriter::new(layout, one.writer()?, target)?)];
        Ok(Self { layout: layout.clone(), tracker, runs, run: 0, reader: None, writers,
            pending: [Some(zero), Some(one)], has_rows: [false; 2], bit, finished: false })
    }

    /// Reads/writes at most one bounded IPC batch per step.
    pub(in crate::operators::aggregate) fn step(&mut self) -> Result<Option<[Option<SpillRun>; 2]>> {
        self.tracker.check_cancelled()?;
        if self.finished { return Err(VeinError::internal("aggregate partition cursor already finished")); }
        if self.reader.is_none() {
            if let Some(run) = self.runs.get(self.run) {
                self.reader = Some(self.layout.open_reader(run.open()?)?);
                return Ok(None);
            }
            let mut output = [None, None];
            for (index, slot) in output.iter_mut().enumerate() {
                let writer = self.writers[index].take().ok_or_else(|| VeinError::internal("partition writer missing"))?;
                let pending = self.pending[index].take().ok_or_else(|| VeinError::internal("partition run missing"))?;
                let run = pending.commit(writer.finish()?)?;
                if self.has_rows[index] { *slot = Some(run); }
            }
            self.finished = true;
            self.runs.clear();
            return Ok(Some(output));
        }
        let reader = self.reader.as_mut().ok_or_else(|| VeinError::internal("partition reader missing"))?;
        let Some(mut batch) = reader.next_batch_charged(&self.tracker)? else {
            self.reader = None;
            self.run += 1;
            return Ok(None);
        };
        for group in std::mem::take(&mut batch.groups) {
            let side = usize::from(group.hash & (1_u64 << self.bit) != 0);
            self.writers[side].as_mut().ok_or_else(|| VeinError::internal("partition writer missing"))?
                .push_retained(group, &batch._leases)?;
            self.has_rows[side] = true;
        }
        for writer in self.writers.iter_mut().flatten() { writer.flush()?; }
        Ok(None)
    }
}

impl AggregateSpillSchema {
    pub(in crate::operators::aggregate) fn partition_runs(&self, runs: &[SpillRun],
        tracker: &QueryMemoryTracker, bit: u32,
    ) -> Result<[Option<SpillRun>; 2]> {
        let mut cursor = AggregatePartitionCursor::new(self, runs.to_vec(), tracker.clone(), bit)?;
        loop { if let Some(output) = cursor.step()? { return Ok(output); } }
    }
}
