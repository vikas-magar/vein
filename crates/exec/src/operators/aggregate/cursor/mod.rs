//! Owned, bounded finalization over the existing aggregate kernels.
mod distinct;
mod spill;

use super::*;
use std::collections::VecDeque;
use crate::pipeline::resumable::OperatorStep;
use distinct::DistinctActive;
use spill::SpillCursor;

pub(super) enum Phase {
    Distinct,
    Prepare,
    Memory(usize),
    Spill(SpillCursor),
    Finished,
}

pub(in crate::operators::aggregate) struct AggregateOutputCursor {
    pub(super) sink: AggregateSink,
    batch_rows: NonZeroUsize,
    phase: Phase,
    distinct: VecDeque<super::distinct::DistinctCollector>,
    active: Option<DistinctActive>,
}
impl AggregateOutputCursor {
    pub(super) fn new(mut sink: AggregateSink, batch_rows: NonZeroUsize) -> Self {
        let distinct = std::mem::take(&mut sink.distinct_collectors).into();
        Self { sink, batch_rows, phase: Phase::Distinct, distinct, active: None }
    }
    pub(in crate::operators::aggregate) fn step(&mut self) -> Result<OperatorStep<()>> {
        self.sink.tracker.check_cancelled()?;
        let phase = std::mem::replace(&mut self.phase, Phase::Finished);
        match phase {
            Phase::Distinct => self.step_distinct(),
            Phase::Prepare => {
                if self.sink.spill_runs.is_empty() {
                    self.sink.ensure_empty_groups()?;
                    self.phase = Phase::Memory(0);
                } else {
                    self.sink.flush_current_run()?;
                    let runs = std::mem::take(&mut self.sink.spill_runs);
                    self.phase = Phase::Spill(SpillCursor::new(&self.sink, runs, self.batch_rows)?);
                }
                Ok(OperatorStep::Runnable)
            }
            Phase::Memory(start) => {
                if start == self.sink.groups.len() { return Ok(OperatorStep::Finished); }
                let end = start.saturating_add(self.batch_rows.get()).min(self.sink.groups.len());
                let output = self.sink.output_range_view(start..end)?;
                self.phase = Phase::Memory(end);
                Ok(OperatorStep::Output(output))
            }
            Phase::Spill(mut cursor) => {
                let step = cursor.step()?;
                if !matches!(step, OperatorStep::Finished) { self.phase = Phase::Spill(cursor); }
                Ok(step)
            }
            Phase::Finished => Ok(OperatorStep::Finished),
        }
    }
}

impl AggregateSink {
    pub(in crate::operators::aggregate) fn take_output_cursor(&mut self, batch_rows: NonZeroUsize) -> Result<AggregateOutputCursor> {
        let mut plan = self.plan.clone();
        plan.estimated_groups = None;
        let replacement = Self::with_registry_and_memory_tracker(plan, self.tracker.clone(), Arc::clone(&self.registry))?;
        Ok(AggregateOutputCursor::new(std::mem::replace(self, replacement), batch_rows))
    }
}
