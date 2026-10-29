use super::*;
use super::super::spill::{AggregatePartitionCursor, AggregateSpillReader};
use crate::spill::SpillStream;
use std::collections::VecDeque;

/// Resumable read, merge, repartition and output state for aggregate spill runs.
pub(in crate::operators::aggregate) struct SpillCursor {
    plan: AggregatePlan,
    tracker: QueryMemoryTracker,
    registry: Arc<FunctionRegistry>,
    schema: AggregateSpillSchema,
    batch_rows: NonZeroUsize,
    runs: Vec<SpillRun>,
    run_index: usize,
    bit: u32,
    reader: Option<AggregateSpillReader<Box<dyn SpillStream>>>,
    sink: Option<AggregateSink>,
    partition: Option<AggregatePartitionCursor>,
    children: VecDeque<Vec<SpillRun>>,
    child: Option<Box<Self>>,
    output: Option<Box<AggregateOutputCursor>>,
}

impl SpillCursor {
    pub(super) fn new(sink: &AggregateSink, runs: Vec<SpillRun>, batch_rows: NonZeroUsize) -> Result<Self> {
        Self::with_template(sink.plan.clone(), sink.tracker.clone(), Arc::clone(&sink.registry),
                            sink.spill_schema.clone(), runs, batch_rows, 0)
    }

    fn with_template(plan: AggregatePlan, tracker: QueryMemoryTracker,
        registry: Arc<FunctionRegistry>, schema: AggregateSpillSchema,
        runs: Vec<SpillRun>, batch_rows: NonZeroUsize, bit: u32,
    ) -> Result<Self> {
        let mut partition_plan = plan.clone();
        partition_plan.estimated_groups = None;
        let sink = AggregateSink::with_registry_and_memory_tracker(
            partition_plan, tracker.clone(), Arc::clone(&registry))?;
        Ok(Self { plan, tracker, registry, schema, batch_rows, runs, run_index: 0,
            bit, reader: None, sink: Some(sink), partition: None,
            children: VecDeque::new(), child: None, output: None })
    }

    fn repartition(&mut self, error: VeinError) -> Result<()> {
        if self.bit >= u64::BITS { return Err(error); }
        self.reader = None;
        self.sink = None;
        self.partition = Some(AggregatePartitionCursor::new(
            &self.schema, self.runs.clone(), self.tracker.clone(), self.bit)?);
        Ok(())
    }

    pub(super) fn step(&mut self) -> Result<OperatorStep<()>> {
        self.tracker.check_cancelled()?;
        if let Some(child) = self.child.as_mut() {
            let step = child.step()?;
            if matches!(step, OperatorStep::Finished) { self.child = None; return Ok(OperatorStep::Runnable); }
            return Ok(step);
        }
        if let Some(output) = self.output.as_mut() {
            let step = output.step()?;
            if matches!(step, OperatorStep::Finished) { self.output = None; self.runs.clear(); return Ok(OperatorStep::Runnable); }
            return Ok(step);
        }
        if let Some(partition) = self.partition.as_mut() {
            if let Some(children) = partition.step()? {
                self.partition = None;
                self.runs.clear();
                for run in children.into_iter().flatten() { self.children.push_back(vec![run]); }
            }
            return Ok(OperatorStep::Runnable);
        }
        if let Some(runs) = self.children.pop_front() {
            self.child = Some(Box::new(Self::with_template(
                self.plan.clone(), self.tracker.clone(), Arc::clone(&self.registry),
                self.schema.clone(), runs, self.batch_rows, self.bit + 1)?));
            return Ok(OperatorStep::Runnable);
        }
        if self.sink.is_none() { return Ok(OperatorStep::Finished); }
        self.read_one_batch()
    }

    fn read_one_batch(&mut self) -> Result<OperatorStep<()>> {
        if self.reader.is_none() {
            if let Some(run) = self.runs.get(self.run_index) {
                self.reader = Some(self.schema.open_reader(run.open()?)?);
                return Ok(OperatorStep::Runnable);
            }
            let mut sink = self.sink.take().ok_or_else(|| VeinError::internal("aggregate spill sink is missing"))?;
            sink.ensure_empty_groups()?;
            self.output = Some(Box::new(AggregateOutputCursor::new(sink, self.batch_rows)));
            return Ok(OperatorStep::Runnable);
        }
        let reader = self.reader.as_mut().ok_or_else(|| VeinError::internal("aggregate spill reader is missing"))?;
        let Some(mut batch) = reader.next_batch_charged(&self.tracker)? else {
            self.reader = None;
            self.run_index += 1;
            return Ok(OperatorStep::Runnable);
        };
        let sink = self.sink.as_mut().ok_or_else(|| VeinError::internal("aggregate spill sink is missing"))?;
        for group in std::mem::take(&mut batch.groups) {
            if let Err(error) = sink.merge_spilled_group(group) {
                if matches!(error, VeinError::ResourceExhausted { .. }) {
                    self.repartition(error)?;
                    return Ok(OperatorStep::Runnable);
                }
                return Err(error);
            }
        }
        Ok(OperatorStep::Runnable)
    }
}
