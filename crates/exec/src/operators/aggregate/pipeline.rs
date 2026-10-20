use std::marker::PhantomData;

use super::*;
use crate::pipeline::DataChunkView;

type SharedAggregateSink = Arc<Mutex<AggregateSink>>;
type AggregatePipelineNode<I> = Box<dyn PipelineNode<I>>;
type SharedAggregate<I> = (SharedAggregateSink, AggregatePipelineNode<I>);

trait AggregateInput: Send + Sync + 'static {
    fn accumulate(self, sink: &mut AggregateSink) -> Result<()>;
}

impl AggregateInput for DataChunk {
    fn accumulate(self, sink: &mut AggregateSink) -> Result<()> {
        sink.accumulate(self)
    }
}

impl AggregateInput for DataChunkView {
    fn accumulate(self, sink: &mut AggregateSink) -> Result<()> {
        sink.accumulate_view(self)
    }
}

/// Pushes aggregate inputs into the shared state used by the recursive runtime.
struct AggregateHandle<I>(SharedAggregateSink, PhantomData<fn(I)>);

/// Worker-owned aggregate state, combined into its matching canonical handle.
struct LocalAggregateHandle<I>(AggregateSink, PhantomData<fn(I)>);

impl<I: AggregateInput> PipelineNode<I> for LocalAggregateHandle<I> {
    fn push(&mut self, batch: I) -> Result<()> {
        batch.accumulate(&mut self.0)
    }

    fn flush(&mut self) -> Result<()> {
        Ok(())
    }

    fn combine_local(self: Box<Self>, canonical: &mut dyn PipelineNode<I>) -> Result<()> {
        let target = canonical
            .as_any_mut()
            .downcast_mut::<AggregateHandle<I>>()
            .ok_or_else(|| VeinError::internal("aggregate local state has a mismatched target"))?;
        let mut sink = target
            .0
            .lock()
            .map_err(|_| VeinError::internal("aggregate mutex poisoned"))?;
        sink.combine_local_state(self.0)
    }
}

impl<I: AggregateInput> PipelineNode<I> for AggregateHandle<I> {
    fn push(&mut self, batch: I) -> Result<()> {
        let mut sink = self
            .0
            .lock()
            .map_err(|_| VeinError::internal("aggregate mutex poisoned"))?;
        batch.accumulate(&mut sink)
    }

    fn flush(&mut self) -> Result<()> {
        Ok(())
    }

    fn fork_local(&self) -> Result<Option<Box<dyn PipelineNode<I>>>> {
        let sink = self
            .0
            .lock()
            .map_err(|_| VeinError::internal("aggregate mutex poisoned"))?;
        let mut local = AggregateSink::with_memory_tracker(sink.plan.clone(), sink.tracker.clone())?;
        local.inherit_hash_builders(&sink)?;
        Ok(Some(Box::new(LocalAggregateHandle(
            local,
            PhantomData,
        ))))
    }
}

/// Builds the accumulator and node for a materialized aggregate input.
pub fn shared(plan: AggregatePlan) -> Result<SharedAggregate<DataChunk>> {
    shared_with_memory_tracker(
        plan,
        QueryMemoryTracker::unlimited(common::cancellation::CancellationToken::new()),
    )
}

/// Builds a budgeted accumulator and node for a materialized aggregate input.
pub fn shared_with_memory_tracker(
    plan: AggregatePlan,
    tracker: QueryMemoryTracker,
) -> Result<SharedAggregate<DataChunk>> {
    shared_for_input::<DataChunk>(plan, tracker)
}

/// Builds a budgeted accumulator that keeps grouping keys in input views.
pub fn shared_view_with_memory_tracker(
    plan: AggregatePlan,
    tracker: QueryMemoryTracker,
) -> Result<SharedAggregate<DataChunkView>> {
    shared_for_input::<DataChunkView>(plan, tracker)
}

fn shared_for_input<I: AggregateInput>(
    plan: AggregatePlan,
    tracker: QueryMemoryTracker,
) -> Result<SharedAggregate<I>> {
    let sink = Arc::new(Mutex::new(AggregateSink::with_memory_tracker(plan, tracker)?));
    let handle: Box<dyn PipelineNode<I>> = Box::new(AggregateHandle(
        Arc::clone(&sink),
        PhantomData,
    ));
    Ok((sink, handle))
}
