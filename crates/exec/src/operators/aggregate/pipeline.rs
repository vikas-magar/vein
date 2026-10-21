use super::*;
use crate::pipeline::DataChunkView;

type SharedAggregateSink = Arc<Mutex<AggregateSink>>;
type AggregatePipelineNode = Box<dyn PipelineNode>;
type SharedAggregate = (SharedAggregateSink, AggregatePipelineNode);

/// Pushes aggregate inputs into the shared state used by the recursive runtime.
struct AggregateHandle(SharedAggregateSink);

/// Worker-owned aggregate state, combined into its matching canonical handle.
struct LocalAggregateHandle(AggregateSink);

impl PipelineNode for LocalAggregateHandle {
    fn push(&mut self, batch: DataChunkView) -> Result<()> {
        self.0.accumulate(batch)
    }

    fn flush(&mut self) -> Result<()> {
        Ok(())
    }

    fn combine_local(self: Box<Self>, canonical: &mut dyn PipelineNode) -> Result<()> {
        let target = canonical
            .as_any_mut()
            .downcast_mut::<AggregateHandle>()
            .ok_or_else(|| VeinError::internal("aggregate local state has a mismatched target"))?;
        let mut sink = target
            .0
            .lock()
            .map_err(|_| VeinError::internal("aggregate mutex poisoned"))?;
        sink.combine_local_state(self.0)
    }
}

impl PipelineNode for AggregateHandle {
    fn push(&mut self, batch: DataChunkView) -> Result<()> {
        let mut sink = self
            .0
            .lock()
            .map_err(|_| VeinError::internal("aggregate mutex poisoned"))?;
        sink.accumulate(batch)
    }

    fn flush(&mut self) -> Result<()> {
        Ok(())
    }

    fn fork_local(&self) -> Result<Option<Box<dyn PipelineNode>>> {
        let sink = self
            .0
            .lock()
            .map_err(|_| VeinError::internal("aggregate mutex poisoned"))?;
        let mut local = AggregateSink::with_registry_and_memory_tracker(
            sink.plan.clone(),
            sink.tracker.clone(),
            Arc::clone(&sink.registry),
        )?;
        local.inherit_hash_builders(&sink)?;
        Ok(Some(Box::new(LocalAggregateHandle(local))))
    }
}

/// Builds the accumulator and node for a view-backed aggregate input.
pub fn shared(plan: AggregatePlan) -> Result<SharedAggregate> {
    shared_with_memory_tracker(
        plan,
        QueryMemoryTracker::unlimited(common::cancellation::CancellationToken::new()),
    )
}

/// Builds a budgeted accumulator and node for a view-backed aggregate input.
pub fn shared_with_memory_tracker(
    plan: AggregatePlan,
    tracker: QueryMemoryTracker,
) -> Result<SharedAggregate> {
    shared_with_registry_and_memory_tracker(
        plan,
        tracker,
        Arc::new(FunctionRegistry::with_builtins()),
    )
}

/// Builds a budgeted aggregate using the same registry as expression execution.
pub fn shared_with_registry_and_memory_tracker(
    plan: AggregatePlan,
    tracker: QueryMemoryTracker,
    registry: Arc<FunctionRegistry>,
) -> Result<SharedAggregate> {
    let sink = Arc::new(Mutex::new(AggregateSink::with_registry_and_memory_tracker(
        plan, tracker, registry,
    )?));
    let handle: AggregatePipelineNode = Box::new(AggregateHandle(Arc::clone(&sink)));
    Ok((sink, handle))
}
