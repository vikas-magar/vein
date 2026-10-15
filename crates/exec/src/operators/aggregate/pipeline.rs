use super::*;

/// Forwards pushes into the shared accumulator so the runtime can take the
/// groups once the source has been drained.
struct AggregateHandle(Arc<Mutex<AggregateSink>>);

/// A worker-owned aggregate state. Unlike [`AggregateHandle`], this node has no
/// shared lock in its batch path; its sink is consumed once at fan-in combine.
struct LocalAggregateHandle(AggregateSink);

impl PipelineNode<DataChunk> for LocalAggregateHandle {
    fn push(&mut self, chunk: DataChunk) -> Result<()> {
        self.0.accumulate(chunk)
    }

    fn flush(&mut self) -> Result<()> {
        Ok(())
    }

    fn combine_local(self: Box<Self>, canonical: &mut dyn PipelineNode<DataChunk>) -> Result<()> {
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

impl PipelineNode<DataChunk> for AggregateHandle {
    fn push(&mut self, chunk: DataChunk) -> Result<()> {
        self.0
            .lock()
            .map_err(|_| VeinError::internal("aggregate mutex poisoned"))?
            .accumulate(chunk)
    }

    /// Nothing to do: the runtime asks for the groups explicitly, because it
    /// has to push them into the sink itself.
    fn flush(&mut self) -> Result<()> {
        Ok(())
    }

    fn fork_local(&self) -> Result<Option<Box<dyn PipelineNode<DataChunk>>>> {
        let sink = self
            .0
            .lock()
            .map_err(|_| VeinError::internal("aggregate mutex poisoned"))?;
        let mut local = AggregateSink::with_memory_tracker(
            sink.plan.clone(),
            sink.tracker.clone(),
        )?;
        // Group hashes are stored in each local index and reused by the
        // canonical index during merge, so every worker must share its seed.
        local.hash_builder = sink.hash_builder.clone();
        Ok(Some(Box::new(LocalAggregateHandle(local))))
    }
}

/// Builds the accumulator and the node that feeds it.
pub fn shared(
    plan: AggregatePlan,
) -> Result<(Arc<Mutex<AggregateSink>>, Box<dyn PipelineNode<DataChunk>>)> {
    shared_with_memory_tracker(
        plan,
        QueryMemoryTracker::unlimited(common::cancellation::CancellationToken::new()),
    )
}

/// Builds a budgeted accumulator and the node that feeds it.
pub fn shared_with_memory_tracker(
    plan: AggregatePlan,
    tracker: QueryMemoryTracker,
) -> Result<(Arc<Mutex<AggregateSink>>, Box<dyn PipelineNode<DataChunk>>)> {
    let sink = Arc::new(Mutex::new(AggregateSink::with_memory_tracker(
        plan, tracker,
    )?));
    let handle: Box<dyn PipelineNode<DataChunk>> = Box::new(AggregateHandle(Arc::clone(&sink)));
    Ok((sink, handle))
}
