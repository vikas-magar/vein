use super::QueryExecutionResources;
use crate::pipeline::resumable::executable::ExecutablePipelinePlan;
use crate::pipeline::resumable::host::HostDriver;
use crate::pipeline::resumable::result::{
    ResultBufferConfig, ResultConsumer, ResultProducer, result_buffer,
};
use crate::pipeline::resumable::scheduler::WaitRegistrar;
use common::Result;
use std::num::NonZeroUsize;
use std::sync::Arc;

impl QueryExecutionResources {
    /// Creates output bound to this query's memory, spill and cancellation root.
    ///
    /// Close/failure cancels remaining work; previously fetched batches retain
    /// their own leases and remain readable.
    pub fn result_buffer(
        self: &Arc<Self>,
        config: ResultBufferConfig,
        producers: NonZeroUsize,
    ) -> Result<(ResultConsumer, Vec<ResultProducer>)> {
        let (mut consumer, producers) = result_buffer(config, producers, self.memory.clone())?;
        consumer.bind_query(Arc::clone(self))?;
        Ok((consumer, producers))
    }

    /// Creates a host driver with cancellation wired before any worker can run.
    pub fn host_driver<R: WaitRegistrar + 'static>(
        self: &Arc<Self>,
        plan: &ExecutablePipelinePlan,
        pipeline_workers: NonZeroUsize,
        budget: NonZeroUsize,
        registrar: R,
    ) -> Result<HostDriver<R>> {
        HostDriver::new(plan, pipeline_workers, budget, registrar).bind_query(Arc::clone(self))
    }
}
