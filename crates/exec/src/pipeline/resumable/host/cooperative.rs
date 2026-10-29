use super::HostDriver;
use crate::pipeline::resumable::scheduler::{SchedulerStep, WaitRegistrar};
use common::Result;
use std::future::Future;

/// Supplies an actual host scheduling opportunity, rather than a ready future.
///
/// Browser implementations must allow task events (I/O, ACKs, cancellation), not
/// just microtasks. Mutable access prevents overlapping yields on one host.
pub trait CooperativeHost {
    /// Returns after the host has had a chance to process other events.
    fn yield_control(&mut self) -> impl Future<Output=Result<()>>;
}

impl<R: WaitRegistrar> HostDriver<R> {
    /// Drives the same scheduler as native execution on one cooperative thread.
    ///
    /// Idle work awaits readiness without polling. Every productive bounded turn
    /// yields through the host. Dropping this future cancels the owned driver.
    pub async fn run_cooperative(mut self, host: &mut impl CooperativeHost) -> Result<SchedulerStep> {
        loop {
            let generation = self.signal.generation();
            match self.scheduler.run_next(self.budget).map_err(|error| self.execution_error(error))? {
                SchedulerStep::Progress => host.yield_control().await.map_err(|error| self.execution_error(error))?,
                SchedulerStep::Idle => self.signal.changed(generation).await,
                terminal => return Ok(self.terminal(terminal)),
            }
        }
    }
}
