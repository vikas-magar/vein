use super::super::executor::{PipelineExecutor, PipelineStep};
use super::{PipelineScheduler, SchedulerStep, WaitRegistrar};
use super::lease::Lease;
use common::Result;
use std::num::NonZeroUsize;

impl<R: WaitRegistrar> PipelineScheduler<R> {
    pub(super) fn run(&self, budget: NonZeroUsize) -> Result<SchedulerStep> {
        let Some(mut lease) = self.core.claim() else { return self.core.outcome() };
        drop(lease.registration.take());
        if self.core.lock().terminal.is_some() {
            lease.committed = true;
            return self.core.outcome();
        }
        let notify = match self.turn(&mut lease, budget) {
            Ok(notify) => notify,
            Err(error) => {
                // Record before Lease::drop retires state or wakes another worker.
                self.core.fail(error);
                return self.core.outcome();
            }
        };
        drop(lease);
        if notify { self.core.notify.wake_by_ref(); }
        Ok(match self.core.outcome()? {
            SchedulerStep::Idle => SchedulerStep::Progress,
            outcome => outcome,
        })
    }

    fn turn(&self, lease: &mut Lease<R::Registration>, budget: NonZeroUsize) -> Result<bool> {
        if lease.executor.is_none() {
            lease.executor = Some(PipelineExecutor::new(lease.definition.create_worker()?));
        }
        let executor = lease.executor.as_mut().unwrap();
        if let Some(token) = lease.token.take() { executor.resume(&token)?; }
        Ok(match executor.execute(budget)? {
            PipelineStep::Runnable => lease.requeue(),
            PipelineStep::Finished => lease.complete(),
            PipelineStep::Blocked { token, reason } => {
                lease.token = Some(token);
                let Some(waker) = lease.begin_wait()? else { return Ok(false) };
                let registration = self.registrar.register(reason, waker)?;
                lease.park(registration)
            }
        })
    }
}
