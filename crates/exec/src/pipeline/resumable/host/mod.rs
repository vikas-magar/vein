//! Host policies over the shared scheduler; operators remain synchronous.
mod cooperative;
mod signal;
#[cfg(not(target_arch = "wasm32"))]
mod native;

use super::executable::ExecutablePipelinePlan;
use super::scheduler::{PipelineScheduler, WaitRegistrar};
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::task::Waker;
use common::cancellation::CancellationRegistration;
use crate::runtime::QueryExecutionResources;
mod query;

pub use cooperative::CooperativeHost;
use signal::Signal;

/// Owns one scheduler invocation and cancels it when dropped.
pub struct HostDriver<R: WaitRegistrar> {
    scheduler: Arc<PipelineScheduler<R>>,
    signal: Arc<Signal>,
    budget: NonZeroUsize,
    scope: Option<Arc<QueryExecutionResources>>,
    cancellation: Option<CancellationRegistration>,
    completed: bool,
}

/// Explicit control for an executing or idle host.
pub struct HostControl<R: WaitRegistrar>(Arc<PipelineScheduler<R>>);

impl<R: WaitRegistrar> Clone for HostControl<R> {
    fn clone(&self) -> Self { Self(Arc::clone(&self.0)) }
}

impl<R: WaitRegistrar> HostControl<R> {
    /// Stops execution and wakes parked host workers.
    pub fn cancel(&self) { self.0.cancel(); }
}

impl<R: WaitRegistrar> HostDriver<R> {
    /// Prepares worker slots and a bounded operator-transition budget per turn.
    ///
    /// Resource registrars must subscribe and check readiness without blocking.
    /// Host worker count is distinct from the number of pipeline worker slots.
    pub fn new(
        plan: &ExecutablePipelinePlan,
        pipeline_workers: NonZeroUsize,
        budget: NonZeroUsize,
        registrar: R,
    ) -> Self {
        let signal = Arc::new(Signal::default());
        let notify = Waker::from(Arc::clone(&signal));
        let scheduler = Arc::new(PipelineScheduler::new(plan, pipeline_workers, registrar, notify));
        Self { scheduler, signal, budget, scope: None, cancellation: None, completed: false }
    }

    /// Retains cancellation access without retaining the driver.
    pub fn control(&self) -> HostControl<R> { HostControl(Arc::clone(&self.scheduler)) }
}

impl<R: WaitRegistrar> Drop for HostDriver<R> {
    fn drop(&mut self) {
        self.scheduler.cancel();
        if !self.completed && let Some(scope) = &self.scope { scope.cancel(); }
    }
}
