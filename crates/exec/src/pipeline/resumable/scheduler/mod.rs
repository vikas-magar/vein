//! Scheduling over canonical pipelines without owning threads or a host runtime.
//!
//! A host calls `run_next` on its workers and uses the injected waker to resume
//! an idle event loop. Operator execution, resource registration, notifications
//! and registration-guard destruction always occur outside scheduler locks.

mod construction;
mod core;
mod lease;
mod execution;
mod completion;
mod wait;

use super::ErasedWaitReason;
use super::executable::ExecutablePipelinePlan;
use common::Result;
use core::{Core, Terminal};
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::task::Waker;

/// Connects typed operator resources to scheduler readiness notifications.
pub trait WaitRegistrar: Send + Sync {
    /// Owned subscription; dropping it must unregister the resource callback.
    type Registration: Send + 'static;

    /// Arm a nonblocking subscription and observe already-existing readiness.
    ///
    /// Readiness occurring before or during subscription must wake `waker`.
    /// Callbacks may run synchronously and may reenter the scheduler. Spurious
    /// or repeated wakeups are allowed; the resource must be checked on resume.
    fn register(&self, reason: ErasedWaitReason, waker: Waker) -> Result<Self::Registration>;
}

/// Outcome of one scheduler turn; no client or thread-pool types are required.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchedulerStep {
    /// A worker performed a bounded turn; the host may request another.
    Progress,
    /// No task is ready; resources or other workers must notify the host.
    Idle,
    /// All pipelines and their worker-state combinations completed.
    Finished,
    /// Shutdown was requested; in-flight turns release their state on return.
    Cancelled,
}

/// Shared scheduling state with exclusive ownership of each running executor.
pub struct PipelineScheduler<R: WaitRegistrar> {
    core: Arc<Core<R::Registration>>,
    registrar: R,
}

impl<R: WaitRegistrar> PipelineScheduler<R> {
    /// Builds worker slots, respecting serial capabilities and graph dependencies.
    /// Local operators are created lazily, after prerequisite pipelines finish.
    pub fn new(plan: &ExecutablePipelinePlan, workers: NonZeroUsize, registrar: R, notify: Waker) -> Self {
        Self { core: Arc::new(Core::new(plan, workers, notify)), registrar }
    }

    /// Runs at most one task turn. Concurrent callers cannot claim the same local state.
    pub fn run_next(&self, budget: NonZeroUsize) -> Result<SchedulerStep> { self.run(budget) }

    /// Cooperatively stops all work and unregisters parked resource subscriptions.
    /// An in-flight bounded execution turn may finish before its state is released.
    pub fn cancel(&self) { self.core.stop(Terminal::Cancelled); }

    pub(crate) fn observe_failure(&self, observer: std::sync::Arc<
        dyn Fn(std::sync::Arc<common::VeinError>) + Send + Sync>,
    ) -> common::Result<()> {
        self.core.failure_observer.set(observer)
            .map_err(|_| common::VeinError::invalid_input("scheduler failure observer already bound"))
    }
}

impl<R: WaitRegistrar> Drop for PipelineScheduler<R> {
    fn drop(&mut self) { self.core.stop(Terminal::Cancelled); }
}
