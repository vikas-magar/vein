use super::HostDriver;
use crate::pipeline::resumable::scheduler::{SchedulerStep, WaitRegistrar};
use crate::runtime::QueryExecutionResources;
use common::{Result, VeinError};
use std::sync::Arc;

impl<R: WaitRegistrar + 'static> HostDriver<R> {
    pub(crate) fn bind_query(mut self, scope: Arc<QueryExecutionResources>) -> Result<Self> {
        if self.scope.is_some() {
            return Err(VeinError::invalid_input("host already belongs to a query scope"));
        }
        let resources = Arc::downgrade(&scope);
        self.scheduler.observe_failure(Arc::new(move |error| {
            if let Some(scope) = resources.upgrade() {
                scope.record_failure(VeinError::from_shared(error));
            }
        }))?;
        let scheduler = Arc::downgrade(&self.scheduler);
        self.cancellation = Some(scope.cancellation().on_cancel(move || {
            if let Some(scheduler) = scheduler.upgrade() { scheduler.cancel(); }
        })?);
        self.scope = Some(scope);
        Ok(self)
    }
}

impl<R: WaitRegistrar> HostDriver<R> {
    pub(super) fn execution_error(&self, error: VeinError) -> VeinError {
        match &self.scope {
            Some(scope) => VeinError::from_shared(scope.fail_query(error)),
            None => error,
        }
    }
    pub(super) fn terminal(&mut self, outcome: SchedulerStep) -> SchedulerStep {
        // Computation finishing does not close unread client output.
        self.completed = outcome == SchedulerStep::Finished;
        outcome
    }
}
