use super::HostDriver;
use crate::pipeline::resumable::scheduler::{SchedulerStep, WaitRegistrar};
use common::{Result, VeinError};
use std::{num::NonZeroUsize, sync::Arc};

/// Cancels detached blocking work if its async owner is abandoned.
struct Execution<R: WaitRegistrar> {
    driver: Arc<HostDriver<R>>,
    completed: bool,
}
impl<R: WaitRegistrar> Drop for Execution<R> {
    fn drop(&mut self) {
        if !self.completed {
            self.driver.scheduler.cancel();
            if let Some(scope) = &self.driver.scope { scope.cancel(); }
        }
    }
}

impl<R: WaitRegistrar + 'static> HostDriver<R> {
    /// Runs persistent async workers, dispatching runnable CPU bursts to Tokio's
    /// blocking pool. Idle workers await readiness without retaining a blocking
    /// slot, allowing storage I/O to progress even with a one-thread blocking pool.
    pub async fn run_native(self, workers: NonZeroUsize) -> Result<SchedulerStep> {
        tokio::runtime::Handle::try_current()
            .map_err(|_| VeinError::invalid_input("native execution requires a Tokio runtime"))?;
        let mut execution = Execution { driver: Arc::new(self), completed: false };
        let mut tasks = tokio::task::JoinSet::new();
        for _ in 0..workers.get() {
            let driver = Arc::clone(&execution.driver);
            tasks.spawn(async move {
                let result = Arc::clone(&driver).worker().await;
                result.map_err(|error| {
                    let error = driver.execution_error(error);
                    driver.scheduler.cancel();
                    error
                })
            });
        }
        let mut failure = None;
        while let Some(outcome) = tasks.join_next().await {
            let result = outcome.unwrap_or_else(|error| Err(VeinError::internal(format!(
                "native pipeline worker could not complete: {error}"))));
            if let Err(error) = result {
                let error = execution.driver.execution_error(error);
                if failure.is_none() { failure = Some(error); }
                execution.driver.scheduler.cancel();
            }
        }
        if let Some(error) = failure { return Err(error); }
        let terminal = execution.driver.scheduler.run_next(execution.driver.budget)
            .map_err(|error| execution.driver.execution_error(error))?;
        // Every worker has relinquished its reference after join completion.
        let driver = Arc::get_mut(&mut execution.driver)
            .ok_or_else(|| VeinError::internal("native worker retained its driver after completion"))?;
        let terminal = driver.terminal(terminal);
        execution.completed = true;
        Ok(terminal)
    }

    async fn worker(self: Arc<Self>) -> Result<SchedulerStep> {
        loop {
            let driver = Arc::clone(&self);
            let (outcome, generation) = tokio::task::spawn_blocking(move || {
                let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| driver.run_ready()));
                let outcome = outcome.unwrap_or_else(|_| Err(VeinError::internal("native pipeline worker panicked")));
                outcome.map_err(|error| {
                    let error = driver.execution_error(error);
                    driver.scheduler.cancel();
                    error
                })
            }).await.map_err(|error| VeinError::internal(format!("native CPU burst failed: {error}")))??;
            match outcome {
                SchedulerStep::Progress => unreachable!("runnable bursts continue until blocked or terminal"),
                SchedulerStep::Idle => self.signal.changed(generation).await,
                terminal => return Ok(terminal),
            }
        }
    }

    fn run_ready(&self) -> Result<(SchedulerStep, u64)> {
        loop {
            let generation = self.signal.generation();
            match self.scheduler.run_next(self.budget)? {
                SchedulerStep::Progress => {}
                terminal => return Ok((terminal, generation)),
            }
        }
    }
}
