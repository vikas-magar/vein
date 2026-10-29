use super::HostDriver;
use crate::pipeline::resumable::scheduler::{SchedulerStep, WaitRegistrar};
use common::{Result, VeinError};
use std::num::NonZeroUsize;
use std::sync::Mutex;

impl<R: WaitRegistrar> HostDriver<R> {
    /// Runs persistent scoped workers, parking idle workers on notifications.
    ///
    /// This blocks its caller; asynchronous clients should dispatch this once to
    /// their blocking pool. No thread/task is created for individual turns.
    pub fn run_native(mut self, workers: NonZeroUsize) -> Result<SchedulerStep> {
        let failure = Mutex::new(None);
        std::thread::scope(|scope| {
            let mut handles = Vec::with_capacity(workers.get());
            for _ in 0..workers.get() {
                let failure = &failure;
                let driver = &self;
                match std::thread::Builder::new().name("vein-pipeline".into()).spawn_scoped(scope, move || {
                    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| driver.worker()));
                    let error = match outcome {
                        Ok(Ok(_)) => return,
                        Ok(Err(error)) => error,
                        Err(_) => VeinError::internal("native pipeline worker panicked"),
                    };
                    let mut first = failure.lock().unwrap_or_else(|poison| poison.into_inner());
                    if first.is_none() { *first = Some(error); }
                    drop(first);
                    driver.scheduler.cancel();
                }) {
                    Ok(handle) => handles.push(handle),
                    Err(error) => {
                        self.scheduler.cancel();
                        *failure.lock().unwrap_or_else(|poison| poison.into_inner()) =
                            Some(VeinError::internal(format!("could not start pipeline worker: {error}")));
                        break;
                    }
                }
            }
            for handle in handles {
                if handle.join().is_err() {
                    self.scheduler.cancel();
                    *failure.lock().unwrap_or_else(|poison| poison.into_inner()) =
                        Some(VeinError::internal("native pipeline worker cleanup panicked"));
                }
            }
        });
        if let Some(error) = failure.into_inner().unwrap_or_else(|poison| poison.into_inner()) {
            return Err(self.execution_error(error));
        }
        let terminal = self.scheduler.run_next(self.budget).map_err(|error| self.execution_error(error))?;
        Ok(self.terminal(terminal))
    }

    fn worker(&self) -> Result<SchedulerStep> {
        loop {
            let generation = self.signal.generation();
            match self.scheduler.run_next(self.budget).map_err(|error| self.execution_error(error))? {
                SchedulerStep::Progress => {}
                SchedulerStep::Idle => self.signal.wait(generation),
                terminal => return Ok(terminal),
            }
        }
    }
}
