use super::super::executable::ExecutablePipeline;
use super::super::executor::{PipelineExecutor, ResumeToken};
use super::SchedulerStep;
use common::{Result, VeinError};
use std::collections::VecDeque;
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};
use std::task::Waker;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum TaskState { Dependencies, Ready, Running, Registering, Parked, Finished }
#[derive(Clone, Copy)]
pub(super) enum Terminal { Cancelled, Failed }

pub(super) struct Task<G> {
    pub definition: Arc<ExecutablePipeline>,
    pub state: TaskState,
    pub executor: Option<PipelineExecutor>,
    pub token: Option<ResumeToken>,
    pub registration: Option<G>,
    pub generation: u64,
    pub notified: bool,
}

pub(super) struct State<G> {
    pub tasks: Vec<Task<G>>,
    pub ready: VecDeque<usize>,
    pub pipeline_tasks: Vec<Vec<usize>>,
    pub remaining_workers: Vec<usize>,
    pub prerequisites: Vec<usize>,
    pub dependents: Vec<Vec<usize>>,
    pub completed: usize,
    pub terminal: Option<Terminal>,
    pub failure: Option<Arc<VeinError>>,
}

pub(super) struct Core<G> {
    pub state: Mutex<State<G>>,
    pub notify: Waker,
    pub failure_observer: OnceLock<Arc<dyn Fn(Arc<VeinError>) + Send + Sync>>,
}

impl<G> Core<G> {
    pub fn lock(&self) -> MutexGuard<'_, State<G>> {
        self.state.lock().unwrap_or_else(|poison| poison.into_inner())
    }

    pub fn outcome(&self) -> Result<SchedulerStep> {
        let state = self.lock();
        match state.terminal {
            Some(Terminal::Cancelled) => Ok(SchedulerStep::Cancelled),
            Some(Terminal::Failed) => Err(VeinError::from_shared(Arc::clone(
                state.failure.as_ref().expect("failed scheduler retains its cause")))),
            None if state.completed == state.pipeline_tasks.len() => Ok(SchedulerStep::Finished),
            None => Ok(SchedulerStep::Idle),
        }
    }

    pub fn stop(&self, terminal: Terminal) {
        let failure = if matches!(terminal, Terminal::Failed) {
            Some(Arc::new(VeinError::internal("pipeline worker unwound before completing its turn")))
        } else { None };
        self.stop_with_failure(terminal, failure);
    }

    pub fn fail(&self, error: VeinError) {
        self.stop_with_failure(Terminal::Failed, Some(error.into_shared()));
    }

    fn stop_with_failure(&self, terminal: Terminal, failure: Option<Arc<VeinError>>) {
        let retired = {
            let mut state = self.lock();
            if state.terminal.is_some() || state.completed == state.pipeline_tasks.len() { return; }
            state.terminal = Some(terminal);
            state.failure = failure.clone();
            state.ready.clear();
            std::mem::take(&mut state.tasks)
        };
        // Publish the cause before dropping operators: their cleanup may cancel
        // output resources and wake other workers or the client.
        if let (Some(observer), Some(error)) = (self.failure_observer.get(), failure) {
            observer(error);
        }
        tracing::debug!(target: "vein.audit", event = "resumable_scheduler_stopped",
            failed = matches!(terminal, Terminal::Failed), retired_tasks = retired.len(),
            "resumable scheduler stopped");
        // Guard destructors can call a resource or even wake the scheduler.
        drop(retired);
        self.notify.wake_by_ref();
    }
}
