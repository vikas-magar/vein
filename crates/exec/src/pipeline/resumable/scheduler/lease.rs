use super::super::executable::ExecutablePipeline;
use super::super::executor::{PipelineExecutor, ResumeToken};
use super::core::{Core, TaskState, Terminal};
use std::sync::Arc;

/// Removing local state from its slot is the exclusive execution lease.
/// Unwinding a worker turn terminates the scheduler rather than orphaning a task.
pub(super) struct Lease<G> {
    pub core: Arc<Core<G>>,
    pub id: usize,
    pub definition: Arc<ExecutablePipeline>,
    pub executor: Option<PipelineExecutor>,
    pub token: Option<ResumeToken>,
    pub registration: Option<G>,
    pub committed: bool,
}

impl<G> Core<G> {
    pub fn claim(self: &Arc<Self>) -> Option<Lease<G>> {
        let mut state = self.lock();
        if state.terminal.is_some() {
            return None;
        }
        let id = state.ready.pop_front()?;
        let task = &mut state.tasks[id];
        debug_assert!(task.state == TaskState::Ready);
        task.state = TaskState::Running;
        Some(Lease {
            core: Arc::clone(self),
            id,
            definition: Arc::clone(&task.definition),
            executor: task.executor.take(),
            token: task.token.take(),
            registration: task.registration.take(),
            committed: false,
        })
    }
}

impl<G> Lease<G> {
    pub fn requeue(&mut self) -> bool {
        let mut state = self.core.lock();
        self.committed = true;
        if state.terminal.is_some() {
            return false;
        }
        let task = &mut state.tasks[self.id];
        task.executor = self.executor.take();
        task.state = TaskState::Ready;
        state.ready.push_back(self.id);
        true
    }
}

impl<G> Drop for Lease<G> {
    fn drop(&mut self) {
        if !self.committed {
            self.core.stop(Terminal::Failed);
        }
    }
}
