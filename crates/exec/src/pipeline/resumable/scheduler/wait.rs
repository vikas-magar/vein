use super::core::{Core, TaskState};
use super::lease::Lease;
use common::{Result, VeinError};
use std::sync::{Arc, Weak};
use std::task::{Wake, Waker};

struct TaskWake<G> {
    core: Weak<Core<G>>,
    id: usize,
    generation: u64,
}

impl<G: Send + 'static> Wake for TaskWake<G> {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        let Some(core) = self.core.upgrade() else {
            return;
        };
        let notify = {
            let mut state = core.lock();
            if state.terminal.is_some() {
                return;
            }
            let task = &mut state.tasks[self.id];
            if task.generation != self.generation {
                return;
            }
            match task.state {
                TaskState::Registering => {
                    task.notified = true;
                    false
                }
                TaskState::Parked => {
                    task.state = TaskState::Ready;
                    state.ready.push_back(self.id);
                    true
                }
                _ => false,
            }
        };
        if notify {
            tracing::debug!(target: "vein.audit", event = "resumable_task_woken",
                task_id = self.id, generation = self.generation, "resumable task woken");
            core.notify.wake_by_ref();
        }
    }
}

impl<G: Send + 'static> Lease<G> {
    pub fn begin_wait(&mut self) -> Result<Option<Waker>> {
        let mut state = self.core.lock();
        if state.terminal.is_some() {
            self.committed = true;
            return Ok(None);
        }
        let task = &mut state.tasks[self.id];
        task.generation = task
            .generation
            .checked_add(1)
            .ok_or_else(|| VeinError::internal("scheduler wait generation overflow"))?;
        task.state = TaskState::Registering;
        task.notified = false;
        Ok(Some(Waker::from(Arc::new(TaskWake {
            core: Arc::downgrade(&self.core),
            id: self.id,
            generation: task.generation,
        }))))
    }

    pub fn park(&mut self, registration: G) -> bool {
        let mut registration = Some(registration);
        let notify = {
            let mut state = self.core.lock();
            self.committed = true;
            if state.terminal.is_some() {
                false
            } else {
                let task = &mut state.tasks[self.id];
                task.executor = self.executor.take();
                task.token = self.token.take();
                task.registration = registration.take();
                let ready = task.notified;
                task.state = if ready {
                    TaskState::Ready
                } else {
                    TaskState::Parked
                };
                if ready {
                    state.ready.push_back(self.id);
                }
                ready
            }
        };
        // Cancellation during registration must not drop a guard under the lock.
        drop(registration);
        tracing::debug!(target: "vein.audit", event = "resumable_task_parked",
            task_id = self.id, pipeline_id = self.definition.id().0, immediately_ready = notify,
            "resumable task wait published");
        notify
    }
}
