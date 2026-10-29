use super::core::{Core, State};
use crate::memory::MemoryReservation;
use crate::pipeline::resumable::{ErasedWaitReason, scheduler::WaitRegistrar};
use common::{Result, VeinError};
use std::mem::size_of;
use std::sync::{Arc, Weak};
use std::task::Waker;

#[derive(Clone, Copy)]
pub(super) enum WaitKind { Capacity(usize), Data }

/// Resource readiness accepted by a T06 WaitRegistrar without client-specific code.
pub struct ResultWait {
    core: Weak<Core>,
    kind: WaitKind,
}

/// Unregisters its callback on drop; stale wakeups remain harmless to T06.
pub struct ResultWaitGuard {
    core: Weak<Core>,
    id: Option<u64>,
    _metadata: MemoryReservation,
}

/// Adapter for pipelines whose blocked resource is the shared output buffer.
/// Hosts with additional resources can dispatch typed waits to this same API.
pub struct ResultWaitRegistrar;

impl WaitRegistrar for ResultWaitRegistrar {
    type Registration = ResultWaitGuard;
    fn register(&self, reason: ErasedWaitReason, waker: Waker) -> Result<ResultWaitGuard> {
        reason.downcast::<ResultWait>()?.register(waker)
    }
}

impl ResultWait {
    pub(super) fn new(core: &Arc<Core>, kind: WaitKind) -> Self { Self { core: Arc::downgrade(core), kind } }

    /// Atomically subscribes and checks current readiness, including prior events.
    /// The callback is invoked only after releasing the buffer mutex.
    pub fn register(self, waker: Waker) -> Result<ResultWaitGuard> {
        let core = self.core.upgrade().ok_or_else(|| VeinError::invalid_input("result buffer no longer exists"))?;
        let metadata = core.tracker.reserve(size_of::<(u64, WaitKind, Waker)>(), "result readiness subscription")?;
        let mut immediate = Some(waker);
        let id = {
            let mut state = core.lock();
            if core.wait_ready(&state, self.kind) { None } else {
                let id = state.next_wait;
                state.next_wait = id.checked_add(1).ok_or_else(|| VeinError::internal("result wait identity overflow"))?;
                state.waiters.insert(id, (self.kind, immediate.take().unwrap()));
                Some(id)
            }
        };
        let guard = ResultWaitGuard { core: self.core, id, _metadata: metadata };
        if let Some(waker) = immediate { waker.wake(); }
        Ok(guard)
    }
}

impl Core {
    fn wait_ready(&self, state: &State, kind: WaitKind) -> bool {
        if state.terminal.is_some() { return true; }
        match kind {
            WaitKind::Capacity(id) => state.producers[id].finished || self.can_acquire(state, id),
            WaitKind::Data => self.data_ready(state) || self.complete(state),
        }
    }
    pub fn notify_ready(&self) {
        let wakers = {
            let mut state = self.lock();
            let ready = state.waiters.iter().filter_map(|(&id, (kind, _))|
                self.wait_ready(&state, *kind).then_some(id)).collect::<Vec<_>>();
            ready.into_iter().map(|id| state.waiters.remove(&id).unwrap().1).collect::<Vec<_>>()
        };
        for waker in wakers { waker.wake(); }
    }
}

impl Drop for ResultWaitGuard {
    fn drop(&mut self) {
        if let (Some(core), Some(id)) = (self.core.upgrade(), self.id) {
            let retired = core.lock().waiters.remove(&id);
            drop(retired);
        }
    }
}
