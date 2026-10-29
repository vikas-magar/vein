//! Bridges standard stream wakeups to scheduler resource subscriptions.
use std::sync::{Arc, Mutex};
use std::task::{Wake, Waker};

#[derive(Default)]
struct State {
    ready: bool,
    subscriber: Option<(Arc<()>, Waker)>,
}

#[derive(Default)]
struct Signal(Mutex<State>);

impl Wake for Signal {
    fn wake(self: Arc<Self>) { self.wake_by_ref(); }
    fn wake_by_ref(self: &Arc<Self>) {
        let notify = {
            let mut state = self.0.lock().unwrap_or_else(|p| p.into_inner());
            state.ready = true;
            state.subscriber.as_ref().map(|(_, w)| w.clone())
        };
        if let Some(waker) = notify { waker.wake(); }
    }
}

/// Readiness for one cursor; no thread, polling loop or batch channel is created.
#[derive(Clone, Default)]
pub struct ScanReady(Arc<Signal>);

impl ScanReady {
    pub(super) fn before_poll(&self) -> Waker {
        self.0.0.lock().unwrap_or_else(|p| p.into_inner()).ready = false;
        Waker::from(Arc::clone(&self.0))
    }

    /// Subscribes without losing a wake that occurred before registration.
    pub fn register(&self, waker: Waker) -> ScanRegistration {
        let token = Arc::new(());
        let (old, ready) = {
            let mut state = self.0.0.lock().unwrap_or_else(|p| p.into_inner());
            let old = state.subscriber.replace((Arc::clone(&token), waker.clone()));
            (old, state.ready)
        };
        drop(old);
        if ready { waker.wake(); }
        ScanRegistration { ready: self.clone(), token }
    }
}

/// Removes only its own subscription; late guard drops cannot remove a new one.
pub struct ScanRegistration {
    ready: ScanReady,
    token: Arc<()>,
}
impl Drop for ScanRegistration {
    fn drop(&mut self) {
        let retired = {
            let mut state = self.ready.0.0.lock().unwrap_or_else(|p| p.into_inner());
            if state.subscriber.as_ref().is_some_and(|(id, _)| Arc::ptr_eq(id, &self.token)) {
                state.subscriber.take()
            } else { None }
        };
        drop(retired);
    }
}
