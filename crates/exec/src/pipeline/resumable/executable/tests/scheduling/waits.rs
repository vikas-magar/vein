use crate::pipeline::resumable::ErasedWaitReason;
use crate::pipeline::resumable::scheduler::WaitRegistrar;
use common::Result;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::Waker;

type RegisterHook = Box<dyn FnOnce(&Waker) -> Result<()> + Send>;
type DropHook = Box<dyn FnOnce() + Send>;
#[derive(Default)]
struct Inner {
    // Keep diagnostic copies to deliberately simulate late, already-unregistered callbacks.
    wakers: Mutex<Vec<Waker>>,
    dropped: AtomicUsize,
    register_hook: Mutex<Option<RegisterHook>>,
    drop_hook: Mutex<Option<DropHook>>,
}
#[derive(Clone, Default)]
pub(super) struct Waits(Arc<Inner>);
pub(super) struct Guard(Waits);

impl Waits {
    pub fn on_register(&self, hook: impl FnOnce(&Waker) -> Result<()> + Send + 'static) {
        *self.0.register_hook.lock().unwrap() = Some(Box::new(hook));
    }
    pub fn on_drop(&self, hook: impl FnOnce() + Send + 'static) {
        *self.0.drop_hook.lock().unwrap() = Some(Box::new(hook));
    }
    pub fn wake(&self, index: usize) {
        let waker = self.0.wakers.lock().unwrap()[index].clone();
        waker.wake();
    }
    pub fn registrations(&self) -> usize {
        self.0.wakers.lock().unwrap().len()
    }
    pub fn dropped(&self) -> usize {
        self.0.dropped.load(Ordering::Relaxed)
    }
}

impl WaitRegistrar for Waits {
    type Registration = Guard;
    fn register(&self, reason: ErasedWaitReason, waker: Waker) -> Result<Guard> {
        reason.downcast::<u8>()?;
        self.0.wakers.lock().unwrap().push(waker.clone());
        let hook = self.0.register_hook.lock().unwrap().take();
        if let Some(hook) = hook {
            hook(&waker)?;
        }
        Ok(Guard(self.clone()))
    }
}

impl Drop for Guard {
    fn drop(&mut self) {
        self.0.0.dropped.fetch_add(1, Ordering::Relaxed);
        let hook = self.0.0.drop_hook.lock().unwrap().take();
        if let Some(hook) = hook {
            hook();
        }
    }
}
