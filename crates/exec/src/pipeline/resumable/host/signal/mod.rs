use futures::task::AtomicWaker;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake};
#[cfg(not(target_arch = "wasm32"))]
use std::sync::Condvar;

/// One cooperative waiter or multiple native waiters, never both concurrently.
#[derive(Default)]
pub(super) struct Signal {
    generation: Mutex<u64>,
    asynchronous: AtomicWaker,
    #[cfg(not(target_arch = "wasm32"))]
    native: Condvar,
}

impl Signal {
    pub fn generation(&self) -> u64 {
        *self.generation.lock().unwrap_or_else(|poison| poison.into_inner())
    }

    pub fn changed(&self, generation: u64) -> Changed<'_> {
        Changed { signal: self, generation }
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn wait(&self, generation: u64) {
        let mut current = self.generation.lock().unwrap_or_else(|poison| poison.into_inner());
        while *current == generation {
            current = self.native.wait(current).unwrap_or_else(|poison| poison.into_inner());
        }
    }

    fn notify(&self) {
        {
            let mut generation = self.generation.lock().unwrap_or_else(|poison| poison.into_inner());
            *generation = generation.wrapping_add(1);
            #[cfg(not(target_arch = "wasm32"))]
            self.native.notify_all();
        }
        // Waking an executor may call external code; never do it under our lock.
        self.asynchronous.wake();
    }
}

impl Wake for Signal {
    fn wake(self: Arc<Self>) { self.notify(); }
    fn wake_by_ref(self: &Arc<Self>) { self.notify(); }
}

pub(super) struct Changed<'a> {
    signal: &'a Signal,
    generation: u64,
}

impl Future for Changed<'_> {
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        // Register before observing the generation to close the lost-wake window.
        self.signal.asynchronous.register(cx.waker());
        if self.signal.generation() != self.generation { Poll::Ready(()) } else { Poll::Pending }
    }
}

impl Drop for Changed<'_> {
    fn drop(&mut self) { self.signal.asynchronous.take(); }
}

#[cfg(test)]
mod tests;
