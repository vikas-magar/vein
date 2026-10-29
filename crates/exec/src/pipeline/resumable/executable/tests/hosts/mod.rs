mod cooperative;
mod semantics;
mod output;
mod query_scope;
#[cfg(not(target_arch = "wasm32"))]
mod native;

use super::execution::{fixture::prepared, stage::Action};
use crate::pipeline::resumable::host::{CooperativeHost, HostDriver};
use crate::pipeline::resumable::scheduler::{SchedulerStep, WaitRegistrar};
use crate::pipeline::resumable::ErasedWaitReason;
use common::Result;
use std::future::Future;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Poll, Waker};

fn n(value: usize) -> NonZeroUsize { NonZeroUsize::new(value).unwrap() }

#[derive(Default)]
struct IoState {
    ready: bool,
    waker: Option<Waker>,
}
#[derive(Clone, Default)]
struct Io(Arc<Mutex<IoState>>, Arc<AtomicUsize>);
struct Registration(Io);
impl Io {
    fn complete(&self) {
        let wake = {
            let mut state = self.0.lock().unwrap();
            state.ready = true;
            state.waker.take()
        };
        if let Some(wake) = wake { wake.wake(); }
    }
}
impl WaitRegistrar for Io {
    type Registration = Registration;
    fn register(&self, reason: ErasedWaitReason, waker: Waker) -> Result<Registration> {
        reason.downcast::<u8>()?;
        let ready = {
            let mut state = self.0.lock().unwrap();
            if state.ready { true } else {
                state.waker = Some(waker.clone());
                false
            }
        };
        self.1.fetch_add(1, Ordering::SeqCst);
        if ready { waker.wake(); }
        Ok(Registration(self.clone()))
    }
}
impl Drop for Registration {
    fn drop(&mut self) { self.0.0.lock().unwrap().waker.take(); }
}

#[derive(Default)]
struct EventHost {
    yields: usize,
}
impl CooperativeHost for EventHost {
    async fn yield_control(&mut self) -> Result<()> {
        self.yields += 1;
        let mut pending = true;
        futures::future::poll_fn(|cx| {
            if std::mem::take(&mut pending) {
                cx.waker().wake_by_ref();
                Poll::Pending
            } else { Poll::Ready(()) }
        }).await;
        Ok(())
    }
}
