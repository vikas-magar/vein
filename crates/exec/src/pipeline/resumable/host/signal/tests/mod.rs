use super::*;
use futures::task::noop_waker;
use std::task::Waker;
use std::{future::Future, task::Context};

#[test]
fn readiness_before_subscription_is_not_lost() {
    let signal = Arc::new(Signal::default());
    let generation = signal.generation();
    Waker::from(Arc::clone(&signal)).wake();
    let mut changed = Box::pin(signal.changed(generation));
    assert!(changed.as_mut().poll(&mut Context::from_waker(&noop_waker())).is_ready());
}

#[test]
fn pending_wait_is_not_a_self_waking_busy_loop() {
    let signal = Arc::new(Signal::default());
    let mut changed = Box::pin(signal.changed(signal.generation()));
    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);
    assert!(changed.as_mut().poll(&mut cx).is_pending());
    assert!(changed.as_mut().poll(&mut cx).is_pending());
    Waker::from(Arc::clone(&signal)).wake();
    assert!(changed.as_mut().poll(&mut cx).is_ready());
}

#[test]
fn dropping_async_wait_removes_its_task_waker() {
    let signal = Signal::default();
    let mut changed = Box::pin(signal.changed(signal.generation()));
    assert!(changed.as_mut().poll(&mut Context::from_waker(&noop_waker())).is_pending());
    drop(changed);
    assert_eq!(signal.generation.receiver_count(), 0);
}

#[test]
fn readiness_after_subscription_wakes_the_async_host() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct Count(AtomicUsize);
    impl Wake for Count {
        fn wake(self: Arc<Self>) { self.0.fetch_add(1, Ordering::SeqCst); }
    }
    let signal = Arc::new(Signal::default());
    let notifications = Arc::new(Count(AtomicUsize::new(0)));
    let host = Waker::from(Arc::clone(&notifications));
    let mut changed = Box::pin(signal.changed(signal.generation()));
    assert!(changed.as_mut().poll(&mut Context::from_waker(&host)).is_pending());
    Waker::from(Arc::clone(&signal)).wake();
    assert_eq!(notifications.0.load(Ordering::SeqCst), 1);
    assert!(changed.as_mut().poll(&mut Context::from_waker(&host)).is_ready());
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn native_readiness_before_sleep_is_not_lost() {
    let signal = Arc::new(Signal::default());
    let generation = signal.generation();
    Waker::from(Arc::clone(&signal)).wake();
    futures::executor::block_on(signal.changed(generation));
}
