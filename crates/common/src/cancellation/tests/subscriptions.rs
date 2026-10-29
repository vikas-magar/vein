use super::super::CancellationToken;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

#[test]
fn subscriptions_observe_existing_and_future_cancellation_exactly_once() {
    for already_cancelled in [false, true] {
        let token = CancellationToken::new();
        if already_cancelled {
            token.cancel();
        }
        let calls = Arc::new(AtomicUsize::new(0));
        let count = Arc::clone(&calls);
        let _registration = token
            .on_cancel(move || {
                count.fetch_add(1, Ordering::SeqCst);
            })
            .unwrap();
        token.cancel();
        token.cancel();
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
}

#[test]
fn dropping_registration_removes_callback_and_releases_captures() {
    let token = CancellationToken::new();
    let owner = Arc::new(());
    let captured = Arc::clone(&owner);
    let registration = token
        .on_cancel(move || {
            drop(captured);
            panic!("unregistered callback");
        })
        .unwrap();
    drop(registration);
    assert_eq!(Arc::strong_count(&owner), 1);
    token.cancel();
}

#[test]
fn callback_can_reenter_cancel_and_register_without_deadlocking() {
    let token = CancellationToken::new();
    let same = token.clone();
    let calls = Arc::new(AtomicUsize::new(0));
    let inner = Arc::clone(&calls);
    let _registration = token
        .on_cancel(move || {
            same.cancel();
            let _nested = same
                .on_cancel(move || {
                    inner.fetch_add(1, Ordering::SeqCst);
                })
                .unwrap();
        })
        .unwrap();
    token.cancel();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[test]
fn panicking_subscription_does_not_skip_remaining_cleanup() {
    let token = CancellationToken::new();
    let _panic = token
        .on_cancel(|| panic!("injected cleanup failure"))
        .unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let inner = Arc::clone(&calls);
    let _good = token
        .on_cancel(move || {
            inner.fetch_add(1, Ordering::SeqCst);
        })
        .unwrap();
    token.cancel();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[test]
fn racing_subscription_and_cancellation_cannot_lose_the_callback() {
    for _ in 0..64 {
        let token = CancellationToken::new();
        let calls = Arc::new(AtomicUsize::new(0));
        std::thread::scope(|scope| {
            let root = &token;
            let cancelling = scope.spawn(move || root.cancel());
            let count = Arc::clone(&calls);
            let registration = token
                .on_cancel(move || {
                    count.fetch_add(1, Ordering::SeqCst);
                })
                .unwrap();
            cancelling.join().unwrap();
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            drop(registration);
        });
    }
}
