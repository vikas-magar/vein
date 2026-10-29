use super::*;
use std::future::Future;
use futures::task::{noop_waker, ArcWake};

#[derive(Default)]
struct Notifications(AtomicUsize);
impl ArcWake for Notifications {
    fn wake_by_ref(owner: &Arc<Self>) { owner.0.fetch_add(1, Ordering::SeqCst); }
}

#[test]
fn dropping_a_pending_fetch_does_not_close_or_replay_the_cursor() {
    let gate = Gate::default();
    let mut query = handle(vec![Action::Wait(gate.clone()), Action::Batch(view(vec![Some(4)])),
                                Action::Complete(None)]);
    let control = query.control();
    {
        let mut fetch = Box::pin(query.fetch());
        assert!(fetch.as_mut().poll(&mut Context::from_waker(&noop_waker())).is_pending());
    }
    assert_eq!(control.status(), QueryStatus::Running);
    gate.release();
    futures::executor::block_on(async {
        assert_eq!(query.fetch().await.unwrap().unwrap().view().num_rows(), 1);
        assert!(query.fetch().await.unwrap().is_none());
    });
    assert_eq!(control.progress().rows_delivered, 1);
}

#[test]
fn control_cancellation_from_another_thread_wakes_pending_fetch() {
    let gate = Gate::default();
    let mut query = handle(vec![Action::Wait(gate), Action::Complete(None)]);
    let control = query.control();
    let notifications = Arc::new(Notifications::default());
    let waker = futures::task::waker(Arc::clone(&notifications));
    {
        let mut fetch = Box::pin(query.fetch());
        assert!(fetch.as_mut().poll(&mut Context::from_waker(&waker)).is_pending());
        std::thread::spawn(move || control.cancel()).join().unwrap();
        assert!(notifications.0.load(Ordering::SeqCst) > 0);
        let error = futures::executor::block_on(fetch).err().unwrap();
        assert!(matches!(error.as_ref(), VeinError::Cancelled));
    }
    let again = futures::executor::block_on(query.fetch()).err().unwrap();
    assert!(matches!(again.as_ref(), VeinError::Cancelled));
}

#[test]
fn controls_do_not_own_cursor_lifetime_and_pre_cancelled_work_never_runs() {
    let mut query = handle(vec![Action::Complete(None)]);
    let control = query.control();
    drop(control.clone());
    assert_eq!(control.status(), QueryStatus::Prepared);
    control.cancel();
    assert!(futures::executor::block_on(query.fetch()).is_err());
    assert_eq!(control.status(), QueryStatus::Cancelled);
    query.close();
    assert_eq!(control.status(), QueryStatus::Cancelled);
}
