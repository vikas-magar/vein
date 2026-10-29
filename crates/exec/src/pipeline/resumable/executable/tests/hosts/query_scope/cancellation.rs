use super::*;
use futures::task::noop_waker;
use std::task::Context;

#[test]
fn dropping_the_query_guard_wakes_a_cooperative_io_wait_and_unregisters_it() {
    let (plan, records) = prepared(vec![
        vec![Action::Block(1), Action::Finish],
        vec![Action::Need, Action::AcceptEof, Action::Finish],
    ]);
    let resources = resources(&plan);
    let guard = resources.guard();
    let io = Io::default();
    let driver = resources
        .host_driver(&plan, n(1), n(100), io.clone())
        .unwrap();
    let mut host = EventHost::default();
    let mut run = Box::pin(driver.run_cooperative(&mut host));
    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);
    assert!(run.as_mut().poll(&mut cx).is_pending());
    assert!(run.as_mut().poll(&mut cx).is_pending());
    assert_eq!(records[0].lock().unwrap().steps, 1);
    drop(guard);
    assert!(io.0.lock().unwrap().waker.is_none());
    assert_eq!(
        futures::executor::block_on(run).unwrap(),
        SchedulerStep::Cancelled
    );
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn the_same_scope_stops_parked_native_workers() {
    use std::time::{Duration, Instant};
    let (plan, _) = prepared(vec![
        vec![Action::Block(1), Action::Finish],
        vec![Action::Need, Action::AcceptEof, Action::Finish],
    ]);
    let resources = resources(&plan);
    let guard = resources.guard();
    let io = Io::default();
    let driver = resources
        .host_driver(&plan, n(1), n(100), io.clone())
        .unwrap();
    let run = tokio::spawn(driver.run_native(n(3)));
    let deadline = Instant::now() + Duration::from_secs(5);
    while io.1.load(Ordering::SeqCst) == 0 {
        assert!(Instant::now() < deadline);
        tokio::task::yield_now().await;
    }
    drop(guard);
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(5), run).await.unwrap().unwrap().unwrap(),
        SchedulerStep::Cancelled
    );
    assert!(io.0.lock().unwrap().waker.is_none());
}

#[test]
fn driver_failure_or_abandoned_future_cancels_the_query_scope() {
    for abandon in [false, true] {
        let action = if abandon { Action::Work } else { Action::Fail };
        let (plan, _) = prepared(vec![
            vec![action, Action::Finish],
            vec![Action::Need, Action::AcceptEof, Action::Finish],
        ]);
        let resources = resources(&plan);
        let driver = resources
            .host_driver(&plan, n(1), n(1), Io::default())
            .unwrap();
        let mut host = EventHost::default();
        let mut run = Box::pin(driver.run_cooperative(&mut host));
        if abandon {
            let waker = noop_waker();
            assert!(
                run.as_mut()
                    .poll(&mut Context::from_waker(&waker))
                    .is_pending()
            );
            drop(run);
        } else {
            assert!(futures::executor::block_on(run).is_err());
        }
        assert!(resources.cancellation().is_cancelled());
    }
}

#[tokio::test]
async fn scheduler_failure_reaches_the_query_before_cleanup_cancellation() {
    for native in [false, true] {
        let (plan, _) = prepared(vec![vec![Action::Fail], vec![Action::Need]]);
        let scope = resources(&plan);
        let driver = scope.host_driver(&plan, n(1), n(1), Io::default()).unwrap();
        let result = if native {
            #[cfg(not(target_arch = "wasm32"))]
            { driver.run_native(n(4)).await }
            #[cfg(target_arch = "wasm32")]
            { continue; }
        } else { futures::executor::block_on(driver.run_cooperative(&mut EventHost::default())) };
        let error = result.err().unwrap().into_shared();
        let original = scope.query_failure().unwrap();
        assert!(Arc::ptr_eq(&error, &original));
        assert!(original.to_string().contains("injected stage failure"));
        assert_eq!(scope.query_status(), crate::runtime::QueryStatus::Failed);
    }
}
