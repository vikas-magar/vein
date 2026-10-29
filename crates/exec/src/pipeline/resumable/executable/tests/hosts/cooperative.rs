use super::*;
use futures::task::noop_waker;
use std::pin::pin;
use std::task::Context;

#[test]
fn budget_exhaustion_returns_control_before_more_operator_work() {
    let (plan, records) = prepared(vec![
        vec![Action::Work, Action::Work, Action::Finish],
        vec![Action::Need, Action::AcceptEof, Action::Finish],
    ]);
    let driver = HostDriver::new(&plan, n(1), n(1), Io::default());
    let mut host = EventHost::default();
    {
        let mut future = pin!(driver.run_cooperative(&mut host));
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        assert!(future.as_mut().poll(&mut cx).is_pending());
        let before: usize = records.iter().map(|r| r.lock().unwrap().steps).sum();
        assert_eq!(before, 1);
        assert_eq!(futures::executor::block_on(future).unwrap(), SchedulerStep::Finished);
    }
    assert!(host.yields >= 3);
}

#[test]
fn idle_io_wait_does_not_repeat_execution_or_host_yields() {
    let (plan, records) = prepared(vec![
        vec![Action::Block(1), Action::Finish],
        vec![Action::Need, Action::AcceptEof, Action::Finish],
    ]);
    let io = Io::default();
    let driver = HostDriver::new(&plan, n(1), n(100), io.clone());
    let mut host = EventHost::default();
    {
        let mut future = pin!(driver.run_cooperative(&mut host));
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        assert!(future.as_mut().poll(&mut cx).is_pending()); // Productive turn yields.
        assert!(future.as_mut().poll(&mut cx).is_pending()); // Now waiting for I/O.
        for _ in 0..10 { assert!(future.as_mut().poll(&mut cx).is_pending()); }
        assert_eq!(records[0].lock().unwrap().steps, 1);
        assert_eq!(io.1.load(Ordering::SeqCst), 1);
        io.complete();
        assert_eq!(futures::executor::block_on(future).unwrap(), SchedulerStep::Finished);
    }
    assert_eq!(host.yields, 1);
    assert!(io.0.lock().unwrap().waker.is_none());
}

#[test]
fn cancellation_wakes_idle_host_and_dropped_future_unregisters_io() {
    for drop_future in [false, true] {
        let (plan, _) = prepared(vec![
            vec![Action::Block(1), Action::Finish],
            vec![Action::Need, Action::AcceptEof, Action::Finish],
        ]);
        let io = Io::default();
        let driver = HostDriver::new(&plan, n(1), n(100), io.clone());
        let control = driver.control();
        let mut host = EventHost::default();
        let mut future = Box::pin(driver.run_cooperative(&mut host));
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        assert!(future.as_mut().poll(&mut cx).is_pending());
        assert!(future.as_mut().poll(&mut cx).is_pending());
        if drop_future { drop(future); } else {
            control.cancel();
            assert_eq!(futures::executor::block_on(future).unwrap(), SchedulerStep::Cancelled);
        }
        assert!(io.0.lock().unwrap().waker.is_none());
    }
}

#[test]
fn failed_host_yield_cancels_the_owned_scheduler() {
    struct Failed;
    impl CooperativeHost for Failed {
        async fn yield_control(&mut self) -> Result<()> { Err(common::VeinError::internal("host failed")) }
    }
    let (plan, _) = prepared(vec![vec![Action::Work, Action::Finish],
                                  vec![Action::Need, Action::AcceptEof, Action::Finish]]);
    let driver = HostDriver::new(&plan, n(1), n(1), Io::default());
    let error = futures::executor::block_on(driver.run_cooperative(&mut Failed)).unwrap_err();
    assert!(error.to_string().contains("host failed"));
}
