use super::*;
use std::time::Duration;

async fn wait_for_io(io: &Io) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while io.1.load(Ordering::SeqCst) == 0 { tokio::task::yield_now().await; }
    }).await.expect("I/O was never registered");
}

#[tokio::test]
async fn native_idle_workers_resume_from_io_and_cancellation() {
    for cancel in [false, true] {
        let (plan, records) = prepared(vec![
            vec![Action::Block(1), Action::Finish],
            vec![Action::Need, Action::AcceptEof, Action::Finish],
        ]);
        let io = Io::default();
        let driver = HostDriver::new(&plan, n(1), n(100), io.clone());
        let control = driver.control();
        let run = tokio::spawn(driver.run_native(n(3)));
        wait_for_io(&io).await;
        assert_eq!(records[0].lock().unwrap().steps, 1);
        if cancel { control.cancel(); } else { io.complete(); }
        let terminal = tokio::time::timeout(Duration::from_secs(5), run).await.unwrap().unwrap().unwrap();
        assert_eq!(terminal, if cancel { SchedulerStep::Cancelled } else { SchedulerStep::Finished });
        assert!(io.0.lock().unwrap().waker.is_none());
    }
}

#[tokio::test]
async fn worker_errors_and_panics_stop_other_workers() {
    for panic in [false, true] {
        // Exercise real unwinding without the IDE's expensive expected-panic backtrace.
        let action = if panic {
            Action::Hook(Box::new(|| std::panic::resume_unwind(Box::new("injected worker panic"))))
        } else { Action::Fail };
        let (plan, _) = prepared(vec![vec![action], vec![Action::Need]]);
        let driver = HostDriver::new(&plan, n(1), n(1), Io::default());
        assert!(tokio::time::timeout(Duration::from_secs(5), driver.run_native(n(3))).await.unwrap().is_err());
    }
}

#[tokio::test]
async fn abandoning_the_async_owner_cancels_parked_blocking_workers() {
    let (plan, _) = prepared(vec![vec![Action::Block(1)], vec![Action::Need]]);
    let io = Io::default();
    let driver = HostDriver::new(&plan, n(1), n(1), io.clone());
    let run = tokio::spawn(driver.run_native(n(2)));
    wait_for_io(&io).await;
    run.abort();
    assert!(run.await.unwrap_err().is_cancelled());
    tokio::time::timeout(Duration::from_secs(5), async {
        while io.0.lock().unwrap().waker.is_some() { tokio::task::yield_now().await; }
    }).await.expect("abandoned workers must release I/O subscriptions");
}
