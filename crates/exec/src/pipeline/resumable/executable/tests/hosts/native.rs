use super::*;
use std::time::Duration;

#[test]
fn native_idle_workers_resume_from_io_and_cancellation() {
    for cancel in [false, true] {
        let (plan, records) = prepared(vec![
            vec![Action::Block(1), Action::Finish],
            vec![Action::Need, Action::AcceptEof, Action::Finish],
        ]);
        let io = Io::default();
        let driver = HostDriver::new(&plan, n(1), n(100), io.clone());
        let control = driver.control();
        let (send, receive) = std::sync::mpsc::sync_channel(1);
        let thread = std::thread::spawn(move || { send.send(driver.run_native(n(3))).unwrap(); });
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while io.1.load(Ordering::SeqCst) == 0 {
            assert!(std::time::Instant::now() < deadline, "I/O was never registered");
            std::thread::yield_now();
        }
        assert_eq!(records[0].lock().unwrap().steps, 1);
        if cancel { control.cancel(); } else { io.complete(); }
        let terminal = receive.recv_timeout(Duration::from_secs(5)).unwrap().unwrap();
        thread.join().unwrap();
        assert_eq!(terminal, if cancel { SchedulerStep::Cancelled } else { SchedulerStep::Finished });
        assert!(io.0.lock().unwrap().waker.is_none());
    }
}

#[test]
fn worker_errors_and_panics_stop_other_workers() {
    for panic in [false, true] {
        let action = if panic { Action::Hook(Box::new(|| panic!("injected worker panic"))) } else { Action::Fail };
        let (plan, _) = prepared(vec![vec![action], vec![Action::Need]]);
        let driver = HostDriver::new(&plan, n(1), n(1), Io::default());
        let (send, receive) = std::sync::mpsc::sync_channel(1);
        let thread = std::thread::spawn(move || { send.send(driver.run_native(n(3))).unwrap(); });
        assert!(receive.recv_timeout(Duration::from_secs(5)).unwrap().is_err());
        thread.join().unwrap();
    }
}
