use super::super::execution::stage::Action::*;
use super::{SchedulerStep, budget, scheduler};
use crate::pipeline::test_support::dense_view;
use arrow::array::{ArrayRef, Int64Array};
use common::VeinError;
use std::sync::Arc;

#[test]
fn cancellation_before_start_prevents_operator_creation_and_execution() {
    let array: ArrayRef = Arc::new(Int64Array::from(vec![7]));
    let (scheduler, records, waits, _) = scheduler(vec![
        vec![Output(dense_view(vec![Arc::clone(&array)])), Finish],
        vec![Need, AcceptEof, Finish],
    ]);
    scheduler.cancel();
    scheduler.cancel();
    assert_eq!(
        scheduler.run_next(budget()).unwrap(),
        SchedulerStep::Cancelled
    );
    assert_eq!(records[0].lock().unwrap().steps, 0);
    assert_eq!(waits.registrations(), 0);
    assert_eq!(
        Arc::strong_count(&array),
        1,
        "unstarted definitions must release their resources"
    );
}

#[test]
fn cancellation_during_registration_discards_the_subscription_and_pending_output() {
    let array: ArrayRef = Arc::new(Int64Array::from(vec![7]));
    let (scheduler, records, waits, _) = scheduler(vec![
        vec![Output(dense_view(vec![Arc::clone(&array)])), Finish],
        vec![
            Need,
            RetryBatch,
            Block(1),
            Need,
            AcceptBatch,
            Need,
            AcceptEof,
            Finish,
        ],
    ]);
    let weak = Arc::downgrade(&scheduler);
    waits.on_register(move |waker| {
        weak.upgrade().unwrap().cancel();
        waker.wake_by_ref();
        Ok(())
    });
    assert_eq!(
        scheduler.run_next(budget()).unwrap(),
        SchedulerStep::Cancelled
    );
    assert_eq!(waits.dropped(), 1);
    assert_eq!(Arc::strong_count(&array), 1);
    assert_eq!(records[0].lock().unwrap().combined, 0);
    waits.wake(0);
    assert_eq!(
        scheduler.run_next(budget()).unwrap(),
        SchedulerStep::Cancelled
    );
}

#[test]
fn cancelling_a_parked_task_drops_its_guard_outside_the_scheduler_lock() {
    let (scheduler, records, waits, _) =
        scheduler(vec![vec![Block(1), Finish], vec![Need, AcceptEof, Finish]]);
    scheduler.run_next(budget()).unwrap();
    let weak = Arc::downgrade(&scheduler);
    waits.on_drop(move || {
        assert_eq!(
            weak.upgrade().unwrap().run_next(budget()).unwrap(),
            SchedulerStep::Cancelled
        );
    });
    scheduler.cancel();
    assert_eq!(waits.dropped(), 1);
    waits.wake(0);
    assert_eq!(records[0].lock().unwrap().steps, 1);
    assert_eq!(
        scheduler.run_next(budget()).unwrap(),
        SchedulerStep::Cancelled
    );
}

#[test]
fn unsubscription_can_cancel_a_claimed_task_before_its_next_execution_turn() {
    let (scheduler, records, waits, _) =
        scheduler(vec![vec![Block(1), Finish], vec![Need, AcceptEof, Finish]]);
    scheduler.run_next(budget()).unwrap();
    let weak = Arc::downgrade(&scheduler);
    waits.on_drop(move || weak.upgrade().unwrap().cancel());
    waits.wake(0);
    assert_eq!(
        scheduler.run_next(budget()).unwrap(),
        SchedulerStep::Cancelled
    );
    assert_eq!(records[0].lock().unwrap().steps, 1);
}

#[test]
fn registrar_errors_and_operator_errors_stop_execution() {
    let (scheduler, records, waits, _) =
        scheduler(vec![vec![Block(1), Finish], vec![Need, AcceptEof, Finish]]);
    waits.on_register(|_| Err(VeinError::internal("injected subscription failure")));
    assert!(
        scheduler
            .run_next(budget())
            .unwrap_err()
            .to_string()
            .contains("injected subscription failure")
    );
    assert!(scheduler.run_next(budget()).is_err());
    assert_eq!(records[0].lock().unwrap().combined, 0);
    let (other, _, _, _) = super::scheduler(vec![vec![Fail], vec![Need, AcceptEof, Finish]]);
    assert!(other.run_next(budget()).is_err());
    assert!(other.run_next(budget()).is_err());
}

#[test]
fn unwinding_a_worker_turn_does_not_leave_the_scheduler_running_forever() {
    let (scheduler, _, _, _) = scheduler(vec![
        vec![
            Hook(Box::new(|| panic!("injected worker shutdown"))),
            Finish,
        ],
        vec![Need, AcceptEof, Finish],
    ]);
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        scheduler.run_next(budget())
    }));
    assert!(result.is_err());
    assert!(scheduler.run_next(budget()).is_err());
}

#[test]
fn scheduler_drop_unregisters_waits_and_late_callbacks_are_harmless() {
    let (scheduler, records, waits, _) =
        scheduler(vec![vec![Block(1), Finish], vec![Need, AcceptEof, Finish]]);
    scheduler.run_next(budget()).unwrap();
    drop(scheduler);
    assert_eq!(waits.dropped(), 1);
    waits.wake(0);
    assert_eq!(records[0].lock().unwrap().steps, 1);
}

#[test]
fn cancellation_racing_an_in_flight_turn_prevents_its_rescheduling() {
    use std::sync::mpsc;
    use std::time::Duration;
    let (entered, at) = mpsc::sync_channel(0);
    let (release, go) = mpsc::sync_channel(0);
    let (scheduler, records, _, _) = scheduler(vec![
        vec![
            Hook(Box::new(move || {
                entered.send(()).unwrap();
                go.recv_timeout(Duration::from_secs(5)).unwrap();
            })),
            Finish,
        ],
        vec![Need, AcceptEof, Finish],
    ]);
    let running = Arc::clone(&scheduler);
    let worker = std::thread::spawn(move || running.run_next(budget()).unwrap());
    at.recv_timeout(Duration::from_secs(5)).unwrap();
    scheduler.cancel();
    assert_eq!(
        scheduler.run_next(budget()).unwrap(),
        SchedulerStep::Cancelled
    );
    release.send(()).unwrap();
    assert_eq!(worker.join().unwrap(), SchedulerStep::Cancelled);
    let steps = records[0].lock().unwrap().steps;
    assert_eq!(
        scheduler.run_next(budget()).unwrap(),
        SchedulerStep::Cancelled
    );
    assert_eq!(records[0].lock().unwrap().steps, steps);
}
