use super::super::execution::stage::Action::*;
use super::{SchedulerStep, budget, finish, scheduler};
use std::sync::atomic::Ordering;

#[test]
fn synchronous_repeated_wakes_during_registration_are_not_lost_or_duplicated() {
    let (scheduler, records, waits, _) =
        scheduler(vec![vec![Block(1), Finish], vec![Need, AcceptEof, Finish]]);
    waits.on_register(|waker| {
        for _ in 0..10 {
            waker.wake_by_ref();
        }
        Ok(())
    });
    assert_eq!(
        scheduler.run_next(budget()).unwrap(),
        SchedulerStep::Progress
    );
    assert_eq!(
        scheduler.run_next(budget()).unwrap(),
        SchedulerStep::Finished
    );
    assert_eq!(
        scheduler.run_next(budget()).unwrap(),
        SchedulerStep::Finished
    );
    assert_eq!(records[0].lock().unwrap().steps, 2);
    assert_eq!((waits.registrations(), waits.dropped()), (1, 1));
}

#[test]
fn parked_wakes_enqueue_one_task_and_old_generations_cannot_wake_new_waits() {
    let (scheduler, records, waits, notifications) = scheduler(vec![
        vec![Block(1), Block(2), Finish],
        vec![Need, AcceptEof, Finish],
    ]);
    assert_eq!(
        scheduler.run_next(budget()).unwrap(),
        SchedulerStep::Progress
    );
    assert_eq!(scheduler.run_next(budget()).unwrap(), SchedulerStep::Idle);
    let before = notifications.0.load(Ordering::Relaxed);
    for _ in 0..10 {
        waits.wake(0);
    }
    assert_eq!(notifications.0.load(Ordering::Relaxed), before + 1);
    assert_eq!(
        scheduler.run_next(budget()).unwrap(),
        SchedulerStep::Progress
    );
    assert_eq!(waits.registrations(), 2);
    waits.wake(0);
    assert_eq!(scheduler.run_next(budget()).unwrap(), SchedulerStep::Idle);
    assert_eq!(records[0].lock().unwrap().steps, 2);
    waits.wake(1);
    finish(&scheduler);
    assert_eq!(waits.dropped(), 2);
}

#[test]
fn registration_and_unsubscription_callbacks_may_reenter_without_a_scheduler_lock() {
    let (scheduler, _, waits, _) =
        scheduler(vec![vec![Block(1), Finish], vec![Need, AcceptEof, Finish]]);
    let weak = std::sync::Arc::downgrade(&scheduler);
    waits.on_register(move |waker| {
        assert_eq!(
            weak.upgrade().unwrap().run_next(budget()).unwrap(),
            SchedulerStep::Idle
        );
        waker.wake_by_ref();
        Ok(())
    });
    assert_eq!(
        scheduler.run_next(budget()).unwrap(),
        SchedulerStep::Progress
    );
    let weak = std::sync::Arc::downgrade(&scheduler);
    waits.on_drop(move || {
        assert_eq!(
            weak.upgrade().unwrap().run_next(budget()).unwrap(),
            SchedulerStep::Idle
        );
    });
    finish(&scheduler);
}

#[test]
fn only_one_worker_can_execute_a_local_state_at_a_time() {
    use std::sync::{Arc, mpsc};
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
    assert_eq!(scheduler.run_next(budget()).unwrap(), SchedulerStep::Idle);
    release.send(()).unwrap();
    assert_eq!(worker.join().unwrap(), SchedulerStep::Finished);
    assert_eq!(records[0].lock().unwrap().steps, 2);
    assert_eq!(records[0].lock().unwrap().combined, 1);
}
