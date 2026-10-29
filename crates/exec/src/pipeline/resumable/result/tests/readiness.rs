use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Wake, Waker};

#[derive(Default)]
struct Counter(AtomicUsize);
impl Wake for Counter {
    fn wake(self: Arc<Self>) { self.0.fetch_add(1, Ordering::Relaxed); }
}
fn watcher() -> (Arc<Counter>, Waker) {
    let counter = Arc::new(Counter::default());
    (Arc::clone(&counter), Waker::from(counter))
}

#[test]
fn publish_before_subscribe_is_observed_without_a_lost_wake() {
    let (mut consumer, mut producers, _, _) = setup(OutputOrder::Unordered, 1, 1);
    let wait = match consumer.try_fetch().unwrap() {
        FetchResult::Pending(wait) => wait,
        _ => panic!()
    };
    send(&mut producers[0], view(1));
    let (counter, waker) = watcher();
    let _guard = wait.register(waker).unwrap();
    assert_eq!(counter.0.load(Ordering::Relaxed), 1);
    assert_eq!(value(&fetch(&mut consumer)), 1);
    producers[0].finish().unwrap();
}

#[test]
fn result_readiness_routes_through_the_scheduler_registrar_without_consuming_other_types() {
    use crate::pipeline::resumable::{ErasedWaitReason, scheduler::WaitRegistrar};
    let (mut consumer, mut producers, _, _) = setup(OutputOrder::Unordered, 1, 1);
    let wait = match consumer.try_fetch().unwrap() {
        FetchResult::Pending(wait) => wait,
        _ => panic!()
    };
    let reason = ErasedWaitReason::new(wait);
    assert!(reason.is::<ResultWait>());
    assert!(!reason.is::<u8>());
    let (counter, waker) = watcher();
    let _guard = ResultWaitRegistrar.register(reason, waker).unwrap();
    producers[0].finish().unwrap();
    assert_eq!(counter.0.load(Ordering::Relaxed), 1);
}

#[test]
fn freeing_capacity_wakes_waiting_producers_once_and_guards_unregister() {
    let (mut consumer, mut producers, tracker, _) = setup(OutputOrder::Unordered, 1, 1);
    send(&mut producers[0], view(1));
    let wait = match producers[0].try_acquire().unwrap() {
        Admission::Blocked(wait) => wait,
        _ => panic!()
    };
    let before = tracker.used();
    let (counter, waker) = watcher();
    let guard = wait.register(waker).unwrap();
    assert!(tracker.used() > before);
    drop(fetch(&mut consumer));
    assert_eq!(counter.0.load(Ordering::Relaxed), 1);
    drop(guard);
    send(&mut producers[0], view(2));
    let wait = match producers[0].try_acquire().unwrap() {
        Admission::Blocked(wait) => wait,
        _ => panic!()
    };
    let (counter, waker) = watcher();
    drop(wait.register(waker).unwrap());
    drop(fetch(&mut consumer));
    assert_eq!(counter.0.load(Ordering::Relaxed), 0);
    producers[0].finish().unwrap();
}

#[test]
fn ordered_frontier_completion_wakes_the_next_producer() {
    let (mut consumer, mut producers, _, _) = setup(OutputOrder::ProducerOrder, 2, 1);
    let wait = match producers[1].try_acquire().unwrap() {
        Admission::Blocked(wait) => wait,
        _ => panic!()
    };
    let (counter, waker) = watcher();
    let _guard = wait.register(waker).unwrap();
    producers[0].finish().unwrap();
    assert_eq!(counter.0.load(Ordering::Relaxed), 1);
    send(&mut producers[1], view(2));
    producers[1].finish().unwrap();
    assert_eq!(value(&fetch(&mut consumer)), 2);
}

#[test]
fn close_wakes_both_data_and_ordered_capacity_subscriptions() {
    let (mut consumer, mut producers, _, _) = setup(OutputOrder::ProducerOrder, 2, 2);
    send(&mut producers[1], view(2));
    let data = match consumer.try_fetch().unwrap() {
        FetchResult::Pending(wait) => wait,
        _ => panic!()
    };
    let space = match producers[1].try_acquire().unwrap() {
        Admission::Blocked(wait) => wait,
        _ => panic!()
    };
    let (reads, read_waker) = watcher();
    let (writes, write_waker) = watcher();
    let _read = data.register(read_waker).unwrap();
    let _write = space.register(write_waker).unwrap();
    consumer.close();
    assert_eq!(reads.0.load(Ordering::Relaxed), 1);
    assert_eq!(writes.0.load(Ordering::Relaxed), 1);
}

#[test]
fn wake_callbacks_may_reenter_buffer_state_without_deadlocking() {
    struct Inspect(std::sync::Weak<super::super::core::Core>);
    impl Wake for Inspect {
        fn wake(self: Arc<Self>) {
            let core = self.0.upgrade().unwrap();
            assert_eq!(core.lock().queued, 1);
        }
    }
    let (mut consumer, mut producers, _, _) = setup(OutputOrder::Unordered, 1, 1);
    let wait = match consumer.try_fetch().unwrap() {
        FetchResult::Pending(wait) => wait,
        _ => panic!()
    };
    let _guard = wait.register(Waker::from(Arc::new(Inspect(Arc::downgrade(&consumer.core))))).unwrap();
    send(&mut producers[0], view(1));
    producers[0].finish().unwrap();
}
