mod cleanup;
mod dependencies;
mod readiness;
mod waits;
mod result;

use super::execution::{
    fixture::prepared,
    stage::{Action, Record},
};
use crate::pipeline::resumable::scheduler::{PipelineScheduler, SchedulerStep};
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Wake, Waker};
use waits::Waits;

type Scheduler = PipelineScheduler<Waits>;
fn one() -> NonZeroUsize {
    NonZeroUsize::new(1).unwrap()
}
fn budget() -> NonZeroUsize {
    NonZeroUsize::new(100).unwrap()
}

#[derive(Default)]
struct Notifications(AtomicUsize);
impl Wake for Notifications {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

fn scheduler(
    scripts: Vec<Vec<Action>>,
) -> (
    Arc<Scheduler>,
    Vec<Arc<Mutex<Record>>>,
    Waits,
    Arc<Notifications>,
) {
    let (plan, records) = prepared(scripts);
    let waits = Waits::default();
    let notifications = Arc::new(Notifications::default());
    let scheduler = Arc::new(PipelineScheduler::new(
        &plan,
        one(),
        waits.clone(),
        Waker::from(Arc::clone(&notifications)),
    ));
    (scheduler, records, waits, notifications)
}

fn finish(scheduler: &Scheduler) {
    for _ in 0..1000 {
        match scheduler.run_next(one()).unwrap() {
            SchedulerStep::Finished => return,
            SchedulerStep::Progress => {}
            outcome => panic!("unexpected scheduler state: {outcome:?}"),
        }
    }
    panic!("scheduler failed to finish");
}
