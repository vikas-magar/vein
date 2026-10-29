use super::super::super::ExecutablePipelinePlan;
use super::super::fixtures::*;
use super::{Notifications, SchedulerStep, Waits, budget, finish, one};
use crate::pipeline::resumable::OperatorFactoryRegistry;
use crate::pipeline::resumable::scheduler::PipelineScheduler;
use std::num::NonZeroUsize;
use std::sync::{Arc, atomic::Ordering};
use std::task::Waker;

#[test]
fn join_probe_is_not_created_until_every_build_worker_has_finished_combining() {
    let probe = source(empty());
    let build = source(empty());
    let joined = join(Arc::clone(&probe), Arc::clone(&build));
    let provider = Provider {
        combine: true,
        ..Default::default()
    };
    let plan = ExecutablePipelinePlan::prepare(
        &client(joined),
        &OperatorFactoryRegistry::new(),
        &provider,
    )
    .unwrap();
    let scheduler = PipelineScheduler::new(
        &plan,
        NonZeroUsize::new(3).unwrap(),
        Waits::default(),
        Waker::from(Arc::new(Notifications::default())),
    );
    let records = provider.records.lock().unwrap();
    for finished in 1..=3 {
        assert_eq!(
            scheduler.run_next(budget()).unwrap(),
            SchedulerStep::Progress
        );
        assert_eq!(
            records[&pointer(&build)].combined.lock().unwrap().len(),
            finished
        );
        assert!(records[&pointer(&probe)].created.lock().unwrap().is_empty());
    }
    finish(&scheduler);
    assert_eq!(records[&pointer(&probe)].created.lock().unwrap().len(), 3);
    assert_eq!(records[&pointer(&probe)].combined.lock().unwrap().len(), 3);
}

#[test]
fn serial_capability_limits_worker_creation_even_when_the_host_requests_more() {
    let input = source(empty());
    let mut provider = Provider::default();
    provider.serial.insert(pointer(&input));
    let plan = ExecutablePipelinePlan::prepare(
        &client(Arc::clone(&input)),
        &OperatorFactoryRegistry::new(),
        &provider,
    )
    .unwrap();
    let scheduler = PipelineScheduler::new(
        &plan,
        NonZeroUsize::new(8).unwrap(),
        Waits::default(),
        Waker::from(Arc::new(Notifications::default())),
    );
    finish(&scheduler);
    let records = provider.records.lock().unwrap();
    assert_eq!(records[&pointer(&input)].created.lock().unwrap().len(), 1);
}

#[test]
fn blocking_extensions_finalize_only_after_all_input_pipelines_complete() {
    let (plan, counts) = super::super::finalization::prepared_sink();
    let scheduler = PipelineScheduler::new(
        &plan,
        one(),
        Waits::default(),
        Waker::from(Arc::new(Notifications::default())),
    );
    assert_eq!(
        scheduler.run_next(budget()).unwrap(),
        SchedulerStep::Progress
    );
    assert_eq!(counts.inputs.load(Ordering::Relaxed), 1);
    assert_eq!(counts.finalizations.load(Ordering::Relaxed), 0);
    assert_eq!(
        scheduler.run_next(budget()).unwrap(),
        SchedulerStep::Progress
    );
    assert_eq!(counts.inputs.load(Ordering::Relaxed), 2);
    assert_eq!(counts.finalizations.load(Ordering::Relaxed), 0);
    assert_eq!(
        scheduler.run_next(budget()).unwrap(),
        SchedulerStep::Finished
    );
    assert_eq!(counts.finalizations.load(Ordering::Relaxed), 1);
}

#[test]
fn exhausted_turns_are_requeued_behind_other_ready_workers() {
    let input = source(empty());
    let provider = Provider::default();
    let plan = ExecutablePipelinePlan::prepare(
        &client(Arc::clone(&input)),
        &OperatorFactoryRegistry::new(),
        &provider,
    )
    .unwrap();
    let scheduler = PipelineScheduler::new(
        &plan,
        NonZeroUsize::new(3).unwrap(),
        Waits::default(),
        Waker::from(Arc::new(Notifications::default())),
    );
    let records = provider.records.lock().unwrap();
    for workers_started in 1..=3 {
        assert_eq!(scheduler.run_next(one()).unwrap(), SchedulerStep::Progress);
        assert_eq!(
            records[&pointer(&input)].created.lock().unwrap().len(),
            workers_started
        );
    }
    finish(&scheduler);
}
