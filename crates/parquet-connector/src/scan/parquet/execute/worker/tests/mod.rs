use std::sync::Arc;
use std::time::Duration;

use exec::operators::source::scan::ScanWorkerState;
use exec::runtime::filter::RuntimeFilterSet;
use parquet::arrow::arrow_reader::PredicateOrderPolicy;
use types::VeinSchema;

use super::AdaptiveWorkerState;
use crate::scan::parquet::predicate::AdaptiveFilterOrder;

fn work_unit(worker: &ScanWorkerState) -> Arc<AdaptiveFilterOrder> {
    let previous = worker.with::<AdaptiveWorkerState, _>(|state| state.order.clone()).unwrap();
    let policy = AdaptiveFilterOrder::for_scan(previous, 2, None,
                                               &VeinSchema::empty(), &RuntimeFilterSet::empty()).unwrap();
    worker.with::<AdaptiveWorkerState, _>(|state| state.order = Some(Arc::clone(&policy))).unwrap();
    for _ in 0..5 {
        let epoch = policy.snapshot().epoch;
        policy.observe_filter(Duration::from_micros(100), epoch);
    }
    policy
}

#[test]
fn observations_survive_work_unit_boundaries_and_reach_a_trial() {
    let worker = ScanWorkerState::default();
    let first = work_unit(&worker);
    for _ in 0..4 {
        let later = work_unit(&worker);
        assert!(Arc::ptr_eq(&first, &later));
    }
    let snapshot = first.snapshot();
    assert_eq!(snapshot.observations, 25);
    assert_eq!(snapshot.swap_attempts, 1);
}

#[test]
fn workers_and_queries_do_not_share_observations() {
    let first_worker = ScanWorkerState::default();
    let first = work_unit(&first_worker);
    work_unit(&first_worker);
    let second = work_unit(&ScanWorkerState::default());
    let next_query = work_unit(&ScanWorkerState::default());
    assert!(!Arc::ptr_eq(&first, &second));
    assert!(!Arc::ptr_eq(&first, &next_query));
    assert_eq!(first.snapshot().observations, 10);
    assert_eq!(second.snapshot().observations, 5);
    assert_eq!(next_query.snapshot().observations, 5);
}
