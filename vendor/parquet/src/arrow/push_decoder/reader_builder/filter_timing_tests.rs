use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use arrow_array::BooleanArray;
use crate::arrow::array_reader::RowGroupCache;
use crate::arrow::arrow_reader::{
    ArrowPredicate, ArrowPredicateFn, PredicateOrder, PredicateOrderPolicy, RowFilter,
};
use crate::arrow::ProjectionMask;

use super::{AdvanceResult, CacheInfo, FilterInfo, FilterTiming};

#[test]
fn timing_accumulates_evaluation_passes_without_counting_waits() {
    let origin = Instant::now();
    let mut timing = FilterTiming::default();

    // The gap before the first start and between filter evaluations represents
    // asynchronous I/O and scheduler wait time, which is not predicate cost.
    timing.start(origin + Duration::from_millis(10));
    timing.finish(origin + Duration::from_millis(15));

    timing.start(origin + Duration::from_millis(30));
    timing.finish(origin + Duration::from_millis(37));
    assert_eq!(timing.elapsed(), Duration::from_millis(12));
}

#[test]
fn finish_without_a_started_pass_does_not_change_elapsed_time() {
    let mut timing = FilterTiming::default();
    timing.finish(Instant::now());
    assert_eq!(timing.elapsed(), Duration::ZERO);
}

struct ReverseOrder;

impl PredicateOrderPolicy for ReverseOrder {
    fn next_order(&self, _current: &[usize], epoch: u64) -> Option<PredicateOrder> {
        Some(PredicateOrder {
            identities: vec![1, 0],
            epoch,
        })
    }

    fn observe_filter(&self, _elapsed: Duration, _epoch: u64) {}
}

#[test]
fn first_filter_callback_tracks_active_order_after_reordering() {
    let observed = Arc::new(Mutex::new(Vec::new()));
    let predicate = |identity| {
        let observed = Arc::clone(&observed);
        Box::new(
            ArrowPredicateFn::new(ProjectionMask::all(), |_| {
                Ok(BooleanArray::from(Vec::<bool>::new()))
            })
                .with_first_filter_callback(move |first| {
                    observed.lock().expect("callback state").push((identity, first));
                }),
        ) as Box<dyn ArrowPredicate>
    };
    let mut filter = RowFilter::new(vec![predicate(0), predicate(1)])
        .with_order_policy(Arc::new(ReverseOrder));
    assert!(filter.reorder_for_filter_window());

    let cache = CacheInfo::new(
        ProjectionMask::all(),
        Arc::new(RwLock::new(RowGroupCache::new(1, 0))),
    );
    let mut info = FilterInfo::new(filter, cache);
    info.current_mut();
    let AdvanceResult::Continue(mut info) = info.advance() else {
        panic!("first of two predicates must continue");
    };
    info.current_mut();

    assert_eq!(
        *observed.lock().expect("callback state"),
        [(1, true), (0, false)]
    );
}
