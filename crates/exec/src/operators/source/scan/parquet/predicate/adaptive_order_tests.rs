use std::time::Duration;

use super::{AdaptiveFilterConfig, AdaptiveFilterOrder};
use parquet::arrow::arrow_reader::{PredicateOrder, PredicateOrderPolicy};

fn policy() -> AdaptiveFilterOrder {
    AdaptiveFilterOrder::new(
        2,
        AdaptiveFilterConfig {
            warmup_samples: 1,
            baseline_samples: 2,
            trial_samples: 2,
            random_seed: 7,
        },
    )
    .expect("two-stage policy")
}

fn begin_trial(policy: &AdaptiveFilterOrder, baseline: Duration) -> PredicateOrder {
    policy.observe_filter(Duration::from_millis(1), 0);
    policy.observe_filter(baseline, 0);
    policy.observe_filter(baseline, 0);
    policy
        .next_order(&[0, 1], 0)
        .expect("baseline should propose an adjacent swap")
}

#[test]
fn keeps_swap_when_trial_window_is_faster() {
    let policy = policy();
    let order = begin_trial(&policy, Duration::from_millis(10));
    assert_eq!(order.identities, [1, 0]);
    policy.observe_filter(Duration::from_millis(1), order.epoch);
    policy.observe_filter(Duration::from_millis(1), order.epoch);

    let snapshot = policy.snapshot();
    assert_eq!(snapshot.permutation, [1, 0]);
    assert_eq!(snapshot.accepted_swaps, 1);
    assert_eq!(snapshot.reverted_swaps, 0);
}

#[test]
fn reverts_swap_when_trial_window_does_not_improve() {
    let policy = policy();
    let order = begin_trial(&policy, Duration::from_millis(1));
    assert_eq!(order.identities, [1, 0]);
    policy.observe_filter(Duration::from_millis(10), order.epoch);
    policy.observe_filter(Duration::from_millis(10), order.epoch);

    let snapshot = policy.snapshot();
    assert_eq!(snapshot.permutation, [0, 1]);
    assert_eq!(snapshot.swap_attempts, 1);
    assert_eq!(snapshot.reverted_swaps, 1);
}

#[test]
fn rejects_invalid_stage_counts_and_retains_unmodified_order() {
    assert!(AdaptiveFilterOrder::new(1, AdaptiveFilterConfig::default()).is_none());
    let policy = policy();
    assert!(policy.next_order(&[0, 1], 0).is_none());
}

#[test]
fn ignores_measurements_started_under_an_old_order_epoch() {
    let policy = policy();
    let trial = begin_trial(&policy, Duration::from_millis(10));
    policy.observe_filter(Duration::from_millis(100), 0);
    assert_eq!(policy.snapshot().observations, 3);
    assert_eq!(policy.snapshot().stale_observations, 1);

    policy.observe_filter(Duration::from_millis(1), trial.epoch);
    policy.observe_filter(Duration::from_millis(1), trial.epoch);
    let completed = policy.snapshot();
    assert_eq!(completed.epoch, trial.epoch);
    assert_eq!(completed.permutation, [1, 0]);
    assert_eq!(completed.accepted_swaps, 1);
}
