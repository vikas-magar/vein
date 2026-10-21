use std::time::Duration;

use parquet::arrow::arrow_reader::{PredicateOrder, PredicateOrderPolicy};

use super::{AdaptiveFilterOrder, State};

impl AdaptiveFilterOrder {
    fn observe(&self, elapsed: Duration, epoch: u64) {
        let mut state = self.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        if epoch != state.epoch {
            state.stale_observations = state.stale_observations.saturating_add(1);
            return;
        }
        state.iteration_count = state.iteration_count.saturating_add(1);
        state.runtime_sum_ns += elapsed.as_nanos() as f64;
        state.observations = state.observations.saturating_add(1);
        if state.warmup {
            if state.iteration_count == self.config.warmup_samples {
                reset_window(&mut state);
                state.warmup = false;
            }
            return;
        }

        let decision = if state.observing && state.iteration_count == self.config.trial_samples {
            Some(finish_trial(&mut state))
        } else if !state.observing && state.iteration_count == self.config.baseline_samples {
            start_trial(self, &mut state);
            None
        } else {
            None
        };
        drop(state);

        if let Some((reverted, index, before, after, baseline_ns, trial_ns)) = decision {
            tracing::info!(target: "vein.parquet_adaptive_filter", event = "parquet_adaptive_filter_reordered",
                outcome = if reverted { "reverted" } else { "kept" },
                swap_index = index, order_before = ?before, order_after = ?after,
                baseline_mean_us = baseline_ns / 1_000.0,
                trial_mean_us = trial_ns / 1_000.0,
                "adaptive Parquet filter order trial completed");
        }
    }
}

impl PredicateOrderPolicy for AdaptiveFilterOrder {
    fn next_order(&self, current_order: &[usize], current_epoch: u64) -> Option<PredicateOrder> {
        let state = self.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        (state.permutation != current_order || state.epoch != current_epoch).then(|| {
            PredicateOrder {
                identities: state.permutation.clone(),
                epoch: state.epoch,
            }
        })
    }

    fn observe_filter(&self, elapsed: Duration, epoch: u64) {
        self.observe(elapsed, epoch);
    }
}

fn finish_trial(state: &mut State) -> (bool, usize, Vec<usize>, Vec<usize>, f64, f64) {
    let trial_mean_ns = state.runtime_sum_ns / state.iteration_count as f64;
    let index = state.swap_index;
    let mut baseline_order = state.permutation.clone();
    baseline_order.swap(index, index + 1);
    let reverted = state.previous_mean_ns - trial_mean_ns <= 0.0;
    if reverted {
        state.permutation.swap(index, index + 1);
        state.epoch = state.epoch.saturating_add(1);
        let likelihood = &mut state.swap_likeliness[index];
        if *likelihood > 1 {
            *likelihood /= 2;
        }
        state.reverted_swaps = state.reverted_swaps.saturating_add(1);
    } else {
        state.swap_likeliness[index] = 100;
        state.accepted_swaps = state.accepted_swaps.saturating_add(1);
    }
    let decision = (reverted, index, baseline_order, state.permutation.clone(),
                    state.previous_mean_ns, trial_mean_ns);
    state.observing = false;
    reset_window(state);
    decision
}

fn start_trial(policy: &AdaptiveFilterOrder, state: &mut State) {
    state.previous_mean_ns = state.runtime_sum_ns / state.iteration_count as f64;
    let random = next_random(&mut state.random_state)
        % policy.right_random_border.saturating_sub(1).max(1)
        + 1;
    let index = usize::try_from(random / 100).unwrap_or(usize::MAX);
    let likelihood = random - 100 * index as u64;
    if index < state.swap_likeliness.len()
        && u64::from(state.swap_likeliness[index]) > likelihood
    {
        state.permutation.swap(index, index + 1);
        state.epoch = state.epoch.saturating_add(1);
        state.swap_index = index;
        state.observing = true;
        state.swap_attempts = state.swap_attempts.saturating_add(1);
        tracing::debug!(target: "vein.parquet_adaptive_filter", event = "parquet_adaptive_filter_trial_started",
            swap_index = index, baseline_mean_us = state.previous_mean_ns / 1_000.0,
            "adaptive Parquet filter swap trial started");
    }
    reset_window(state);
}

fn reset_window(state: &mut State) {
    state.iteration_count = 0;
    state.runtime_sum_ns = 0.0;
}

fn next_random(state: &mut u64) -> u64 {
    let mut value = *state;
    value ^= value << 13;
    value ^= value >> 7;
    value ^= value << 17;
    *state = value.max(1);
    *state
}
