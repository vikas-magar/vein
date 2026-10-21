//! Feedback-driven adjacent predicate swaps for staged Parquet filters.

use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hasher};
use std::sync::Mutex;

#[path = "adaptive_order/observe.rs"]
mod observe;

/// Window sizes mirror DuckDB's adaptive filter defaults and remain configurable.
#[derive(Clone, Debug)]
pub struct AdaptiveFilterConfig {
    pub warmup_samples: usize,
    pub baseline_samples: usize,
    pub trial_samples: usize,
    pub random_seed: u64,
}

impl Default for AdaptiveFilterConfig {
    fn default() -> Self {
        let random_seed = RandomState::new().build_hasher().finish();
        Self {
            warmup_samples: 5,
            baseline_samples: 20,
            trial_samples: 10,
            random_seed,
        }
    }
}

#[derive(Debug)]
struct State {
    permutation: Vec<usize>,
    swap_likeliness: Vec<u32>,
    iteration_count: usize,
    runtime_sum_ns: f64,
    previous_mean_ns: f64,
    warmup: bool,
    observing: bool,
    swap_index: usize,
    random_state: u64,
    observations: u64,
    swap_attempts: u64,
    accepted_swaps: u64,
    reverted_swaps: u64,
    stale_observations: u64,
    epoch: u64,
}

/// A cheap snapshot used for end-of-scan diagnostics.
#[derive(Clone, Debug)]
pub struct AdaptiveFilterSnapshot {
    pub stage_count: usize,
    pub observations: u64,
    pub swap_attempts: u64,
    pub accepted_swaps: u64,
    pub reverted_swaps: u64,
    pub stale_observations: u64,
    pub permutation: Vec<usize>,
    pub epoch: u64,
    pub warmup: bool,
}

impl AdaptiveFilterSnapshot {
    pub fn log(&self, path: &str) {
        tracing::info!(target: "vein.parquet_adaptive_filter",
            event = "parquet_adaptive_filter_summary",
            source_id = %common::logging::LogLocation::from(path),
            stage_count = self.stage_count, observations = self.observations,
            swap_attempts = self.swap_attempts, accepted_swaps = self.accepted_swaps,
            reverted_swaps = self.reverted_swaps, order = ?self.permutation,
            stale_observations = self.stale_observations, epoch = self.epoch,
            warmup = self.warmup, "Parquet adaptive filter summary");
    }
}

/// Adaptive order state shared by partitions of one logical scan.
pub struct AdaptiveFilterOrder {
    stage_count: usize,
    config: AdaptiveFilterConfig,
    right_random_border: u64,
    state: Mutex<State>,
}

impl AdaptiveFilterOrder {
    pub fn new(stage_count: usize, config: AdaptiveFilterConfig) -> Option<Self> {
        if stage_count < 2
            || config.warmup_samples == 0
            || config.baseline_samples == 0
            || config.trial_samples == 0
        {
            return None;
        }
        let right_random_border = 100_u64.saturating_mul((stage_count - 1) as u64);
        let random_state = config.random_seed.max(1);
        tracing::info!(target: "vein.parquet_adaptive_filter", event = "parquet_adaptive_filter_initialized",
            stage_count, initial_order = ?(0..stage_count).collect::<Vec<_>>(),
            warmup_samples = config.warmup_samples,
            baseline_samples = config.baseline_samples,
            trial_samples = config.trial_samples,
            "adaptive Parquet filter ordering initialized");
        Some(Self {
            stage_count,
            config,
            right_random_border,
            state: Mutex::new(State {
                permutation: (0..stage_count).collect(),
                swap_likeliness: vec![100; stage_count - 1],
                iteration_count: 0,
                runtime_sum_ns: 0.0,
                previous_mean_ns: 0.0,
                warmup: true,
                observing: false,
                swap_index: 0,
                random_state,
                observations: 0,
                swap_attempts: 0,
                accepted_swaps: 0,
                reverted_swaps: 0,
                stale_observations: 0,
                epoch: 0,
            }),
        })
    }

    pub fn stage_count(&self) -> usize {
        self.stage_count
    }

    pub fn snapshot(&self) -> AdaptiveFilterSnapshot {
        let state = self.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        AdaptiveFilterSnapshot {
            stage_count: self.stage_count,
            observations: state.observations,
            swap_attempts: state.swap_attempts,
            accepted_swaps: state.accepted_swaps,
            reverted_swaps: state.reverted_swaps,
            stale_observations: state.stale_observations,
            permutation: state.permutation.clone(),
            epoch: state.epoch,
            warmup: state.warmup,
        }
    }
}

#[cfg(test)]
#[path = "adaptive_order_tests.rs"]
mod tests;
