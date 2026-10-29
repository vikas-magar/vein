// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! [`FilterInfo`] state machine for evaluating row filters

use crate::arrow::ProjectionMask;
use crate::arrow::array_reader::{CacheOptionsBuilder, RowGroupCache};
use crate::arrow::arrow_reader::{ArrowPredicate, RowFilter};
use std::num::NonZeroUsize;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use web_time::Instant;

/// State machine for evaluating a sequence of predicates.
///
/// The `FilterInfo` owns the [`RowFilter`] being evaluated and tracks the current
/// predicate to evaluate.
#[derive(Debug)]
pub(super) struct FilterInfo {
    /// The predicates to evaluate, in order
    ///
    /// RowFilter is owned by `FilterInfo` because they may be mutated as part
    /// of evaluation. Specifically, [`ArrowPredicate`] requires &mut self for
    /// evaluation.
    filter: RowFilter,
    /// The next filter to be evaluated
    next_predicate: NonZeroUsize,
    /// Previously computed filter results
    cache_info: CacheInfo,
    /// Wall time spent requesting, reading, and evaluating this window’s predicate chain.
    filter_timing: FilterTiming,
}

#[derive(Debug, Default)]
struct FilterTiming {
    started_at: Option<Instant>,
    elapsed_nanos: u64,
}

impl FilterTiming {
    fn start(&mut self, now: Instant) {
        self.started_at.get_or_insert(now);
    }

    fn finish(&mut self, now: Instant) {
        if let Some(started_at) = self.started_at.take() {
            self.elapsed_nanos = self.elapsed_nanos.saturating_add(
                u64::try_from(now.saturating_duration_since(started_at).as_nanos())
                    .unwrap_or(u64::MAX),
            );
        }
    }

    fn elapsed(&self) -> Duration {
        Duration::from_nanos(self.elapsed_nanos)
    }
}

/// Predicate cache
///
/// Note this is basically the same as CacheOptionsBuilder
/// but it owns the ProjectionMask and RowGroupCache
#[derive(Debug)]
pub(super) struct CacheInfo {
    /// The columns to cache in the predicate cache.
    /// Normally these are the columns that filters may look at such that
    /// if we have a filter like `(a + 10 > 5) AND (a + b = 0)` we cache `a` to avoid re-reading it between evaluating `a + 10 > 5` and `a + b = 0`.
    cache_projection: ProjectionMask,
    row_group_cache: Arc<RwLock<RowGroupCache>>,
}

impl CacheInfo {
    pub(super) fn new(
        cache_projection: ProjectionMask,
        row_group_cache: Arc<RwLock<RowGroupCache>>,
    ) -> Self {
        Self {
            cache_projection,
            row_group_cache,
        }
    }

    pub(super) fn builder(&self) -> CacheOptionsBuilder<'_> {
        CacheOptionsBuilder::new(&self.cache_projection, &self.row_group_cache)
    }
}

pub(super) enum AdvanceResult {
    /// Advanced to the next predicate
    Continue(FilterInfo),
    /// No more predicates returns the row filter and cache info
    Done(RowFilter, CacheInfo),
}

impl FilterInfo {
    /// Create a new FilterInfo
    pub(super) fn new(filter: RowFilter, cache_info: CacheInfo) -> Self {
        Self {
            filter,
            next_predicate: NonZeroUsize::new(1).expect("1 is always non-zero"),
            cache_info,
            filter_timing: FilterTiming::default(),
        }
    }

    /// Start measuring predicate work once its required bytes are available.
    ///
    /// The async reader may yield while those bytes are fetched. Keeping that
    /// wait outside the adaptive-filter sample avoids attributing scheduler or
    /// storage latency to the predicate's CPU cost.
    pub(super) fn start_filter_evaluation(&mut self) {
        self.filter_timing.start(Instant::now());
    }

    /// Finish timing predicate decoding and evaluation for this window.
    pub(super) fn finish_filter_evaluation(&mut self) {
        self.filter_timing.finish(Instant::now());
    }

    fn observe_filter(&self) {
        self.filter.observe_filter_duration(self.filter_timing.elapsed());
    }

    /// Advance to the next predicate
    ///
    /// Returns
    /// * [`AdvanceResult::Continue`] returning the `FilterInfo` if there are
    ///   more predicate to evaluate.
    /// * [`AdvanceResult::Done`] with the inner [`RowFilter`] and [`CacheInfo]`
    ///   if there are no more predicates
    pub(super) fn advance(mut self) -> AdvanceResult {
        if self.next_predicate.get() >= self.filter.predicates.len() {
            self.observe_filter();
            AdvanceResult::Done(self.filter, self.cache_info)
        } else {
            self.next_predicate = self
                .next_predicate
                .checked_add(1)
                .expect("no usize overflow");
            AdvanceResult::Continue(self)
        }
    }

    /// Return a mutable reference to the current predicate
    pub(super) fn current_mut(&mut self) -> &mut dyn ArrowPredicate {
        let current_index = self.next_predicate.get() - 1;
        let predicate = self.filter
            .predicates
            .get_mut(current_index)
            // advance ensures next_predicate is always in bounds
            .unwrap()
            .as_mut();
        predicate.set_first_filter(current_index == 0);
        predicate
    }

    /// Return the current predicate to evaluate
    pub(super) fn current(&self) -> &dyn ArrowPredicate {
        self.filter
            .predicates
            .get(self.next_predicate.get() - 1)
            // advance ensures next_predicate is always in bounds
            .unwrap()
            .as_ref()
    }

    /// Returns `true` if the current predicate is the last one in the chain
    /// (i.e. the next call to [`Self::advance`] will return
    /// [`AdvanceResult::Done`]).
    pub(super) fn is_last(&self) -> bool {
        self.next_predicate.get() == self.filter.predicates.len()
    }

    /// Return a reference to the cache projection
    pub(super) fn cache_projection(&self) -> &ProjectionMask {
        &self.cache_info.cache_projection
    }

    /// Return a cache builder to save the results of predicate evaluation
    pub(super) fn cache_builder(&self) -> CacheOptionsBuilder<'_> {
        self.cache_info.builder()
    }

    /// Returns the inner filter, consuming this FilterInfo
    pub(super) fn into_filter(self) -> RowFilter {
        self.observe_filter();
        self.filter
    }
}

#[cfg(test)]
#[path = "filter_timing_tests.rs"]
mod timing_tests;
