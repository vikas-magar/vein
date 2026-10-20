//! Immutable shared filter sets and their basic metadata.

use std::fmt::Debug;
use std::sync::Arc;

use common::Result;
use common::statistics::OperatorStatisticsHandle;
use error::VeinError;
use types::ColumnId;

use super::contract::RuntimeFilter;
use evaluation::{FILTER_ROWS_KEPT, FILTER_ROWS_SEEN, RuntimeFilterCounters};

mod binding;
mod evaluation;
mod metadata;

pub use evaluation::RuntimeFilterBinding;

/// Immutable, shareable set of filters for one operator's probe input.
#[derive(Clone)]
pub struct RuntimeFilterSet {
    filters: Arc<[Arc<dyn RuntimeFilter>]>,
    statistics: Option<RuntimeFilterCounters>,
}

impl Debug for RuntimeFilterSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RuntimeFilterSet")
            .field("filter_count", &self.filters.len())
            .field("required_columns", &self.required_column_ids())
            .finish()
    }
}

impl RuntimeFilterSet {
    /// Create a shareable set from filters built for the same probe input.
    pub fn new(filters: Vec<Arc<dyn RuntimeFilter>>) -> Self {
        Self::with_optional_statistics(filters, None)
    }

    /// Creates a set whose runtime feedback is recorded by the query's shared
    /// operator statistics handle.
    pub fn with_statistics(
        filters: Vec<Arc<dyn RuntimeFilter>>,
        statistics: Arc<OperatorStatisticsHandle>,
    ) -> Self {
        Self::with_optional_statistics(filters, Some(statistics))
    }

    fn with_optional_statistics(
        filters: Vec<Arc<dyn RuntimeFilter>>,
        statistics: Option<Arc<OperatorStatisticsHandle>>,
    ) -> Self {
        Self {
            filters: Arc::from(filters),
            statistics: statistics.map(|statistics| RuntimeFilterCounters {
                rows_seen: statistics.register_counter(FILTER_ROWS_SEEN),
                rows_kept: statistics.register_counter(FILTER_ROWS_KEPT),
            }),
        }
    }

    /// Build a set from policy-selected candidate indices without cloning filter
    /// state. Candidate handles are shared; duplicate or out-of-range indices
    /// are rejected as a policy contract violation.
    pub fn from_selection(
        candidates: &[Arc<dyn RuntimeFilter>],
        selected_indices: &[usize],
    ) -> Result<Self> {
        Self::from_selection_with_optional_statistics(candidates, selected_indices, None)
    }

    /// Selects candidate handles and records feedback through shared query stats.
    pub fn from_selection_with_statistics(
        candidates: &[Arc<dyn RuntimeFilter>],
        selected_indices: &[usize],
        statistics: Arc<OperatorStatisticsHandle>,
    ) -> Result<Self> {
        Self::from_selection_with_optional_statistics(
            candidates,
            selected_indices,
            Some(statistics),
        )
    }

    fn from_selection_with_optional_statistics(
        candidates: &[Arc<dyn RuntimeFilter>],
        selected_indices: &[usize],
        statistics: Option<Arc<OperatorStatisticsHandle>>,
    ) -> Result<Self> {
        let mut selected = Vec::with_capacity(selected_indices.len());
        let mut seen = std::collections::HashSet::with_capacity(selected_indices.len());
        for &index in selected_indices {
            if index >= candidates.len() || !seen.insert(index) {
                return Err(VeinError::internal(format!(
                    "runtime filter policy returned invalid candidate index {index}"
                )));
            }
            selected.push(Arc::clone(&candidates[index]));
        }
        Ok(Self::with_optional_statistics(selected, statistics))
    }

    /// An empty set, used when a source or plan cannot support pushdown.
    pub fn empty() -> Self {
        Self::new(Vec::new())
    }

    /// Whether this set contains any filters.
    pub fn is_empty(&self) -> bool {
        self.filters.is_empty()
    }

    /// Number of independent filters in the set.
    pub fn len(&self) -> usize {
        self.filters.len()
    }

    /// Estimated retained bytes across all filters.
    pub fn memory_bytes(&self) -> usize {
        self.filters
            .iter()
            .fold(0usize, |total, filter| total.saturating_add(filter.memory_bytes()))
    }

    /// Filter implementation names, in evaluation order.
    pub fn names(&self) -> Vec<&'static str> {
        self.filters.iter().map(|filter| filter.name()).collect()
    }

    /// Distinct required column IDs in stable first-seen order.
    pub fn required_column_ids(&self) -> Vec<ColumnId> {
        let mut columns = Vec::new();
        for filter in self.filters.iter() {
            for column in filter.required_columns() {
                if !columns.contains(column) {
                    columns.push(*column);
                }
            }
        }
        columns
    }

    /// Whether at least one filter can evaluate rows after scan decoding.
    pub fn has_row_filters(&self) -> bool {
        self.filters
            .iter()
            .any(|filter| filter.supports_row_evaluation())
    }
}

impl Default for RuntimeFilterSet {
    fn default() -> Self {
        Self::empty()
    }
}
