//! Storage-independent runtime filter and factory contracts.

use std::fmt::Debug;
use std::sync::Arc;

use arrow::array::BooleanArray;
use arrow::record_batch::RecordBatch;
use catalog::StatisticsPredicate;
use common::Result;
use types::{ColumnId, LogicalLiteral, VeinSchema};

use crate::memory::QueryMemoryTracker;

/// A necessary-condition filter over one or more columns.
///
/// An implementation may return false only when a row cannot match. Returning
/// true for a nonmatching row is a permitted false positive; returning false
/// for a matching row would change query results and violate this contract.
pub trait RuntimeFilter: Debug + Send + Sync {
    /// Stable diagnostic name for this filter implementation.
    fn name(&self) -> &'static str;

    /// Input columns in the order expected by [`Self::evaluate`].
    fn required_columns(&self) -> &[ColumnId];

    /// Approximate bytes retained by this filter, including accounted backing
    /// storage where available.
    fn memory_bytes(&self) -> usize;

    /// Whether evaluation is total and independent of filter order for the
    /// implementation's supported input schema. Unknown implementations remain
    /// fixed in the planner's order by default.
    fn reorder_safe(&self) -> bool {
        false
    }

    /// Evaluate a narrow batch containing only the filter's required columns.
    /// `column_positions` maps each required column to a position in `batch`.
    fn evaluate(
        &self,
        batch: &RecordBatch,
        column_positions: &[usize],
    ) -> Result<BooleanArray>;

    /// Whether this filter has a row-level evaluator. Statistics-only filters
    /// can prune data units before scheduling without adding scan columns or
    /// allocating a per-batch selection mask.
    fn supports_row_evaluation(&self) -> bool {
        true
    }

    /// Format-neutral necessary predicates that may reject a data unit using
    /// exact metadata. An empty result means the filter has no safe metadata
    /// proof for this schema; the scan must retain the unit.
    fn statistics_predicates(&self, _schema: &VeinSchema) -> Vec<StatisticsPredicate> {
        Vec::new()
    }
}

/// Inputs available after a join build completes and before its probe starts.
pub struct RuntimeFilterBuildInput<'a> {
    /// Probe-side key columns in join-key order.
    pub probe_columns: &'a [ColumnId],
    /// Canonical hashes already computed by the build hash table.
    pub build_hashes: &'a [u64],
    /// Exact inclusive bounds for each build key, when available. Bounds are
    /// computed from non-null build rows and may be used only as necessary
    /// conditions on the probe side.
    pub build_key_bounds: Vec<Option<(LogicalLiteral, LogicalLiteral)>>,
    /// Query tracker used to account optional filter memory.
    pub memory: &'a QueryMemoryTracker,
}

/// Creates one or more filters from a completed join build.
///
/// Returning an empty vector means the factory chose an exact-execution
/// fallback, for example because the filter is unsupported or does not fit the
/// remaining query memory budget.
pub trait RuntimeFilterFactory: Debug + Send + Sync {
    /// Build filters for a probe key from the build side's canonical hashes.
    fn build_filters(
        &self,
        input: RuntimeFilterBuildInput<'_>,
    ) -> Result<Vec<Arc<dyn RuntimeFilter>>>;
}
