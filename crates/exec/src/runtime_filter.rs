//! Execution-local filters that can safely reject rows before payload decoding.
//!
//! The filter contract is independent of any storage format. Scan adapters bind
//! the required column IDs to their source schema and decide whether they can
//! evaluate the filter before materializing projected payload columns.

use std::fmt::Debug;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use arrow::array::{Array, BooleanArray, builder::BooleanBuilder};
use arrow::record_batch::RecordBatch;
use catalog::StatisticsPredicate;
use common::Result;
use error::VeinError;
use types::{ColumnId, LogicalLiteral, ResolvedColumnRef, VeinSchema};

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

/// Immutable, shareable set of filters for one operator's probe input.
#[derive(Clone)]
pub struct RuntimeFilterSet {
    filters: Arc<[Arc<dyn RuntimeFilter>]>,
    metrics: Arc<RuntimeFilterMetrics>,
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
        Self {
            filters: Arc::from(filters),
            metrics: Arc::new(RuntimeFilterMetrics::default()),
        }
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

    /// Collect safe format-neutral predicates for source-level metadata
    /// pruning. These predicates are additional necessary conditions and are
    /// conjoined by the storage adapter with any static scan predicates.
    pub fn statistics_predicates(&self, schema: &VeinSchema) -> Vec<StatisticsPredicate> {
        self.filters
            .iter()
            .flat_map(|filter| filter.statistics_predicates(schema))
            .collect()
    }

    /// Bind the set to a scan schema. `None` means at least one required column
    /// is absent, so a scan must retain its ordinary exact behavior.
    pub fn bind(&self, schema: &VeinSchema) -> Option<RuntimeFilterBinding> {
        if !self.has_row_filters() {
            return None;
        }

        let row_filters = self
            .filters
            .iter()
            .filter(|filter| filter.supports_row_evaluation())
            .collect::<Vec<_>>();
        let mut column_ids = Vec::new();
        for filter in &row_filters {
            for column in filter.required_columns() {
                if !column_ids.contains(column) {
                    column_ids.push(*column);
                }
            }
        }
        // Scan adapters such as Parquet require predicate columns in source
        // order. Keep each filter's key order in its positional mapping below,
        // while exposing the narrow input columns in the scan schema's order.
        let columns = schema
            .fields
            .iter()
            .filter(|field| column_ids.contains(&field.id))
            .cloned()
            .collect::<Vec<ResolvedColumnRef>>();
        if columns.len() != column_ids.len() {
            return None;
        }

        let filters = row_filters
            .iter()
            .map(|filter| {
                let positions = filter
                    .required_columns()
                    .iter()
                    .map(|id| columns.iter().position(|column| column.id == *id))
                    .collect::<Option<Vec<_>>>()?;
                Some(BoundRuntimeFilter {
                    filter: Arc::clone(filter),
                    positions,
                })
            })
            .collect::<Option<Vec<_>>>()?;

        Some(RuntimeFilterBinding {
            columns,
            filters,
            metrics: Arc::clone(&self.metrics),
        })
    }

    /// Number of source-filter rows presented to the runtime filters.
    pub fn rows_seen(&self) -> u64 {
        self.metrics.rows_seen.load(Ordering::Relaxed)
    }

    /// Number of rows retained by all filters in the bound predicate.
    pub fn rows_kept(&self) -> u64 {
        self.metrics.rows_kept.load(Ordering::Relaxed)
    }
}

impl Default for RuntimeFilterSet {
    fn default() -> Self {
        Self::empty()
    }
}

/// Scan-schema fields and positional key mappings for a runtime filter set.
pub struct RuntimeFilterBinding {
    columns: Vec<ResolvedColumnRef>,
    filters: Vec<BoundRuntimeFilter>,
    metrics: Arc<RuntimeFilterMetrics>,
}

impl RuntimeFilterBinding {
    /// Narrow scan columns needed to evaluate the bound filters.
    pub fn columns(&self) -> &[ResolvedColumnRef] {
        &self.columns
    }

    /// Evaluate all filters and return one combined selection mask.
    ///
    /// Only the boolean mask is newly allocated. The input payload arrays are
    /// borrowed through Arrow's `RecordBatch` and are never gathered or copied.
    pub fn evaluate(&self, batch: &RecordBatch) -> Result<BooleanArray> {
        let masks = self
            .filters
            .iter()
            .map(|bound| bound.filter.evaluate(batch, &bound.positions))
            .collect::<Result<Vec<_>>>()?;
        if masks.iter().any(|mask| mask.len() != batch.num_rows()) {
            return Err(VeinError::internal(
                "runtime filter returned a mask with a different row count",
            ));
        }

        let mut output = BooleanBuilder::with_capacity(batch.num_rows());
        let mut rows_kept = 0u64;
        for row in 0..batch.num_rows() {
            let keep = masks
                .iter()
                .all(|mask| mask.is_valid(row) && mask.value(row));
            output.append_value(keep);
            rows_kept += u64::from(keep);
        }
        self.metrics
            .rows_seen
            .fetch_add(batch.num_rows() as u64, Ordering::Relaxed);
        self.metrics.rows_kept.fetch_add(rows_kept, Ordering::Relaxed);
        Ok(output.finish())
    }
}

struct BoundRuntimeFilter {
    filter: Arc<dyn RuntimeFilter>,
    positions: Vec<usize>,
}

#[derive(Default)]
struct RuntimeFilterMetrics {
    rows_seen: AtomicU64,
    rows_kept: AtomicU64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use types::{ResolvedColumn, VeinDataType};

    #[derive(Debug)]
    struct KeepValues {
        columns: Vec<ColumnId>,
        expected: i64,
    }

    impl RuntimeFilter for KeepValues {
        fn name(&self) -> &'static str {
            "test_keep_values"
        }

        fn required_columns(&self) -> &[ColumnId] {
            &self.columns
        }

        fn memory_bytes(&self) -> usize {
            0
        }

        fn evaluate(
            &self,
            batch: &RecordBatch,
            column_positions: &[usize],
        ) -> Result<BooleanArray> {
            let values = batch
                .column(column_positions[0])
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| VeinError::internal("test key must be Int64"))?;
            let mut output = BooleanBuilder::with_capacity(batch.num_rows());
            for row in 0..batch.num_rows() {
                output.append_value(values.is_valid(row) && values.value(row) == self.expected);
            }
            Ok(output.finish())
        }
    }

    fn schema() -> VeinSchema {
        VeinSchema::new(vec![
            Arc::new(ResolvedColumn::new(
                ColumnId(7),
                "id",
                None,
                VeinDataType::Int64,
            )),
            Arc::new(ResolvedColumn::new(
                ColumnId(8),
                "label",
                None,
                VeinDataType::Utf8,
            )),
        ])
    }

    #[test]
    fn binding_maps_filter_columns_by_id_and_preserves_stable_order() {
        let filters: Vec<Arc<dyn RuntimeFilter>> = vec![Arc::new(KeepValues {
            columns: vec![ColumnId(7)],
            expected: 2,
        })];
        let set = RuntimeFilterSet::new(filters);
        let binding = set.bind(&schema()).expect("filter columns bind");

        assert_eq!(binding.columns().len(), 1);
        assert_eq!(binding.columns()[0].id, ColumnId(7));
    }

    #[test]
    fn binding_uses_scan_order_without_changing_filter_key_positions() {
        let set = RuntimeFilterSet::new(vec![
            Arc::new(KeepValues {
                columns: vec![ColumnId(8)],
                expected: 20,
            }),
            Arc::new(KeepValues {
                columns: vec![ColumnId(7)],
                expected: 2,
            }),
        ]);
        let binding = set.bind(&schema()).expect("filter columns bind");
        let bound_ids = binding
            .columns()
            .iter()
            .map(|column| column.id)
            .collect::<Vec<_>>();
        assert_eq!(bound_ids, vec![ColumnId(7), ColumnId(8)]);

        let batch = RecordBatch::try_from_iter(vec![
            (
                "id",
                Arc::new(Int64Array::from(vec![1, 2, 3])) as Arc<dyn arrow::array::Array>,
            ),
            (
                "label",
                Arc::new(Int64Array::from(vec![10, 20, 30])) as Arc<dyn arrow::array::Array>,
            ),
        ])
            .expect("test batch");
        let mask = binding.evaluate(&batch).expect("evaluate bound filters");

        assert_eq!(mask, BooleanArray::from(vec![false, true, false]));
    }

    #[test]
    fn missing_filter_column_declines_pushdown() {
        let set = RuntimeFilterSet::new(vec![Arc::new(KeepValues {
            columns: vec![ColumnId(99)],
            expected: 2,
        })]);

        assert!(set.bind(&schema()).is_none());
    }

    #[test]
    fn combined_runtime_filters_return_a_selection_and_account_rows() {
        let set = RuntimeFilterSet::new(vec![
            Arc::new(KeepValues {
                columns: vec![ColumnId(7)],
                expected: 2,
            }),
            Arc::new(KeepValues {
                columns: vec![ColumnId(7)],
                expected: 2,
            }),
        ]);
        let batch = RecordBatch::try_from_iter(vec![(
            "id",
            Arc::new(Int64Array::from(vec![1, 2, 3])) as Arc<dyn arrow::array::Array>,
        )])
            .expect("test batch");
        let binding = set.bind(&schema()).expect("filter columns bind");
        let mask = binding.evaluate(&batch).expect("combine filters");

        assert_eq!(mask, BooleanArray::from(vec![false, true, false]));
        assert_eq!(set.rows_seen(), 3);
        assert_eq!(set.rows_kept(), 1);
    }

    #[test]
    fn empty_filter_set_does_not_bind_or_allocate_a_mask() {
        assert!(RuntimeFilterSet::empty().bind(&schema()).is_none());
    }
}
