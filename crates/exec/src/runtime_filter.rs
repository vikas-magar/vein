//! Execution-local filters that can safely reject rows before payload decoding.
//!
//! The filter contract is independent of any storage format. Scan adapters bind
//! the required column IDs to their source schema and decide whether they can
//! evaluate the filter before materializing projected payload columns.

use std::fmt::Debug;
use std::sync::Arc;

use arrow::array::{Array, BooleanArray, builder::BooleanBuilder};
use arrow::record_batch::RecordBatch;
use catalog::StatisticsPredicate;
use common::Result;
use common::statistics::{
    OperatorCounter, OperatorStatisticsHandle, QueryStatisticsSnapshot,
};
use error::VeinError;
use types::{ColumnId, LogicalLiteral, ResolvedColumnRef, VeinSchema};

use crate::memory::QueryMemoryTracker;

const FILTER_ROWS_SEEN: &str = "runtime_filter_rows_seen";
const FILTER_ROWS_KEPT: &str = "runtime_filter_rows_kept";

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

/// Query-wide statistics and runtime measurements available at a join
/// build/probe checkpoint.
///
/// These values are execution-local: build cardinalities come from the completed
/// hash table, while the probe estimate is supplied by the scan's pinned plan
/// metadata when available. `query_statistics` is the shared live context; its
/// scan observations retain their own coverage so policies can distinguish
/// partial feedback from complete source statistics.
#[derive(Debug, Clone)]
pub struct RuntimeFilterDecisionContext {
    /// Rows retained by the completed in-memory build table.
    pub build_rows: u64,
    /// Number of non-null build keys represented in the filter input.
    pub build_keys: usize,
    /// Probe rows known from the direct scan's execution metadata.
    pub probe_rows_estimate: Option<u64>,
    /// Revision-pinned identity of the direct probe scan, when available.
    pub probe_source_key: Option<String>,
    /// Query memory available after build completion.
    pub available_memory_bytes: usize,
    /// Consistent point-in-time view of this query's source metadata and runtime feedback.
    pub query_statistics: QueryStatisticsSnapshot,
}

impl RuntimeFilterDecisionContext {
    /// Returns the best authoritative probe cardinality available at this
    /// checkpoint. A complete query-local scan observation supersedes the
    /// planning estimate; incomplete observations never do.
    pub fn probe_rows(&self) -> (Option<u64>, &'static str) {
        self.probe_source_key
            .as_deref()
            .and_then(|key| self.query_statistics.complete_scan_row_count(key))
            .map(|rows| (Some(rows), "complete_runtime_scan"))
            .unwrap_or((self.probe_rows_estimate, "plan_estimate"))
    }
}

/// Chooses which safe runtime-filter candidates to apply to the remaining probe.
///
/// Returned indices address `candidates`. Returning no indices preserves exact
/// join execution without a scan prefilter. Implementations must only choose
/// among the supplied candidates; the executor validates indices and rejects a
/// malformed decision before starting the probe.
pub trait RuntimeFilterDecisionPolicy: std::fmt::Debug + Send + Sync {
    /// Whether it is worth constructing candidates for this checkpoint.
    ///
    /// The default keeps existing behavior. Policies can avoid even transient
    /// candidate allocations when current-query evidence proves there is no
    /// remaining probe work.
    fn should_build_candidates(&self, _context: &RuntimeFilterDecisionContext) -> bool {
        true
    }

    /// Select candidate filters from current-query measurements.
    fn select_filters(
        &self,
        context: &RuntimeFilterDecisionContext,
        candidates: &[Arc<dyn RuntimeFilter>],
    ) -> Result<Vec<usize>>;
}

/// Per-filter capabilities exposed to scan diagnostics without revealing data values.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeFilterPruningDiagnostic {
    /// Stable implementation name.
    pub name: &'static str,
    /// Whether the source adapter can apply this filter to decoded rows.
    pub row_evaluation: bool,
    /// Number of safe metadata predicates available for this scan schema.
    pub metadata_predicate_count: usize,
}

/// Safe default policy: skip filter setup for a provably empty probe; otherwise
/// apply every filter the source adapter can safely evaluate.
#[derive(Debug, Default)]
pub struct ConservativeRuntimeFilterPolicy;

impl RuntimeFilterDecisionPolicy for ConservativeRuntimeFilterPolicy {
    fn should_build_candidates(&self, context: &RuntimeFilterDecisionContext) -> bool {
        context.build_rows > 0
            && context.build_keys > 0
            && context.probe_rows().0 != Some(0)
    }

    fn select_filters(
        &self,
        context: &RuntimeFilterDecisionContext,
        candidates: &[Arc<dyn RuntimeFilter>],
    ) -> Result<Vec<usize>> {
        if context.build_rows == 0
            || context.build_keys == 0
            || context.probe_rows().0 == Some(0)
        {
            return Ok(Vec::new());
        }
        Ok((0..candidates.len()).collect())
    }
}

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

    /// Collect safe format-neutral predicates for source-level metadata
    /// pruning. These predicates are additional necessary conditions and are
    /// conjoined by the storage adapter with any static scan predicates.
    pub fn statistics_predicates(&self, schema: &VeinSchema) -> Vec<StatisticsPredicate> {
        self.collect_statistics_predicates(schema, |_| {})
    }

    /// Collect metadata predicates and a bounded per-filter capability summary
    /// in one pass, so adapters can explain which filters reach metadata pruning.
    pub fn statistics_predicates_with_diagnostics(
        &self,
        schema: &VeinSchema,
    ) -> (Vec<StatisticsPredicate>, Vec<RuntimeFilterPruningDiagnostic>) {
        let mut diagnostics = Vec::with_capacity(self.filters.len());
        let predicates = self.collect_statistics_predicates(schema, |diagnostic| {
            diagnostics.push(diagnostic);
        });
        (predicates, diagnostics)
    }

    fn collect_statistics_predicates(
        &self,
        schema: &VeinSchema,
        mut observe: impl FnMut(RuntimeFilterPruningDiagnostic),
    ) -> Vec<StatisticsPredicate> {
        let mut predicates = Vec::new();
        for filter in self.filters.iter() {
            let filter_predicates = filter.statistics_predicates(schema);
            observe(RuntimeFilterPruningDiagnostic {
                name: filter.name(),
                row_evaluation: filter.supports_row_evaluation(),
                metadata_predicate_count: filter_predicates.len(),
            });
            predicates.extend(filter_predicates);
        }
        predicates
    }

    /// Names of filters that can reject rows after decoding their required
    /// columns. Metadata-only filters are intentionally omitted.
    pub fn row_filter_names(&self) -> Vec<&'static str> {
        self.filters
            .iter()
            .filter(|filter| filter.supports_row_evaluation())
            .map(|filter| filter.name())
            .collect()
    }

    /// Bind the set to a scan schema. `None` means at least one required column
    /// is absent, so a scan must retain its ordinary exact behavior.
    pub fn bind(&self, schema: &VeinSchema) -> Option<RuntimeFilterBinding> {
        let row_filters = self
            .filters
            .iter()
            .filter(|filter| filter.supports_row_evaluation())
            .collect::<Vec<_>>();
        self.bind_filters(schema, &row_filters, RuntimeFilterStatisticsRole::BOTH)
    }

    /// Bind each row filter independently so a scan reader can apply Arrow's
    /// row selection between filters and avoid reading later filter columns for
    /// rows rejected by earlier filters.
    pub fn bind_stages(&self, schema: &VeinSchema) -> Option<Vec<RuntimeFilterBinding>> {
        let row_filters = self
            .filters
            .iter()
            .filter(|filter| filter.supports_row_evaluation())
            .collect::<Vec<_>>();
        if row_filters.is_empty() {
            return None;
        }

        // Filters that need the same column set share one decode stage. Other
        // sets stay separate so Parquet can apply the row selection between them.
        let mut groups: Vec<(Vec<ColumnId>, Vec<&Arc<dyn RuntimeFilter>>)> = Vec::new();
        for filter in row_filters {
            let mut columns = filter.required_columns().to_vec();
            columns.sort_unstable();
            columns.dedup();
            if let Some((_, group)) = groups.iter_mut().find(|(existing, _)| *existing == columns) {
                group.push(filter);
            } else {
                groups.push((columns, vec![filter]));
            }
        }

        let count = groups.len();
        groups
            .into_iter()
            .enumerate()
            .map(|(index, (_, filters))| {
                let role = match (index == 0, index + 1 == count) {
                    (true, true) => RuntimeFilterStatisticsRole::BOTH,
                    (true, false) => RuntimeFilterStatisticsRole::FIRST,
                    (false, true) => RuntimeFilterStatisticsRole::LAST,
                    (false, false) => RuntimeFilterStatisticsRole::NONE,
                };
                self.bind_filters(schema, &filters, role)
            })
            .collect()
    }

    fn bind_filters(
        &self,
        schema: &VeinSchema,
        row_filters: &[&Arc<dyn RuntimeFilter>],
        statistics_role: RuntimeFilterStatisticsRole,
    ) -> Option<RuntimeFilterBinding> {
        if row_filters.is_empty() {
            return None;
        }
        let mut column_ids = Vec::new();
        for filter in row_filters {
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
            statistics: self.statistics.clone(),
            statistics_role,
        })
    }

    /// Number of source-filter rows presented to the runtime filters.
    pub fn rows_seen(&self) -> u64 {
        self.statistics
            .as_ref()
            .map_or(0, |statistics| statistics.rows_seen.value())
    }

    /// Number of rows retained by all filters in the bound predicate.
    pub fn rows_kept(&self) -> u64 {
        self.statistics
            .as_ref()
            .map_or(0, |statistics| statistics.rows_kept.value())
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
    statistics: Option<RuntimeFilterCounters>,
    statistics_role: RuntimeFilterStatisticsRole,
}

impl RuntimeFilterBinding {
    /// Narrow scan columns needed to evaluate the bound filters.
    pub fn columns(&self) -> &[ResolvedColumnRef] {
        &self.columns
    }

    /// Stable diagnostic names for the filters in this decode stage.
    pub fn filter_names(&self) -> Vec<&'static str> {
        self.filters.iter().map(|filter| filter.filter.name()).collect()
    }

    /// True only when every evaluator in this bound stage explicitly permits
    /// evaluation in a different order.
    pub fn reorder_safe(&self) -> bool {
        self.filters.iter().all(|filter| filter.filter.reorder_safe())
    }

    /// Evaluate all filters and return one combined selection mask.
    ///
    /// Only the boolean mask is newly allocated. The input payload arrays are
    /// borrowed through Arrow's `RecordBatch` and are never gathered or copied.
    pub fn evaluate(&self, batch: &RecordBatch) -> Result<BooleanArray> {
        if let [bound] = self.filters.as_slice() {
            let mask = bound.filter.evaluate(batch, &bound.positions)?;
            if mask.len() != batch.num_rows() {
                return Err(VeinError::internal(
                    "runtime filter returned a mask with a different row count",
                ));
            }
            if let Some(statistics) = &self.statistics {
                let rows_kept = self.statistics_role.records_output.then(|| {
                    (0..mask.len())
                        .filter(|&row| mask.is_valid(row) && mask.value(row))
                        .count() as u64
                });
                self.record_statistics(statistics, batch.num_rows(), rows_kept);
            }
            return Ok(mask);
        }

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
        if let Some(statistics) = &self.statistics {
            self.record_statistics(statistics, batch.num_rows(), Some(rows_kept));
        }
        Ok(output.finish())
    }

    fn record_statistics(
        &self,
        statistics: &RuntimeFilterCounters,
        rows_seen: usize,
        rows_kept: Option<u64>,
    ) {
        if self.statistics_role.records_input {
            statistics.rows_seen.add(rows_seen as u64);
        }
        if self.statistics_role.records_output {
            statistics.rows_kept.add(rows_kept.unwrap_or_default());
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct RuntimeFilterStatisticsRole {
    records_input: bool,
    records_output: bool,
}

impl RuntimeFilterStatisticsRole {
    const NONE: Self = Self { records_input: false, records_output: false };
    const FIRST: Self = Self { records_input: true, records_output: false };
    const LAST: Self = Self { records_input: false, records_output: true };
    const BOTH: Self = Self { records_input: true, records_output: true };
}

struct BoundRuntimeFilter {
    filter: Arc<dyn RuntimeFilter>,
    positions: Vec<usize>,
}

#[derive(Clone)]
struct RuntimeFilterCounters {
    rows_seen: OperatorCounter,
    rows_kept: OperatorCounter,
}

#[cfg(test)]
mod tests;
