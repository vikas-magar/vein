//! Policy inputs and decisions for query-local runtime filters.

use std::sync::Arc;

use common::Result;
use common::statistics::QueryStatisticsSnapshot;

use super::contract::RuntimeFilter;

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
