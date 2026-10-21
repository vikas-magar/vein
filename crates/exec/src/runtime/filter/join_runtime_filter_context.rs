//! Snapshot the available probe cardinality and query feedback at a join checkpoint.

use super::super::{DataSource, ExecutionTrace, QueryMemoryTracker};
use super::RuntimeFilterDecisionContext;

pub(super) struct JoinRuntimeFilterContext {
    pub(super) context: RuntimeFilterDecisionContext,
    pub(super) effective_probe_rows: Option<u64>,
    pub(super) probe_rows_source: &'static str,
    pub(super) source_statistics_count: usize,
    pub(super) runtime_scan_observation_count: usize,
    pub(super) runtime_operators_with_output: usize,
    pub(super) runtime_operator_output_rows: u64,
    pub(super) runtime_operator_output_batches: u64,
    pub(super) runtime_source_bytes_observed: u64,
}

impl JoinRuntimeFilterContext {
    pub(super) fn collect(
        source: &DataSource,
        build_rows: u64,
        build_keys: usize,
        memory: &QueryMemoryTracker,
        trace: &ExecutionTrace,
    ) -> Self {
        let probe_rows_estimate = match source {
            DataSource::File(scan) => scan
                .runtime_statistics
                .as_ref()
                .and_then(|statistics| statistics.expected_rows),
            _ => None,
        };
        let probe_source_key = match source {
            DataSource::File(scan) => scan
                .catalog_snapshot
                .as_ref()
                .map(common::statistics::runtime::table_statistics_key),
            _ => None,
        };
        let query_statistics = trace.query_statistics_context().snapshot();
        let source_statistics_count = query_statistics.sources.len();
        let runtime_scan_observation_count =
            query_statistics.scans.values().map(Vec::len).sum::<usize>();
        let runtime_operators_with_output = query_statistics
            .operators
            .iter()
            .filter(|operator| operator.output_batches > 0)
            .count();
        let runtime_operator_output_rows = query_statistics
            .operators
            .iter()
            .map(|operator| operator.output_rows)
            .sum::<u64>();
        let runtime_operator_output_batches = query_statistics
            .operators
            .iter()
            .map(|operator| operator.output_batches)
            .sum::<u64>();
        let runtime_source_bytes_observed = query_statistics
            .operators
            .iter()
            .map(|operator| operator.source_bytes_read)
            .sum::<u64>();
        let context = RuntimeFilterDecisionContext {
            build_rows,
            build_keys,
            probe_rows_estimate,
            probe_source_key,
            available_memory_bytes: memory.limit().saturating_sub(memory.used()),
            query_statistics,
        };
        let (effective_probe_rows, probe_rows_source) = context.probe_rows();
        Self {
            context,
            effective_probe_rows,
            probe_rows_source,
            source_statistics_count,
            runtime_scan_observation_count,
            runtime_operators_with_output,
            runtime_operator_output_rows,
            runtime_operator_output_batches,
            runtime_source_bytes_observed,
        }
    }
}
