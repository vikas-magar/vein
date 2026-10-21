//! Installs staged row predicates in the Parquet late-materialization reader.

use std::sync::Arc;

use common::Result;
use exec::operators::source::scan::ScanRequest;

use super::pruning::ParquetScanBuilder;
use crate::scan::parquet::metrics::ScanPredicateMetrics;
use crate::scan::parquet::predicate::{
    AdaptiveFilterOrder, PredicateStageMetrics, ScanPredicateContext, build_scan_predicates,
};

/// Predicate execution metadata needed by scan diagnostics.
pub(super) struct PredicateSetup {
    pub(super) stage_metrics: Vec<PredicateStageMetrics>,
    pub(super) has_row_filter: bool,
}

/// Builds predicate stages and attaches them before stream construction.
pub(super) fn install(
    builder: ParquetScanBuilder,
    request: &ScanRequest<'_>,
    path: &str,
    detailed_metrics_enabled: bool,
    predicate_metrics: &Arc<ScanPredicateMetrics>,
    adaptive_order: &mut Option<Arc<AdaptiveFilterOrder>>,
) -> Result<(ParquetScanBuilder, PredicateSetup)> {
    let mut builder = builder;
    let mut pipeline = build_scan_predicates(ScanPredicateContext {
        arrow_schema: builder.schema(),
        parquet_schema: builder.parquet_schema(),
        path,
        projected: request.projected,
        pushdown: request.pushdown,
        registry: request.registry,
        expression_kernels: request.expression_kernels,
        detailed_metrics_enabled,
        predicate_metrics,
        runtime_filters: request.runtime_filters,
        adaptive_order: adaptive_order.clone(),
    })?;
    *adaptive_order = pipeline.adaptive_order.clone();
    let stage_metrics = pipeline.stage_metrics.clone();
    let has_row_filter = pipeline.has_row_filter;
    if let Some(row_filter) = pipeline.row_filter.take() {
        builder = builder.with_row_filter(row_filter);
    }
    Ok((
        builder,
        PredicateSetup {
            stage_metrics,
            has_row_filter,
        },
    ))
}
