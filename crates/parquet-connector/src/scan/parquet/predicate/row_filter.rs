//! Adapts compiled Vein and runtime predicates to Parquet's late materializer.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use arrow::datatypes::Schema;
use common::Result;
use common::protocol::ScanPredicate;
use parquet::arrow::arrow_reader::{
    ArrowPredicate, ArrowPredicateFn, PredicateOrderPolicy, RowFilter,
};
use parquet::schema::types::SchemaDescriptor;
use types::VeinSchema;

use exec::expression::eval::ExpressionKernelProvider;
use exec::runtime::filter::RuntimeFilterSet;
use functions::FunctionRegistry;

#[path = "row_filter/expression.rs"]
mod expression;
use expression::{
    ExpressionPredicateRequest, as_arrow_error, expression_predicate, predicate_projection,
};

use super::super::metrics::ScanPredicateMetrics;
use super::adaptive_order::{AdaptiveFilterConfig, AdaptiveFilterOrder};

pub type PredicateStageMetrics = (usize, usize, Vec<String>, Arc<ScanPredicateMetrics>);

pub struct ScanPredicateContext<'a> {
    pub arrow_schema: &'a Schema,
    pub parquet_schema: &'a SchemaDescriptor,
    pub path: &'a str,
    pub projected: &'a VeinSchema,
    pub pushdown: Option<&'a ScanPredicate>,
    pub registry: &'a Arc<FunctionRegistry>,
    pub expression_kernels: &'a Arc<dyn ExpressionKernelProvider>,
    pub detailed_metrics_enabled: bool,
    pub predicate_metrics: &'a Arc<ScanPredicateMetrics>,
    pub runtime_filters: &'a RuntimeFilterSet,
    /// Adaptive state owned by this scan worker and reused across its files.
    pub adaptive_order: Option<Arc<AdaptiveFilterOrder>>,
}

pub struct ScanPredicatePipeline {
    pub row_filter: Option<RowFilter>,
    pub stage_metrics: Vec<PredicateStageMetrics>,
    pub has_row_filter: bool,
    pub adaptive_order: Option<Arc<AdaptiveFilterOrder>>,
}

pub fn build_scan_predicates(context: ScanPredicateContext<'_>) -> Result<ScanPredicatePipeline> {
    let ScanPredicateContext {
        arrow_schema,
        parquet_schema,
        path,
        projected,
        pushdown,
        registry,
        detailed_metrics_enabled,
        predicate_metrics,
        runtime_filters,
        adaptive_order,
        expression_kernels,
    } = context;
    let mut predicates: Vec<Box<dyn ArrowPredicate>> = Vec::new();
    let mut stage_metrics = Vec::new();
    let mut reorder_safe = true;

    if let Some(pushdown) = pushdown {
        for (index, stage) in pushdown.stages.iter().enumerate() {
            reorder_safe &= stage.reorder_safe;
            let metrics = Arc::new(ScanPredicateMetrics::default());
            stage_metrics.push((
                index,
                stage.predicate_count,
                stage
                    .input
                    .fields
                    .iter()
                    .map(|field| field.name.clone())
                    .collect(),
                Arc::clone(&metrics),
            ));
            predicates.push(expression_predicate(ExpressionPredicateRequest {
                arrow_schema,
                parquet_schema,
                fields: &stage.input.fields,
                stage,
                path,
                registry: Arc::clone(registry),
                detailed: detailed_metrics_enabled,
                expression_kernels: Arc::clone(expression_kernels),
                total_metrics: Arc::clone(predicate_metrics),
                stage_metrics: metrics,
                record_source_rows: index == 0,
            })?);
        }
    }

    if let Some(bindings) = runtime_filters.bind_stages(projected) {
        let filter_names = bindings
            .iter()
            .flat_map(|binding| binding.filter_names())
            .collect::<Vec<_>>();
        let filter_count = bindings.len();
        for (index, binding) in bindings.into_iter().enumerate() {
            reorder_safe &= binding.reorder_safe();
            let stage_filter_names = binding.filter_names();
            let key_columns = binding.columns().len();
            let (mask, positions) =
                predicate_projection(arrow_schema, parquet_schema, binding.columns(), path)?;
            let record_source_rows = predicates.is_empty() && index == 0;
            let metrics = Arc::clone(predicate_metrics);
            let first_filter = Arc::new(AtomicBool::new(record_source_rows));
            let record_first_filter = Arc::clone(&first_filter);
            predicates.push(Box::new(
                ArrowPredicateFn::new(mask, move |batch| {
                    if record_first_filter.load(Ordering::Relaxed) {
                        metrics.record_source_rows(batch.num_rows());
                    }
                    let view = exec::pipeline::arrow_adapter::from_record_batch_ref(&batch)
                        .and_then(|view| view.project(&positions))
                        .map_err(as_arrow_error)?;
                    binding.evaluate(&Arc::new(view)).map_err(as_arrow_error)
                })
                    .with_first_filter_callback(move |first| {
                        first_filter.store(first, Ordering::Relaxed);
                    }),
            ));
            tracing::debug!(target: "vein.parquet_runtime_filter",
                event = "scan_runtime_filter_stage_bound",
                source_id = %common::logging::LogLocation::from(path),
                filters = ?stage_filter_names, stage_index = index,
                stage_count = filter_count, key_columns,
                "runtime filter installed as an independent Parquet row-filter stage");
        }
        tracing::info!(target: "vein.parquet_runtime_filter",
            event = "scan_runtime_filters_bound",
            source_id = %common::logging::LogLocation::from(path),
            filter_names = ?filter_names, stage_count = filter_count,
            filter_bytes = runtime_filters.memory_bytes(),
            execution_stage = "ordered_parquet_row_filter_before_output_decode",
            participates_in_parquet_page_pruning = false,
            "runtime filters bound as separate scan predicates");
    }

    let has_row_filter = !predicates.is_empty();
    let stage_count = predicates.len();
    let adaptive_order = if !reorder_safe {
        tracing::info!(target: "vein.audit", event = "parquet_adaptive_filter_disabled",
            stage_count, reason = "predicate_not_proven_reorder_safe",
            "adaptive filter ordering disabled for an order-sensitive predicate");
        None
    } else if stage_count < 2 {
        if has_row_filter {
            tracing::debug!(target: "vein.audit", event = "parquet_adaptive_filter_disabled",
                stage_count, reason = "fewer_than_two_stages",
                "adaptive filter ordering requires at least two stages");
        }
        None
    } else {
        adaptive_order
            .filter(|policy| policy.stage_count() == stage_count)
            .or_else(|| {
                tracing::debug!(target: "vein.parquet_adaptive_filter",
                    event = "parquet_adaptive_filter_state_worker_local", stage_count,
                    "creating adaptive filter state for this scan worker");
                AdaptiveFilterOrder::new(stage_count, AdaptiveFilterConfig::default()).map(Arc::new)
            })
    };
    let row_filter = if has_row_filter {
        let row_filter = RowFilter::new(predicates);
        Some(match &adaptive_order {
            Some(policy) => {
                let policy: Arc<dyn PredicateOrderPolicy> = policy.clone();
                row_filter.with_order_policy(policy)
            }
            None => row_filter,
        })
    } else {
        None
    };
    Ok(ScanPredicatePipeline {
        row_filter,
        stage_metrics,
        has_row_filter,
        adaptive_order,
    })
}

#[cfg(test)]
#[path = "row_filter_tests.rs"]
mod tests;
#[cfg(test)]
#[path = "worker_policy_tests.rs"]
mod worker_policy_tests;
