//! Adapts compiled Vein and runtime predicates to Parquet's late materializer.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;

use arrow::array::{Array, BooleanArray};
use arrow::datatypes::Schema;
use arrow::error::ArrowError;
use common::protocol::{ScanPredicate, ScanPredicateStage};
use common::Result;
use error::VeinError;
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::{
    ArrowPredicate, ArrowPredicateFn, PredicateOrderPolicy, RowFilter,
};
use parquet::schema::types::SchemaDescriptor;
use types::{ResolvedColumnRef, VeinSchema};

use crate::pipeline::DataChunk;
use crate::runtime_filter::RuntimeFilterSet;
use crate::udf::FunctionRegistry;
use crate::vm::VectorVmExecutor;

use super::super::metrics::ScanPredicateMetrics;
use super::super::projection::root_indices;
use super::super::selection::project_batch;
use super::adaptive_order::{AdaptiveFilterConfig, AdaptiveFilterOrder};

pub type PredicateStageMetrics =
(usize, usize, Vec<String>, Arc<ScanPredicateMetrics>);

pub struct ScanPredicateContext<'a> {
    pub arrow_schema: &'a Schema,
    pub parquet_schema: &'a SchemaDescriptor,
    pub path: &'a str,
    pub projected: &'a VeinSchema,
    pub pushdown: Option<&'a ScanPredicate>,
    pub registry: &'a Arc<FunctionRegistry>,
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

pub fn build_scan_predicates(
    context: ScanPredicateContext<'_>,
) -> Result<ScanPredicatePipeline> {
    let ScanPredicateContext {
        arrow_schema, parquet_schema, path, projected, pushdown, registry,
        detailed_metrics_enabled, predicate_metrics, runtime_filters, adaptive_order,
    } = context;
    let mut predicates: Vec<Box<dyn ArrowPredicate>> = Vec::new();
    let mut stage_metrics = Vec::new();
    let mut reorder_safe = true;

    if let Some(pushdown) = pushdown {
        for (index, stage) in pushdown.stages.iter().enumerate() {
            reorder_safe &= stage.reorder_safe;
            let metrics = Arc::new(ScanPredicateMetrics::default());
            stage_metrics.push((index, stage.predicate_count,
                                stage.input.fields.iter().map(|field| field.name.clone()).collect(),
                                Arc::clone(&metrics)));
            predicates.push(vm_predicate(
                arrow_schema, parquet_schema, &stage.input.fields, stage, path,
                Arc::clone(registry), detailed_metrics_enabled,
                Arc::clone(predicate_metrics), metrics, index == 0,
            )?);
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
            let (mask, positions) = predicate_projection(
                arrow_schema, parquet_schema, binding.columns(), path,
            )?;
            let record_source_rows = predicates.is_empty() && index == 0;
            let metrics = Arc::clone(predicate_metrics);
            let first_filter = Arc::new(AtomicBool::new(record_source_rows));
            let record_first_filter = Arc::clone(&first_filter);
            predicates.push(Box::new(
                ArrowPredicateFn::new(mask, move |batch| {
                    if record_first_filter.load(Ordering::Relaxed) {
                        metrics.record_source_rows(batch.num_rows());
                    }
                    let narrow = project_batch(&batch, &positions).map_err(as_arrow_error)?;
                    binding.evaluate(&narrow).map_err(as_arrow_error)
                })
                    .with_first_filter_callback(move |first| {
                        first_filter.store(first, Ordering::Relaxed);
                    }),
            ));
            tracing::debug!(target: "vein.parquet_runtime_filter",
                event = "scan_runtime_filter_stage_bound", path = %path,
                filters = ?stage_filter_names, stage_index = index,
                stage_count = filter_count, key_columns,
                "runtime filter installed as an independent Parquet row-filter stage");
        }
        tracing::info!(target: "vein.parquet_runtime_filter",
            event = "scan_runtime_filters_bound", path = %path,
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

fn vm_predicate(
    arrow_schema: &Schema,
    parquet_schema: &SchemaDescriptor,
    fields: &[ResolvedColumnRef],
    stage: &ScanPredicateStage,
    path: &str,
    registry: Arc<FunctionRegistry>,
    detailed: bool,
    total_metrics: Arc<ScanPredicateMetrics>,
    stage_metrics: Arc<ScanPredicateMetrics>,
    record_source_rows: bool,
) -> Result<Box<dyn ArrowPredicate>> {
    let (mask, positions) = predicate_projection(arrow_schema, parquet_schema, fields, path)?;
    let mut evaluator = VectorVmExecutor::for_evaluation(Arc::clone(&stage.program), registry);
    let result = stage.result;
    let first_filter = Arc::new(AtomicBool::new(record_source_rows));
    let record_first_filter = Arc::clone(&first_filter);
    Ok(Box::new(ArrowPredicateFn::new(mask, move |batch| {
        if record_first_filter.load(Ordering::Relaxed) {
            total_metrics.record_source_rows(batch.num_rows());
        }
        let array_bytes = detailed.then(|| positions.iter().fold(0usize, |bytes, &position| {
            bytes.saturating_add(batch.column(position).get_array_memory_size())
        })).unwrap_or_default();
        let started = detailed.then(Instant::now);
        let arrays = positions.iter().map(|&position| Arc::clone(batch.column(position))).collect();
        let mut chunk = DataChunk::from_vectors(arrays);
        let value = evaluator.evaluate(&mut chunk, result).map_err(as_arrow_error)?;
        let predicate = value.as_any().downcast_ref::<BooleanArray>().ok_or_else(|| {
            ArrowError::ComputeError("scan predicate did not return a boolean array".into())
        })?;
        if predicate.len() != batch.num_rows() {
            return Err(ArrowError::ComputeError("scan predicate returned a mask with the wrong row count".into()));
        }
        if let Some(started) = started {
            let rows_kept = (0..predicate.len())
                .filter(|&row| predicate.is_valid(row) && predicate.value(row)).count();
            let elapsed = started.elapsed();
            total_metrics.record_batch(batch.num_rows(), rows_kept, elapsed, array_bytes);
            stage_metrics.record_batch(batch.num_rows(), rows_kept, elapsed, array_bytes);
        }
        Ok(predicate.clone())
    }).with_first_filter_callback(move |first| {
        first_filter.store(first, Ordering::Relaxed);
    })))
}

fn predicate_projection(
    arrow_schema: &Schema,
    parquet_schema: &SchemaDescriptor,
    fields: &[ResolvedColumnRef],
    path: &str,
) -> Result<(ProjectionMask, Vec<usize>)> {
    let field_roots = fields.iter().map(|field| {
        root_indices(arrow_schema, std::slice::from_ref(field), path).map(|roots| roots[0])
    }).collect::<Result<Vec<_>>>()?;
    let mut roots = field_roots.clone();
    roots.sort_unstable();
    roots.dedup();
    let positions = field_roots.iter().map(|root| roots.binary_search(root)
        .map_err(|_| VeinError::internal("predicate field is absent from its Parquet projection")))
        .collect::<Result<Vec<_>>>()?;
    Ok((ProjectionMask::roots(parquet_schema, roots), positions))
}

fn as_arrow_error(error: impl std::fmt::Display) -> ArrowError {
    ArrowError::ComputeError(error.to_string())
}

#[cfg(test)]
#[path = "row_filter_tests.rs"]
mod tests;
#[cfg(test)]
#[path = "worker_policy_tests.rs"]
mod worker_policy_tests;
