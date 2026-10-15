//! Builds the staged Parquet row filters for scan pushdown and runtime joins.

use std::sync::Arc;
use std::time::Instant;

use arrow::array::BooleanArray;
use arrow::datatypes::Schema;
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use common::protocol::ScanPredicate;
use common::Result;
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::{ArrowPredicate, ArrowPredicateFn};
use parquet::schema::types::SchemaDescriptor;

use crate::pipeline::DataChunk;
use crate::runtime_filter::RuntimeFilterSet;
use crate::udf::FunctionRegistry;
use crate::vm::VectorVmExecutor;
use types::VeinSchema;

use super::metrics::{count_true_rows, ScanPredicateMetrics};
use super::projection::root_indices;

pub(super) type PredicateStageMetrics =
(usize, usize, Vec<String>, Arc<ScanPredicateMetrics>);

pub(super) struct ScanPredicateContext<'a> {
    pub(super) parquet_schema: &'a SchemaDescriptor,
    pub(super) arrow_schema: &'a Schema,
    pub(super) path: &'a str,
    pub(super) projected: &'a VeinSchema,
    pub(super) pushdown: Option<&'a ScanPredicate>,
    pub(super) registry: &'a Arc<FunctionRegistry>,
    pub(super) detailed_metrics_enabled: bool,
    pub(super) predicate_metrics: &'a Arc<ScanPredicateMetrics>,
    pub(super) runtime_filters: &'a RuntimeFilterSet,
}

pub(super) struct ScanPredicatePipeline {
    pub(super) predicates: Vec<Box<dyn ArrowPredicate>>,
    pub(super) stage_metrics: Vec<PredicateStageMetrics>,
}

/// Creates the ordered row filters for pushed expressions and runtime filters.
///
/// Each stage receives Arrow's surviving row selection from earlier stages and
/// decodes only the columns in that stage's projection mask.
pub(super) fn build_scan_predicates(
    context: ScanPredicateContext<'_>,
) -> Result<ScanPredicatePipeline> {
    let ScanPredicateContext {
        parquet_schema,
        arrow_schema,
        path,
        projected,
        pushdown,
        registry,
        detailed_metrics_enabled,
        predicate_metrics,
        runtime_filters,
    } = context;
    let mut predicates: Vec<Box<dyn ArrowPredicate>> =
        Vec::with_capacity(pushdown.map_or(0, |predicate| predicate.stages.len()) + 1);
    let mut stage_metrics = Vec::new();

    if let Some(pushdown) = pushdown {
        for (stage_index, stage) in pushdown.stages.iter().enumerate() {
            let mask = ProjectionMask::roots(
                parquet_schema,
                root_indices(arrow_schema, &stage.input.fields, path)?,
            );
            let mut evaluator = VectorVmExecutor::for_evaluation(
                Arc::clone(&stage.program),
                Arc::clone(registry),
            );
            let result = stage.result;
            let stage_predicate_count = stage.predicate_count;
            let predicate_metrics = Arc::clone(predicate_metrics);
            let metrics = Arc::new(ScanPredicateMetrics::default());
            stage_metrics.push((
                stage_index,
                stage_predicate_count,
                stage
                    .input
                    .fields
                    .iter()
                    .map(|field| field.name.clone())
                    .collect(),
                Arc::clone(&metrics),
            ));
            let predicate = ArrowPredicateFn::new(mask, move |batch: RecordBatch| {
                let rows_in = batch.num_rows();
                let input_array_bytes = if detailed_metrics_enabled {
                    batch
                        .columns()
                        .iter()
                        .map(|array| array.get_array_memory_size())
                        .sum()
                } else {
                    0
                };
                let eval_started = detailed_metrics_enabled.then(Instant::now);
                let mut chunk = DataChunk::from_record_batch(batch);
                let boolean = evaluator
                    .evaluate(&mut chunk, result)
                    .map_err(|error| ArrowError::ComputeError(error.to_string()))?
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .cloned()
                    .ok_or_else(|| {
                        ArrowError::ComputeError(
                            "scan predicate did not evaluate to a boolean column".to_string(),
                        )
                    })?;
                if let Some(eval_started) = eval_started {
                    let rows_kept = count_true_rows(&boolean);
                    let eval_wall = eval_started.elapsed();
                    predicate_metrics.record_batch(rows_in, rows_kept, eval_wall, input_array_bytes);
                    metrics.record_batch(rows_in, rows_kept, eval_wall, input_array_bytes);
                    tracing::trace!(
                        target: "vein.audit",
                        event = "scan_filter_batch",
                        stage_index,
                        stage_predicate_count,
                        rows_in,
                        rows_kept,
                        input_array_bytes,
                        eval_wall_us = eval_wall.as_micros() as u64,
                        "scan_filter_batch"
                    );
                }
                Ok(boolean)
            });
            predicates.push(Box::new(predicate));
        }
    }

    if let Some(binding) = runtime_filters.bind(projected) {
        let columns = binding.columns();
        let key_column_count = columns.len();
        let mask = ProjectionMask::roots(
            parquet_schema,
            root_indices(arrow_schema, columns, path)?,
        );
        let predicate = ArrowPredicateFn::new(mask, move |batch: RecordBatch| {
            binding
                .evaluate(&batch)
                .map_err(|error| ArrowError::ComputeError(error.to_string()))
        });
        tracing::debug!(
            target: "vein.audit",
            event = "scan_runtime_filter_bound",
            path = %path,
            filter_names = ?runtime_filters.names(),
            key_columns = key_column_count,
            filter_bytes = runtime_filters.memory_bytes(),
            "scan runtime filter bound"
        );
        predicates.push(Box::new(predicate));
    } else if runtime_filters.has_row_filters() {
        tracing::debug!(
            target: "vein.audit",
            event = "scan_runtime_filter_skipped",
            path = %path,
            reason = "required_column_missing",
            filter_names = ?runtime_filters.names(),
            "scan runtime filter skipped"
        );
    }

    Ok(ScanPredicatePipeline {
        predicates,
        stage_metrics,
    })
}
