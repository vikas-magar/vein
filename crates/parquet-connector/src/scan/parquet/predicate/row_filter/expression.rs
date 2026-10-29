//! Compiles bound expressions into Parquet row-filter stages.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use common::time::Instant;

use arrow::array::Array;
use arrow::datatypes::Schema;
use arrow::error::ArrowError;
use common::Result;
use common::protocol::ScanPredicateStage;
use error::VeinError;
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::{ArrowPredicate, ArrowPredicateFn};
use parquet::schema::types::SchemaDescriptor;
use types::ResolvedColumnRef;

use exec::expression::eval::{ExpressionEvaluator, ExpressionKernelProvider};
use functions::FunctionRegistry;

use super::super::super::metrics::ScanPredicateMetrics;
use super::super::super::projection::root_indices;

pub(super) struct ExpressionPredicateRequest<'a> {
    pub(super) arrow_schema: &'a Schema,
    pub(super) parquet_schema: &'a SchemaDescriptor,
    pub(super) fields: &'a [ResolvedColumnRef],
    pub(super) stage: &'a ScanPredicateStage,
    pub(super) path: &'a str,
    pub(super) registry: Arc<FunctionRegistry>,
    pub(super) detailed: bool,
    pub(super) expression_kernels: Arc<dyn ExpressionKernelProvider>,
    pub(super) total_metrics: Arc<ScanPredicateMetrics>,
    pub(super) stage_metrics: Arc<ScanPredicateMetrics>,
    pub(super) record_source_rows: bool,
}

pub(super) fn expression_predicate(
    request: ExpressionPredicateRequest<'_>,
) -> Result<Box<dyn ArrowPredicate>> {
    let ExpressionPredicateRequest {
        arrow_schema,
        parquet_schema,
        fields,
        stage,
        path,
        registry,
        detailed,
        expression_kernels,
        total_metrics,
        stage_metrics,
        record_source_rows,
    } = request;
    let (mask, positions) = predicate_projection(arrow_schema, parquet_schema, fields, path)?;
    let expression = stage.predicate.clone();
    let first_filter = Arc::new(AtomicBool::new(record_source_rows));
    let record_first_filter = Arc::clone(&first_filter);
    Ok(Box::new(
        ArrowPredicateFn::new(mask, move |batch| {
            if record_first_filter.load(Ordering::Relaxed) {
                total_metrics.record_source_rows(batch.num_rows());
            }
            let array_bytes = if detailed {
                positions.iter().fold(0usize, |bytes, &position| {
                    bytes.saturating_add(batch.column(position).get_array_memory_size())
                })
            } else {
                0
            };
            let started = detailed.then(Instant::now);
            let view = exec::pipeline::arrow_adapter::from_record_batch_ref(&batch)
                .and_then(|view| view.project(&positions))
                .map_err(as_arrow_error)?;
            let view = Arc::new(view);
            let evaluator =
                ExpressionEvaluator::new(expression_kernels.as_ref(), registry.as_ref());
            let predicate = evaluator
                .evaluate_boolean(&expression, &view)
                .map_err(as_arrow_error)?;
            if let Some(started) = started {
                let rows_kept = (0..predicate.len())
                    .filter(|&row| predicate.is_valid(row) && predicate.value(row))
                    .count();
                let elapsed = started.elapsed();
                total_metrics.record_batch(batch.num_rows(), rows_kept, elapsed, array_bytes);
                stage_metrics.record_batch(batch.num_rows(), rows_kept, elapsed, array_bytes);
            }
            Ok(predicate)
        })
            .with_first_filter_callback(move |first| {
                first_filter.store(first, Ordering::Relaxed);
            }),
    ))
}

pub(super) fn predicate_projection(
    arrow_schema: &Schema,
    parquet_schema: &SchemaDescriptor,
    fields: &[ResolvedColumnRef],
    path: &str,
) -> Result<(ProjectionMask, Vec<usize>)> {
    let field_roots = fields
        .iter()
        .map(|field| {
            root_indices(arrow_schema, std::slice::from_ref(field), path).map(|roots| roots[0])
        })
        .collect::<Result<Vec<_>>>()?;
    let mut roots = field_roots.clone();
    roots.sort_unstable();
    roots.dedup();
    let positions = field_roots
        .iter()
        .map(|root| {
            roots.binary_search(root).map_err(|_| {
                VeinError::internal("predicate field is absent from its Parquet projection")
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok((ProjectionMask::roots(parquet_schema, roots), positions))
}

pub(super) fn as_arrow_error(error: impl std::fmt::Display) -> ArrowError {
    ArrowError::ComputeError(error.to_string())
}
