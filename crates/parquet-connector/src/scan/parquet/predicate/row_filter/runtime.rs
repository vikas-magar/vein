//! Runtime membership predicate with optional per-stage decode diagnostics.
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use common::time::Instant;

use arrow::array::Array;
use common::Result;
use exec::runtime::filter::RuntimeFilterBinding;
use parquet::arrow::arrow_reader::{ArrowPredicate, ArrowPredicateFn};

use super::{ScanPredicateContext, expression::{as_arrow_error, predicate_projection}};
use super::super::super::metrics::ScanPredicateMetrics;

pub(super) fn predicate(
    context: &ScanPredicateContext<'_>,
    binding: RuntimeFilterBinding,
    stage_metrics: Arc<ScanPredicateMetrics>,
    first: bool,
) -> Result<Box<dyn ArrowPredicate>> {
    let (mask, positions) = predicate_projection(
        context.arrow_schema, context.parquet_schema, binding.columns(), context.path,
    )?;
    let total_metrics = Arc::clone(context.predicate_metrics);
    let detailed = context.detailed_metrics_enabled;
    let first_filter = Arc::new(AtomicBool::new(first));
    let record_first = Arc::clone(&first_filter);
    Ok(Box::new(ArrowPredicateFn::new(mask, move |batch| {
        if record_first.load(Ordering::Relaxed) {
            total_metrics.record_source_rows(batch.num_rows());
        }
        let started = detailed.then(Instant::now);
        let view = exec::pipeline::arrow_adapter::from_record_batch_ref(&batch)
            .and_then(|view| view.project(&positions)).map_err(as_arrow_error)?;
        let predicate = binding.evaluate(&Arc::new(view)).map_err(as_arrow_error)?;
        if let Some(started) = started {
            let rows_kept = (0..predicate.len())
                .filter(|&row| predicate.is_valid(row) && predicate.value(row)).count();
            let bytes = batch.columns().iter().fold(0usize, |total, array| {
                total.saturating_add(array.get_array_memory_size())
            });
            stage_metrics.record_batch(batch.num_rows(), rows_kept, started.elapsed(), bytes);
        }
        Ok(predicate)
    }).with_first_filter_callback(move |first| {
        first_filter.store(first, Ordering::Relaxed);
    })))
}
