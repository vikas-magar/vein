//! Zero-copy batch evaluation and per-filter execution feedback.

use std::sync::Arc;

use arrow::array::{Array, BooleanArray, builder::BooleanBuilder};
use common::Result;
use common::statistics::OperatorCounter;
use error::VeinError;
use types::ResolvedColumnRef;

use super::super::contract::RuntimeFilter;
use crate::pipeline::DataChunkView;

pub(super) const FILTER_ROWS_SEEN: &str = "runtime_filter_rows_seen";
pub(super) const FILTER_ROWS_KEPT: &str = "runtime_filter_rows_kept";

/// Scan-schema fields and positional key mappings for a runtime filter set.
pub struct RuntimeFilterBinding {
    pub(super) columns: Vec<ResolvedColumnRef>,
    pub(super) filters: Vec<BoundRuntimeFilter>,
    pub(super) statistics: Option<RuntimeFilterCounters>,
    pub(super) statistics_role: RuntimeFilterStatisticsRole,
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
    /// Only the boolean mask is newly allocated. The input payload arrays stay
    /// borrowed through the view's source mappings.
    pub fn evaluate(&self, view: &DataChunkView) -> Result<BooleanArray> {
        if let [bound] = self.filters.as_slice() {
            let mask = bound.filter.evaluate(view, &bound.positions)?;
            if mask.len() != view.num_rows() {
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
                self.record_statistics(statistics, view.num_rows(), rows_kept);
            }
            return Ok(mask);
        }

        let masks = self
            .filters
            .iter()
            .map(|bound| bound.filter.evaluate(view, &bound.positions))
            .collect::<Result<Vec<_>>>()?;
        if masks.iter().any(|mask| mask.len() != view.num_rows()) {
            return Err(VeinError::internal(
                "runtime filter returned a mask with a different row count",
            ));
        }

        let mut output = BooleanBuilder::with_capacity(view.num_rows());
        let mut rows_kept = 0u64;
        for row in 0..view.num_rows() {
            let keep = masks
                .iter()
                .all(|mask| mask.is_valid(row) && mask.value(row));
            output.append_value(keep);
            rows_kept += u64::from(keep);
        }
        if let Some(statistics) = &self.statistics {
            self.record_statistics(statistics, view.num_rows(), Some(rows_kept));
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
pub(super) struct RuntimeFilterStatisticsRole {
    records_input: bool,
    records_output: bool,
}

impl RuntimeFilterStatisticsRole {
    pub(super) const NONE: Self = Self { records_input: false, records_output: false };
    pub(super) const FIRST: Self = Self { records_input: true, records_output: false };
    pub(super) const LAST: Self = Self { records_input: false, records_output: true };
    pub(super) const BOTH: Self = Self { records_input: true, records_output: true };
}

pub(super) struct BoundRuntimeFilter {
    pub(super) filter: Arc<dyn RuntimeFilter>,
    pub(super) positions: Vec<usize>,
}

#[derive(Clone)]
pub(super) struct RuntimeFilterCounters {
    pub(super) rows_seen: OperatorCounter,
    pub(super) rows_kept: OperatorCounter,
}
