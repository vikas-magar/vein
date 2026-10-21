//! Position maps from source schemas to runtime filter evaluators.

use std::sync::Arc;

use types::{ResolvedColumnRef, VeinSchema};

use super::super::contract::RuntimeFilter;
use super::evaluation::{BoundRuntimeFilter, RuntimeFilterBinding, RuntimeFilterStatisticsRole};
use super::RuntimeFilterSet;

impl RuntimeFilterSet {
    pub(super) fn bind_filters(
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
        // Source adapters may require predicate columns in source order. Keep
        // each filter's key order in its positional mapping below, while
        // exposing the narrow input columns in the scan schema's order.
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
