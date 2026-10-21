//! Statistics predicates and source-schema bindings for runtime filters.

use std::sync::Arc;

use catalog::StatisticsPredicate;
use types::{ColumnId, VeinSchema};

use super::super::contract::RuntimeFilter;
use super::super::policy::RuntimeFilterPruningDiagnostic;
use super::evaluation::{RuntimeFilterBinding, RuntimeFilterStatisticsRole};
use super::RuntimeFilterSet;

struct FilterGroup<'a> {
    columns: Vec<ColumnId>,
    filters: Vec<&'a Arc<dyn RuntimeFilter>>,
}

impl RuntimeFilterSet {
    /// Collect safe format-neutral predicates for source-level metadata
    /// pruning. These predicates are additional necessary conditions and are
    /// conjoined by the storage adapter with any static scan predicates.
    pub fn statistics_predicates(&self, schema: &VeinSchema) -> Vec<StatisticsPredicate> {
        self.collect_statistics_predicates(schema, |_| {})
    }

    /// Collect metadata predicates and a bounded per-filter capability summary
    /// in one pass, so adapters can explain which filters reach metadata pruning.
    pub fn statistics_predicates_with_diagnostics(
        &self,
        schema: &VeinSchema,
    ) -> (Vec<StatisticsPredicate>, Vec<RuntimeFilterPruningDiagnostic>) {
        let mut diagnostics = Vec::with_capacity(self.filters.len());
        let predicates = self.collect_statistics_predicates(schema, |diagnostic| {
            diagnostics.push(diagnostic);
        });
        (predicates, diagnostics)
    }

    fn collect_statistics_predicates(
        &self,
        schema: &VeinSchema,
        mut observe: impl FnMut(RuntimeFilterPruningDiagnostic),
    ) -> Vec<StatisticsPredicate> {
        let mut predicates = Vec::new();
        for filter in self.filters.iter() {
            let filter_predicates = filter.statistics_predicates(schema);
            observe(RuntimeFilterPruningDiagnostic {
                name: filter.name(),
                row_evaluation: filter.supports_row_evaluation(),
                metadata_predicate_count: filter_predicates.len(),
            });
            predicates.extend(filter_predicates);
        }
        predicates
    }

    /// Names of filters that can reject rows after decoding their required
    /// columns. Metadata-only filters are intentionally omitted.
    pub fn row_filter_names(&self) -> Vec<&'static str> {
        self.filters
            .iter()
            .filter(|filter| filter.supports_row_evaluation())
            .map(|filter| filter.name())
            .collect()
    }

    /// Bind the set to a scan schema. `None` means at least one required column
    /// is absent, so a scan must retain its ordinary exact behavior.
    pub fn bind(&self, schema: &VeinSchema) -> Option<RuntimeFilterBinding> {
        let row_filters = self
            .filters
            .iter()
            .filter(|filter| filter.supports_row_evaluation())
            .collect::<Vec<_>>();
        self.bind_filters(schema, &row_filters, RuntimeFilterStatisticsRole::BOTH)
    }

    /// Bind each row filter independently so a scan reader can apply Arrow's
    /// row selection between filters and avoid reading later filter columns for
    /// rows rejected by earlier filters.
    pub fn bind_stages(&self, schema: &VeinSchema) -> Option<Vec<RuntimeFilterBinding>> {
        let row_filters = self
            .filters
            .iter()
            .filter(|filter| filter.supports_row_evaluation())
            .collect::<Vec<_>>();
        if row_filters.is_empty() {
            return None;
        }

        // Filters that need the same column set share one decode stage. Other
        // sets stay separate so capable readers can apply row selection between
        // stages.
        let mut groups: Vec<FilterGroup<'_>> = Vec::new();
        for filter in row_filters {
            let mut columns = filter.required_columns().to_vec();
            columns.sort_unstable();
            columns.dedup();
            if let Some(group) = groups.iter_mut().find(|group| group.columns == columns) {
                group.filters.push(filter);
            } else {
                groups.push(FilterGroup { columns, filters: vec![filter] });
            }
        }

        let count = groups.len();
        groups
            .into_iter()
            .enumerate()
            .map(|(index, group)| {
                let role = match (index == 0, index + 1 == count) {
                    (true, true) => RuntimeFilterStatisticsRole::BOTH,
                    (true, false) => RuntimeFilterStatisticsRole::FIRST,
                    (false, true) => RuntimeFilterStatisticsRole::LAST,
                    (false, false) => RuntimeFilterStatisticsRole::NONE,
                };
                self.bind_filters(schema, &group.filters, role)
            })
            .collect()
    }
}
