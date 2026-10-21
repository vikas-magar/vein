//! Per-row-group page-header requests for missing or incomplete page indexes.

use std::collections::{BTreeMap, BTreeSet, HashSet};

use catalog::StatisticsPredicate;

use super::page::PagePruning;

/// Predicate columns to inspect, grouped by row group.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct PageHeaderReadPlan {
    requests: BTreeMap<usize, BTreeSet<String>>,
}

impl PageHeaderReadPlan {
    /// Adds one row-group/column header request, deduplicating existing pairs.
    pub fn request(&mut self, row_group: usize, column: impl Into<String>) {
        self.requests.entry(row_group).or_default().insert(column.into());
    }

    /// Combines requests from another source, such as dictionary inspection.
    pub fn merge(&mut self, other: Self) {
        for (row_group, columns) in other.requests {
            self.requests.entry(row_group).or_default().extend(columns);
        }
    }

    /// Returns the deterministic set of row-group/column requests.
    pub fn requests(&self) -> &BTreeMap<usize, BTreeSet<String>> {
        &self.requests
    }

    /// Returns unique column names borrowed from this plan.
    pub fn column_names(&self) -> BTreeSet<&str> {
        self.requests
            .values()
            .flat_map(|columns| columns.iter().map(String::as_str))
            .collect()
    }

    /// Returns the number of distinct column names requested.
    pub fn column_count(&self) -> usize {
        self.column_names().len()
    }

    /// Returns the number of distinct row-group/column pairs requested.
    pub fn pair_count(&self) -> usize {
        self.requests
            .values()
            .fold(0usize, |count, columns| count.saturating_add(columns.len()))
    }

    /// Returns whether the plan contains no requests.
    pub fn is_empty(&self) -> bool {
        self.requests.is_empty()
    }
}

/// Plans header reads only for surviving row-group/column pairs not covered by
/// complete page-index bounds. Audit mode deliberately requests all pairs.
pub fn page_header_requests_with_audit(
    predicates: &[StatisticsPredicate],
    row_groups_to_probe: &[usize],
    page_index_available: bool,
    page_index_pruning: &PagePruning,
    audit_complete_page_index: bool,
) -> PageHeaderReadPlan {
    let mut plan = PageHeaderReadPlan::default();
    if predicates.is_empty() || row_groups_to_probe.is_empty() {
        return plan;
    }

    let mut referenced = HashSet::<&str>::new();
    for predicate in predicates {
        predicate.collect_columns(&mut referenced);
    }
    let mut columns = referenced.into_iter().collect::<Vec<_>>();
    columns.sort_unstable();

    for row_group in row_groups_to_probe {
        let complete = page_index_pruning
            .page_index_complete_columns
            .get(row_group);
        for column in &columns {
            let needs_header = audit_complete_page_index
                || !page_index_available
                || !complete.is_some_and(|covered| covered.contains(*column));
            if needs_header {
                plan.request(*row_group, *column);
            }
        }
    }
    plan
}

#[cfg(test)]
#[path = "page_header_plan_tests.rs"]
mod tests;
