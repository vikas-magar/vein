use std::collections::HashSet;
use std::sync::Arc;

use common::statistics::{QuerySourceStatistics, QueryStatisticsContext};
use plan::{LogicalPlan, PlanTraversal};

/// Builds one query-scoped statistics context from all source nodes in a plan.
pub fn query_statistics_context(plan: &LogicalPlan) -> QueryStatisticsContext {
    let mut sources = Vec::new();
    let mut seen = HashSet::new();
    collect_sources(plan, &mut sources, &mut seen);
    QueryStatisticsContext::new(sources)
}

fn collect_sources(
    plan: &LogicalPlan,
    sources: &mut Vec<QuerySourceStatistics>,
    seen: &mut HashSet<String>,
) {
    match plan {
        LogicalPlan::Scan { table, .. } => register_source(table, sources, seen),
        LogicalPlan::ExternalScan {
            source_metadata: Some(table),
            ..
        } => register_source(table, sources, seen),
        _ => {}
    }

    for child in plan.children() {
        collect_sources(child, sources, seen);
    }
    for subquery in plan.subquery_plans() {
        collect_sources(subquery, sources, seen);
    }
}

fn register_source(
    table: &plan::TableMetadata,
    sources: &mut Vec<QuerySourceStatistics>,
    seen: &mut HashSet<String>,
) {
    let source_key = table
        .catalog_snapshot
        .as_ref()
        .map(common::statistics::runtime::table_statistics_key)
        .unwrap_or_else(|| table.name.clone());
    if seen.insert(source_key.clone()) {
        sources.push(QuerySourceStatistics {
            source_key,
            table_name: table.name.clone(),
            statistics: table.statistics.clone().map(Arc::new),
        });
    }
}
