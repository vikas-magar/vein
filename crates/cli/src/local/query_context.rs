use std::collections::HashSet;
use std::sync::Arc;

use common::statistics::{QuerySourceStatistics, QueryStatisticsContext};
use plan::{LogicalPlan, PlanTraversal};

pub(super) fn query_statistics_context(plan: &LogicalPlan) -> QueryStatisticsContext {
    fn collect(plan: &LogicalPlan, sources: &mut Vec<QuerySourceStatistics>, seen: &mut HashSet<String>) {
        fn register_source(
            table: &plan::TableMetadata,
            sources: &mut Vec<QuerySourceStatistics>,
            seen: &mut HashSet<String>,
        ) {
            let source_key = table.catalog_snapshot.as_ref()
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

        match plan {
            LogicalPlan::Scan { table, .. } => register_source(table, sources, seen),
            LogicalPlan::ExternalScan { source_metadata: Some(table), .. } => register_source(table, sources, seen),
            _ => {}
        }
        for child in plan.children() { collect(child, sources, seen); }
        for subquery in plan.subquery_plans() { collect(subquery, sources, seen); }
    }

    let mut sources = Vec::new();
    collect(plan, &mut sources, &mut HashSet::new());
    QueryStatisticsContext::new(sources)
}
