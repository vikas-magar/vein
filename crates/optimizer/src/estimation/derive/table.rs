use crate::context::{StatisticsFreshness, StatisticsProvider};
use super::super::{Estimate, EstimateConfidence, EstimateSource};
use super::super::columns::{ColumnEstimate, RelationEstimate};

pub(super) fn table_estimate<'a, P: StatisticsProvider + ?Sized>(
    table: &'a plan::TableMetadata, schema: &plan::VeinSchema, provider: &'a P,
) -> RelationEstimate<'a> {
    let Some(snapshot) = provider.table_statistics(table) else {
        return RelationEstimate::new(Estimate::unknown(EstimateSource::MissingStatistics));
    };
    tracing::debug!(target: "vein.optimizer", event = "optimizer_statistics_snapshot",
        table = %table.name, freshness = ?snapshot.freshness,
        rows = ?snapshot.statistics.num_rows,
        column_statistics = snapshot.statistics.column_statistics.len(),
        "optimizer_statistics_snapshot");
    if snapshot.freshness == StatisticsFreshness::Stale {
        let Some(rows) = snapshot.statistics.population_rows() else {
            return RelationEstimate::new(Estimate::unknown(EstimateSource::StaleStatistics));
        };
        let mut estimate = RelationEstimate::new(Estimate::model(
            rows as f64,
            EstimateSource::StaleStatistics,
        ));
        for column in &schema.fields {
            estimate.add_cardinality_prior(column.id, rows);
        }
        tracing::warn!(target: "vein.optimizer", event = "optimizer_stale_row_count_prior",
            table = %table.name, rows, confidence = "model",
            "using stale population size as a low-confidence planning prior; stale column synopses ignored");
        return estimate;
    }
    statistics_estimate(snapshot.statistics, schema, |column| column.name.clone(), EstimateSource::Statistics)
}

/// Shared population/column validation for source metadata and completed
/// intermediates. The key strategy preserves lineage without cloning synopses.
pub(super) fn statistics_estimate<'a>(
    statistics: &'a plan::TableStatistics,
    schema: &plan::VeinSchema,
    key: impl Fn(&plan::ResolvedColumn) -> String,
    source: EstimateSource,
) -> RelationEstimate<'a> {
    let Some(rows) = statistics.population_rows() else {
        return RelationEstimate::new(Estimate::unknown(EstimateSource::MissingStatistics));
    };
    if statistics.num_rows.is_none() {
        tracing::info!(
            target: "vein.optimizer",
            event = "optimizer_population_rows_derived_from_units",
            rows,
            file_count = ?statistics.file_count,
            row_group_count = ?statistics.row_group_count,
            "using complete data-unit row counts for table cardinality"
        );
    }
    let mut input = RelationEstimate::new(Estimate::known(rows as f64,
                                                          source, EstimateConfidence::Statistics));
    for column in &schema.fields {
        if let Some(stats) = statistics.column_statistics.get(&key(column)) {
            match ColumnEstimate::from_statistics(stats, rows, column.nullable) {
                Ok(summary) => { input.columns.insert(column.id, summary); }
                Err(reason) => {
                    tracing::warn!(target: "vein.optimizer", event = "optimizer_column_statistics_rejected",
                        table_column = %column.name, reason = ?reason, rows,
                        "ignoring invalid column synopsis and retaining population cardinality");
                    input.add_cardinality_prior(column.id, rows);
                }
            }
        } else {
            input.add_cardinality_prior(column.id, rows);
        }
    }
    for group in &statistics.column_groups {
        let Some(distinct) = group.distinct_count.filter(|count| {
            *count <= rows && (*count > 0 || rows == 0)
        }) else {
            continue;
        };
        if group.columns.is_empty()
            || group.distinct_count_relative_error.is_some_and(|error| {
            !error.is_finite() || !(0.0..=1.0).contains(&error)
        })
        {
            continue;
        }
        let mut ids = Vec::with_capacity(group.columns.len());
        for name in &group.columns {
            let Some(column) = schema.fields.iter().find(|column| key(column) == *name) else {
                ids.clear();
                break;
            };
            ids.push(column.id);
        }
        if ids.len() != group.columns.len() {
            continue;
        }
        ids.sort_unstable();
        ids.dedup();
        if ids.len() != group.columns.len() {
            continue;
        }
        input.joint_distinct.insert(
            ids,
            Estimate::known(
                distinct as f64,
                EstimateSource::Statistics,
                EstimateConfidence::Statistics,
            ),
        );
    }
    input
}

#[cfg(test)]
#[path = "table/tests.rs"]
mod tests;
