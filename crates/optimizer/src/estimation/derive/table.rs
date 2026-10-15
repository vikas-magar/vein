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
        return RelationEstimate::new(Estimate::unknown(EstimateSource::StaleStatistics));
    }
    let Some(rows) = snapshot.statistics.num_rows else {
        return RelationEstimate::new(Estimate::unknown(EstimateSource::MissingStatistics));
    };
    let mut input = RelationEstimate::new(Estimate::known(rows as f64,
                                                          EstimateSource::Statistics, EstimateConfidence::Statistics));
    for column in &schema.fields {
        if let Some(stats) = snapshot.statistics.column_statistics.get(&column.name) {
            let Some(summary) = ColumnEstimate::from_statistics(stats, rows, column.nullable) else {
                return RelationEstimate::new(Estimate::unknown(EstimateSource::InvalidStatistics));
            };
            input.columns.insert(column.id, summary);
        }
    }
    for group in &snapshot.statistics.column_groups {
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
            let Some(column) = schema.fields.iter().find(|column| column.name == *name) else {
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
