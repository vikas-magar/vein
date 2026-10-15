use std::collections::HashSet;

use catalog::{AnalyzedTableStatistics, StatisticsSourceIdentity};
use common::statistics::schema_fingerprint;
use plan::{TableMetadata, VeinSchema};

/// Loads value-derived statistics only when the snapshot, schema, row count,
/// and all source object versions still match their analyzed inputs.
pub(super) fn apply_analyzed_statistics(
    metadata: &mut TableMetadata,
    manifest_revision: u64,
    analyzed: &AnalyzedTableStatistics,
    source_identities: &[StatisticsSourceIdentity],
) -> bool {
    let logical_schema_fingerprint = schema_fingerprint(&metadata.schema);
    let Some(table_statistics) = metadata.statistics.as_mut() else {
        return false;
    };
    let rejection = if analyzed.source_revision != manifest_revision {
        Some("catalog revision changed")
    } else if analyzed.schema_fingerprint != logical_schema_fingerprint {
        Some("logical schema changed")
    } else if analyzed.source_identities != source_identities {
        Some("source file version changed")
    } else if analyzed.statistics.num_rows != table_statistics.num_rows {
        Some("analyzed row count disagrees with storage metadata")
    } else if table_statistics.num_rows.is_some_and(|rows| {
        !column_groups_are_consistent(
            &metadata.schema,
            &analyzed.statistics.column_groups,
            rows,
        )
    }) {
        Some("analyzed multi-column statistics are inconsistent")
    } else {
        None
    };
    if let Some(reason) = rejection {
        tracing::debug!(
            target: "vein.audit",
            event = "catalog_statistics_rejected",
            table = %metadata.name,
            revision = manifest_revision,
            reason,
            "catalog_statistics_rejected"
        );
        return false;
    }

    let Some(rows) = table_statistics.num_rows else {
        return false;
    };
    if analyzed.statistics.column_statistics.len() != metadata.schema.fields.len() {
        tracing::debug!(
            target: "vein.audit",
            event = "catalog_statistics_rejected",
            table = %metadata.name,
            revision = manifest_revision,
            reason = "analyzed column set is incomplete",
            "catalog_statistics_rejected"
        );
        return false;
    }
    for field in &metadata.schema.fields {
        let Some(summary) = analyzed.statistics.column_statistics.get(&field.name) else {
            return false;
        };
        let Some(nulls) = summary.null_count else {
            return false;
        };
        if summary.nan_count.is_none()
            || summary.min_max_complete.is_none()
            || !summary.is_consistent_with_rows(rows)
        {
            tracing::debug!(
                target: "vein.audit",
                event = "catalog_statistics_rejected",
                table = %metadata.name,
                column = %field.name,
                revision = manifest_revision,
                reason = "analyzed column statistics are inconsistent",
                "catalog_statistics_rejected"
            );
            return false;
        }
        if table_statistics
            .column_statistics
            .get(&field.name)
            .and_then(|column| column.null_count)
            .is_some_and(|footer_nulls| footer_nulls != nulls)
        {
            tracing::debug!(
                target: "vein.audit",
                event = "catalog_statistics_rejected",
                table = %metadata.name,
                column = %field.name,
                revision = manifest_revision,
                reason = "footer and analyzed null counts disagree",
                "catalog_statistics_rejected"
            );
            return false;
        }
    }

    for (name, source) in &analyzed.statistics.column_statistics {
        let target = table_statistics
            .column_statistics
            .entry(name.clone())
            .or_default();
        target.null_count = target.null_count.or(source.null_count);
        target.nan_count = target.nan_count.or(source.nan_count);
        target.distinct_count = source.distinct_count;
        target.distinct_count_relative_error = source.distinct_count_relative_error;
        target.distinct_sketch.clone_from(&source.distinct_sketch);
        if source.min_max_complete == Some(true) {
            target.min_value.clone_from(&source.min_value);
            target.max_value.clone_from(&source.max_value);
            target.min_max_complete = Some(true);
        } else if source.min_max_complete == Some(false) {
            target.min_max_complete = Some(false);
        }
        target.numeric.clone_from(&source.numeric);
        target.byte_length.clone_from(&source.byte_length);
        target.most_common_values.clone_from(&source.most_common_values);
        target.histogram.clone_from(&source.histogram);
    }
    table_statistics
        .column_groups
        .clone_from(&analyzed.statistics.column_groups);
    tracing::info!(
        target: "vein.audit",
        event = "catalog_statistics_loaded",
        table = %metadata.name,
        revision = manifest_revision,
        row_count = rows,
        column_count = analyzed.statistics.column_statistics.len(),
        distinct_columns = analyzed.statistics.column_statistics.values()
            .filter(|column| column.distinct_count.is_some())
            .count(),
        bounded_columns = analyzed.statistics.column_statistics.values()
            .filter(|column| column.min_max_complete == Some(true))
            .count(),
        numeric_columns = analyzed.statistics.column_statistics.values()
            .filter(|column| column.numeric.is_some())
            .count(),
        variable_length_columns = analyzed.statistics.column_statistics.values()
            .filter(|column| column.byte_length.is_some())
            .count(),
        "catalog_statistics_loaded"
    );
    true
}

fn column_groups_are_consistent(
    schema: &VeinSchema,
    groups: &[catalog::ColumnGroupStatistics],
    rows: u64,
) -> bool {
    let mut seen = HashSet::with_capacity(groups.len());
    for group in groups {
        if group.columns.len() < 2
            || group.distinct_count.is_none_or(|count| count > rows)
            || group.distinct_count_relative_error.is_none_or(|error| {
            !error.is_finite() || !(0.0..=1.0).contains(&error)
        })
            || group.distinct_sketch.is_none()
        {
            return false;
        }
        let Some(positions) = group
            .columns
            .iter()
            .map(|name| schema.fields.iter().position(|field| field.name == *name))
            .collect::<Option<Vec<_>>>()
        else {
            return false;
        };
        if positions.windows(2).any(|pair| pair[0] >= pair[1]) {
            return false;
        }
        if !seen.insert(group.columns.iter().map(String::as_str).collect::<Vec<_>>()) {
            return false;
        }
    }
    true
}
