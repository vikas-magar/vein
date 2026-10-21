//! Conservative rollups from file and row-group statistics.

use std::collections::HashMap;

use arrow::datatypes::Schema;
use catalog::{ColumnStatistics, DataUnitStatistics, StatisticsUnitKind, TableStatistics};
use types::LogicalLiteral;

/// Builds table-wide facts from file and row-group scopes.
///
/// Additive counts and byte sizes are published only when every contributing
/// unit has a known value and the checked sum succeeds. NDV is intentionally
/// omitted at table scope: summing row-group NDVs double-counts values shared
/// by groups, and Parquet does not provide the sketches needed to merge them.
pub fn table_statistics(
    data_units: Vec<DataUnitStatistics>,
    schema: &Schema,
) -> TableStatistics {
    let files = data_units
        .iter()
        .filter(|unit| unit.kind == StatisticsUnitKind::File && unit.parent.is_none())
        .collect::<Vec<_>>();
    let row_groups = data_units
        .iter()
        .filter(|unit| unit.kind == StatisticsUnitKind::RowGroup)
        .collect::<Vec<_>>();

    let mut column_statistics = HashMap::new();
    for field in schema.fields() {
        let summary = summarize_column(&row_groups, field.name(), field.is_nullable());
        if has_values(&summary) {
            column_statistics.insert(field.name().clone(), summary);
        }
    }

    TableStatistics {
        num_rows: complete_sum(files.iter().map(|unit| unit.num_rows)),
        total_bytes: complete_sum(files.iter().map(|unit| unit.physical_bytes)),
        file_count: u64::try_from(files.len()).ok(),
        row_group_count: u64::try_from(row_groups.len()).ok(),
        compressed_bytes: complete_sum(row_groups.iter().map(|unit| unit.compressed_bytes)),
        uncompressed_bytes: complete_sum(row_groups.iter().map(|unit| unit.uncompressed_bytes)),
        column_statistics,
        column_groups: Vec::new(),
        data_units,
    }
}

pub(super) fn has_values(statistics: &ColumnStatistics) -> bool {
    statistics.null_count.is_some()
        || statistics.distinct_count.is_some()
        || statistics.nan_count.is_some()
        || statistics.min_value.is_some()
        || statistics.max_value.is_some()
}

fn summarize_column(
    row_groups: &[&DataUnitStatistics],
    column_name: &str,
    nullable: bool,
) -> ColumnStatistics {
    if row_groups.is_empty() {
        return ColumnStatistics::default();
    }
    let mut null_count = Some(0_u64);
    let mut nan_count = Some(0_u64);
    let mut min_value: Option<LogicalLiteral> = None;
    let mut max_value: Option<LogicalLiteral> = None;
    let mut min_complete = true;
    let mut max_complete = true;
    let mut saw_non_null_values = false;
    let mut saw_nan_statistics = false;

    for unit in row_groups {
        let Some(rows) = unit.num_rows else {
            null_count = None;
            nan_count = None;
            min_complete = false;
            max_complete = false;
            continue;
        };
        if rows == 0 {
            continue;
        }
        let statistics = unit.column_statistics.get(column_name);
        let Some(statistics) = statistics else {
            null_count = None;
            nan_count = None;
            min_complete = false;
            max_complete = false;
            continue;
        };

        match statistics.null_count {
            Some(count) if count <= rows => {
                null_count = null_count.and_then(|total| total.checked_add(count));
            }
            _ => null_count = None,
        }
        if let Some(count) = statistics.nan_count {
            saw_nan_statistics = true;
            nan_count = nan_count.and_then(|total| total.checked_add(count));
        } else {
            nan_count = None;
        }
        let all_null = statistics.null_count == Some(rows);
        if all_null {
            continue;
        }
        saw_non_null_values = true;
        if let (Some(min), Some(max)) = (&statistics.min_value, &statistics.max_value)
            && min.try_cmp(max).is_none_or(|order| order.is_gt()) {
            min_complete = false;
            max_complete = false;
            continue;
        }
        match &statistics.min_value {
            Some(value) => match &min_value {
                Some(current) => match value.try_cmp(current) {
                    Some(std::cmp::Ordering::Less) => min_value = Some(value.clone()),
                    Some(_) => {}
                    None => min_complete = false,
                },
                None => min_value = Some(value.clone()),
            },
            None => min_complete = false,
        }
        match &statistics.max_value {
            Some(value) => match &max_value {
                Some(current) => match value.try_cmp(current) {
                    Some(std::cmp::Ordering::Greater) => max_value = Some(value.clone()),
                    Some(_) => {}
                    None => max_complete = false,
                },
                None => max_value = Some(value.clone()),
            },
            None => max_complete = false,
        }
    }

    ColumnStatistics {
        null_count: if nullable { null_count } else { Some(0) },
        // Distinct counts are unit-local. A mergeable sketch is required to
        // produce a table NDV when values overlap across units.
        distinct_count: None,
        nan_count: saw_nan_statistics.then_some(nan_count).flatten(),
        min_value: (min_complete && saw_non_null_values)
            .then_some(min_value)
            .flatten(),
        max_value: (max_complete && saw_non_null_values)
            .then_some(max_value)
            .flatten(),
        ..ColumnStatistics::default()
    }
}

pub(in crate::parquet_statistics) fn complete_sum(
    values: impl IntoIterator<Item=Option<u64>>,
) -> Option<u64> {
    let mut total = 0_u64;
    let mut saw_value = false;
    for value in values {
        total = total.checked_add(value?)?;
        saw_value = true;
    }
    saw_value.then_some(total)
}

pub(in crate::parquet_statistics) fn nonnegative(value: i64) -> Option<u64> {
    u64::try_from(value).ok()
}
