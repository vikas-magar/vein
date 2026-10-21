//! Build file and row-group statistics from a cached Parquet footer.

use arrow::datatypes::Schema;
use catalog::{DataUnitStatistics, StatisticsUnitKind};
use parquet::file::metadata::ParquetMetaData;

use super::metadata::{complete_sum, nonnegative, populate_column_statistics, row_group_unit};

/// Extracts one file node and all its row-group children from a cached footer.
///
/// The returned child indices are local to this vector; callers that append
/// several file trees must offset their `parent` indices by the destination
/// vector's starting length.
pub fn file_units(
    file_ordinal: usize,
    file_bytes: u64,
    arrow_schema: &Schema,
    metadata: &ParquetMetaData,
) -> Vec<DataUnitStatistics> {
    let row_groups = metadata.row_groups();
    let mut units = Vec::with_capacity(row_groups.len().saturating_add(1));
    let row_group_units = row_groups
        .iter()
        .enumerate()
        .map(|(ordinal, row_group)| row_group_unit(ordinal, row_group))
        .collect::<Vec<_>>();

    let row_group_rows = if row_group_units.is_empty() {
        Some(0)
    } else {
        complete_sum(row_group_units.iter().map(|unit| unit.num_rows))
    };
    let footer_rows = nonnegative(metadata.file_metadata().num_rows());
    let file_rows = footer_rows.filter(|rows| row_group_rows == Some(*rows));
    let mut file_unit = DataUnitStatistics::new(StatisticsUnitKind::File, file_ordinal);
    file_unit.num_rows = file_rows;
    file_unit.physical_bytes = Some(file_bytes);
    file_unit.compressed_bytes = complete_sum(
        row_group_units
            .iter()
            .map(|unit| unit.compressed_bytes),
    );
    file_unit.uncompressed_bytes = complete_sum(
        row_group_units
            .iter()
            .map(|unit| unit.uncompressed_bytes),
    );
    units.push(file_unit);

    for mut unit in row_group_units {
        unit.parent = Some(0);
        units.push(unit);
    }

    populate_column_statistics(arrow_schema, metadata, &mut units[1..], None);
    units
}