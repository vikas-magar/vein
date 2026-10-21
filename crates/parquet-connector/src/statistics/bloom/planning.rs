use std::collections::{HashMap, HashSet};

use arrow::datatypes::Schema;
use catalog::StatisticsPredicate;
use parquet::arrow::arrow_reader::RowGroupSelection;
use parquet::arrow::arrow_reader::statistics::StatisticsConverter;
use parquet::file::metadata::ParquetMetaData;

use super::super::metadata::{nonnegative, row_group_unit};
use super::values::predicate_may_match_bloom;
use super::{BloomFilterColumn, BloomPruning, ParquetBloomFilter};

/// Resolves equality predicate columns to Parquet leaf ordinals.
///
/// Unsupported nested/schema mappings are omitted and therefore remain
/// conservative: they cannot cause a row group to be skipped.
pub fn bloom_filter_columns(
    arrow_schema: &Schema,
    metadata: &ParquetMetaData,
    predicates: &[StatisticsPredicate],
) -> Vec<BloomFilterColumn> {
    let names = equality_predicate_columns(predicates);

    let parquet_schema = metadata.file_metadata().schema_descr();
    names
        .into_iter()
        .filter_map(|name| {
            let (_, field) = arrow_schema.column_with_name(&name)?;
            let converter = StatisticsConverter::try_new(&name, arrow_schema, parquet_schema).ok()?;
            let parquet_column_index = converter.parquet_column_index()?;
            let descriptor = parquet_schema.column(parquet_column_index);
            Some(BloomFilterColumn {
                name,
                parquet_column_index,
                arrow_type: field.data_type().clone(),
                parquet_type: descriptor.physical_type(),
                type_length: descriptor.type_length(),
            })
        })
        .collect()
}

/// Returns the distinct columns with equality predicates in stable order.
/// Parquet column-chunk Bloom filters answer point-membership queries; range
/// predicates and row-level runtime Bloom filters are not equality candidates.
pub fn equality_predicate_columns(predicates: &[StatisticsPredicate]) -> Vec<String> {
    let mut names = HashSet::new();
    for predicate in predicates {
        collect_equality_columns(predicate, &mut names);
    }
    let mut names = names.into_iter().collect::<Vec<_>>();
    names.sort_unstable();
    names
}

/// Applies row-group column-chunk Bloom filters after exact footer and page
/// index pruning. A missing or unsupported filter is treated as “may match.”
pub fn prune_row_groups_with_bloom(
    metadata: &ParquetMetaData,
    predicates: &[StatisticsPredicate],
    selections: Vec<RowGroupSelection>,
    filters: &HashMap<usize, HashMap<String, ParquetBloomFilter>>,
) -> BloomPruning {
    let mut pruning = BloomPruning::default();
    for selection in selections {
        let row_group_index = selection.row_group_index();
        let Some(row_group_filters) = filters.get(&row_group_index) else {
            pruning.selections.push(selection);
            continue;
        };
        let mut probes = 0;
        let may_match = predicates
            .iter()
            .all(|predicate| predicate_may_match_bloom(predicate, row_group_filters, &mut probes));
        pruning.probes = pruning.probes.saturating_add(probes);
        if may_match {
            pruning.selections.push(selection);
            continue;
        }

        let row_group = metadata.row_group(row_group_index);
        pruning.row_groups_skipped = pruning.row_groups_skipped.saturating_add(1);
        pruning.rows_skipped = pruning
            .rows_skipped
            .saturating_add(nonnegative(row_group.num_rows()).unwrap_or(0));
        pruning.estimated_compressed_bytes_skipped = pruning
            .estimated_compressed_bytes_skipped
            .saturating_add(row_group_unit(row_group_index, row_group).compressed_bytes.unwrap_or(0));
    }
    pruning
}

fn collect_equality_columns(predicate: &StatisticsPredicate, names: &mut HashSet<String>) {
    match predicate {
        StatisticsPredicate::Comparison {
            column,
            comparison: catalog::StatisticsComparison::Equal,
            ..
        } => {
            names.insert(column.clone());
        }
        StatisticsPredicate::And(children) | StatisticsPredicate::Or(children) => {
            for child in children {
                collect_equality_columns(child, names);
            }
        }
        _ => {}
    }
}
