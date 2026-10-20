//! Parquet dictionary-page predicate extraction and row-group pruning.

use std::collections::HashMap;
use std::io::Cursor;
use std::sync::Arc;

use bytes::Bytes;
use catalog::StatisticsPredicate;
use parquet::arrow::arrow_reader::RowGroupSelection;
use parquet::basic::Encoding;
use parquet::column::page::{Page, PageReader};
use parquet::file::metadata::{ColumnChunkMetaData, ParquetMetaData};
use parquet::file::reader::{ChunkReader, Length, SerializedPageReader};
use types::LogicalLiteral;

use super::bloom::BloomFilterColumn;
use super::metadata::{nonnegative, row_group_unit};

mod decoder;
mod values;

/// One equality or same-column `IN` condition eligible for dictionary checks.
///
/// `index` preserves the position in the extracted predicate list so a scan
/// can combine results from multiple dictionary columns without reordering
/// conjuncts.
#[derive(Debug, Clone, PartialEq)]
pub struct DictionaryPredicate {
    /// Position in the dictionary predicate list.
    pub index: usize,
    /// Source schema column containing the dictionary values.
    pub column: String,
    /// Equality alternatives. Any one value may satisfy this condition.
    pub candidates: Vec<LogicalLiteral>,
}

/// Dictionary evidence used to select row groups before data pages are decoded.
#[derive(Debug, Default)]
pub struct DictionaryPruning {
    /// Row groups retained, including their existing page-index selections.
    pub selections: Vec<RowGroupSelection>,
    /// Whole row groups excluded by dictionary evidence.
    pub row_groups_skipped: u64,
    /// Rows in excluded row groups.
    pub rows_skipped: u64,
    /// Compressed bytes in excluded row groups when known.
    pub estimated_compressed_bytes_skipped: u64,
    /// Dictionary predicate checks with a supported decoded result.
    pub probes: u64,
}

/// Extracts equality and same-column `IN` conditions from a conjunction.
///
/// Other predicate shapes are intentionally ignored. In particular, a mixed
/// `OR` cannot be used as a dictionary proof unless every branch is an equality
/// against the same column.
pub fn dictionary_predicates(predicates: &[StatisticsPredicate]) -> Vec<DictionaryPredicate> {
    fn collect(predicate: &StatisticsPredicate, output: &mut Vec<DictionaryPredicate>) {
        match predicate {
            StatisticsPredicate::And(children) => {
                for child in children {
                    collect(child, output);
                }
            }
            StatisticsPredicate::Comparison {
                column,
                comparison: catalog::StatisticsComparison::Equal,
                literal,
            } => output.push(DictionaryPredicate {
                index: output.len(),
                column: column.clone(),
                candidates: vec![literal.clone()],
            }),
            StatisticsPredicate::Or(children) => {
                let mut column: Option<&str> = None;
                let mut candidates = Vec::with_capacity(children.len());
                for child in children {
                    let StatisticsPredicate::Comparison {
                        column: child_column,
                        comparison: catalog::StatisticsComparison::Equal,
                        literal,
                    } = child
                    else {
                        return;
                    };
                    if column.is_some_and(|column| column != child_column) {
                        return;
                    }
                    column = Some(child_column);
                    candidates.push(literal.clone());
                }
                if let Some(column) = column {
                    output.push(DictionaryPredicate {
                        index: output.len(),
                        column: column.to_string(),
                        candidates,
                    });
                }
            }
            _ => {}
        }
    }

    let mut output = Vec::new();
    for predicate in predicates {
        collect(predicate, &mut output);
    }
    output
}

/// Returns whether every data page in this chunk is dictionary encoded.
///
/// Missing encoding statistics and mixed encodings are not sufficient proof;
/// callers must retain those row groups.
pub fn is_fully_dictionary_encoded(column: &ColumnChunkMetaData) -> bool {
    column.dictionary_page_offset().is_some()
        && column.page_encoding_stats_mask().is_some_and(|mask| {
        mask.is_only(Encoding::PLAIN_DICTIONARY) || mask.is_only(Encoding::RLE_DICTIONARY)
    })
}

/// Gets the serialized dictionary-page range when its offsets are complete and
/// contained by the known file length.
///
/// This only locates the dictionary payload. Callers may use its membership
/// result to prune dictionary-encoded pages in a mixed chunk, but must require
/// [`is_fully_dictionary_encoded`] before pruning a whole row group.
pub fn dictionary_page_range(
    column: &ColumnChunkMetaData,
    file_length: u64,
) -> Option<std::ops::Range<u64>> {
    let start = u64::try_from(column.dictionary_page_offset()?).ok()?;
    let end = u64::try_from(column.data_page_offset()).ok()?;
    (start < end && end <= file_length).then_some(start..end)
}

/// Checks candidate literals against a serialized dictionary page.
///
/// Each result is `Some(true)` when the dictionary contains a candidate,
/// `Some(false)` when the supported dictionary proves none are present, and
/// `None` when the page, physical type, or literal conversion is unsupported.
/// `Bytes` slices are retained through Parquet page decoding; this examines only
/// the small dictionary and does not materialize row batches or column arrays.
pub fn dictionary_page_predicate_matches(
    page_bytes: Bytes,
    file_length: u64,
    row_count: usize,
    chunk: &ColumnChunkMetaData,
    column: &BloomFilterColumn,
    predicates: &[DictionaryPredicate],
) -> Vec<Option<bool>> {
    if predicates.is_empty() {
        return vec![None; predicates.len()];
    }
    let Some(range) = dictionary_page_range(chunk, file_length) else {
        return vec![None; predicates.len()];
    };
    let base_offset = range.start;
    let reader = DictionaryPageChunkReader {
        bytes: page_bytes,
        base_offset,
        file_length,
    };
    let Ok(mut pages) = SerializedPageReader::new(Arc::new(reader), chunk, row_count, None) else {
        return vec![None; predicates.len()];
    };
    let Ok(Some(Page::DictionaryPage {
                    buf,
                    num_values,
                    encoding: Encoding::PLAIN,
                    ..
                })) = pages.get_next_page()
    else {
        return vec![None; predicates.len()];
    };
    let Ok(num_values) = usize::try_from(num_values) else {
        return vec![None; predicates.len()];
    };

    values::decode_values(column, buf, num_values, predicates)
}

struct DictionaryPageChunkReader {
    bytes: Bytes,
    base_offset: u64,
    file_length: u64,
}

impl Length for DictionaryPageChunkReader {
    fn len(&self) -> u64 {
        self.file_length
    }
}

impl ChunkReader for DictionaryPageChunkReader {
    type T = Cursor<Bytes>;

    fn get_read(&self, start: u64) -> parquet::errors::Result<Self::T> {
        let position = start
            .checked_sub(self.base_offset)
            .filter(|position| *position <= self.bytes.len() as u64)
            .ok_or_else(|| parquet::errors::ParquetError::General("read outside dictionary page range".into()))?;
        let mut cursor = Cursor::new(self.bytes.clone());
        cursor.set_position(position);
        Ok(cursor)
    }

    fn get_bytes(&self, start: u64, length: usize) -> parquet::errors::Result<Bytes> {
        let start = start
            .checked_sub(self.base_offset)
            .and_then(|offset| usize::try_from(offset).ok())
            .ok_or_else(|| parquet::errors::ParquetError::General("read outside dictionary page range".into()))?;
        let end = start
            .checked_add(length)
            .filter(|end| *end <= self.bytes.len())
            .ok_or_else(|| parquet::errors::ParquetError::General("read outside dictionary page range".into()))?;
        Ok(self.bytes.slice(start..end))
    }
}

/// Applies dictionary predicate evidence to existing row-group selections.
/// Unknown/missing outcomes retain their selection and its page-index ranges.
pub fn prune_row_groups_with_dictionary(
    metadata: &ParquetMetaData,
    selections: Vec<RowGroupSelection>,
    membership: &HashMap<usize, Vec<Option<bool>>>,
) -> DictionaryPruning {
    let mut pruning = DictionaryPruning::default();
    for selection in selections {
        let row_group_index = selection.row_group_index();
        let outcomes = membership.get(&row_group_index);
        let mut proven_absent = false;
        if let Some(outcomes) = outcomes {
            for outcome in outcomes {
                if outcome.is_some() {
                    pruning.probes = pruning.probes.saturating_add(1);
                }
                proven_absent |= *outcome == Some(false);
            }
        }
        if !proven_absent {
            pruning.selections.push(selection);
            continue;
        }
        let Some(row_group) = metadata.row_groups().get(row_group_index) else {
            pruning.selections.push(selection);
            continue;
        };
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
