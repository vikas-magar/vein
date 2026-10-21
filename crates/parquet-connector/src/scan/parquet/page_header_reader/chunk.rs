use std::collections::{BTreeSet, HashMap};

use parquet::file::metadata::RowGroupMetaData;

use super::super::reader::ParquetFileReader;
use super::header::read_page_header_at;
use super::{PageHeaderReadResult, PredicateColumn};

pub(super) async fn read_requested_row_group(
    reader: &mut ParquetFileReader,
    path: &str,
    file_size_bytes: u64,
    row_group_index: usize,
    row_group: &RowGroupMetaData,
    requested_columns: &BTreeSet<String>,
    columns: &[PredicateColumn],
    result: &mut PageHeaderReadResult,
) {
    let Some(row_count) = usize::try_from(row_group.num_rows()).ok() else {
        return;
    };
    let mut row_group_pages = HashMap::new();
    let mut row_group_page_locations = HashMap::new();
    for (name, column_ordinal, arrow_type, physical_type, max_rep_level) in columns {
        if !requested_columns.contains(name) {
            continue;
        }
        // Page-header statistics do not provide the offset index's
        // authoritative first-row mapping for repeated fields. Their page
        // values may cover only part of a logical row, so fail open here.
        if *max_rep_level != 0 {
            result.metrics.unsupported_row_mapping_chunks = result
                .metrics
                .unsupported_row_mapping_chunks
                .saturating_add(1);
            continue;
        }
        let Some(column) = row_group.columns().get(*column_ordinal) else {
            result.metrics.failed_chunks = result.metrics.failed_chunks.saturating_add(1);
            continue;
        };
        let chunk_start = column
            .dictionary_page_offset()
            .unwrap_or_else(|| column.data_page_offset());
        let Some(chunk_start) = u64::try_from(chunk_start).ok() else {
            result.metrics.failed_chunks = result.metrics.failed_chunks.saturating_add(1);
            continue;
        };
        let Some(chunk_size) = u64::try_from(column.compressed_size()).ok() else {
            result.metrics.failed_chunks = result.metrics.failed_chunks.saturating_add(1);
            continue;
        };
        let Some(chunk_end) = chunk_start
            .checked_add(chunk_size)
            .filter(|end| *end <= file_size_bytes)
        else {
            result.metrics.failed_chunks = result.metrics.failed_chunks.saturating_add(1);
            continue;
        };
        let Some(mut offset) = u64::try_from(column.data_page_offset())
            .ok()
            .filter(|offset| *offset >= chunk_start && *offset < chunk_end)
        else {
            result.metrics.failed_chunks = result.metrics.failed_chunks.saturating_add(1);
            continue;
        };

        result.metrics.chunks_scanned = result.metrics.chunks_scanned.saturating_add(1);
        let mut first_row = 0usize;
        let mut page_ordinal = 0usize;
        // Every accepted page must cover at least one row, so a valid flat
        // column cannot contain more data pages than its row group.
        let max_page_count = row_count;
        let mut pages = Vec::new();
        let mut page_locations = Vec::new();
        let mut chunk_valid = true;
        while offset < chunk_end {
            if page_ordinal >= max_page_count {
                chunk_valid = false;
                break;
            }
            page_ordinal = page_ordinal.saturating_add(1);
            let (header, probe_bytes) =
                match read_page_header_at(&mut *reader, offset, chunk_end, *physical_type).await
                {
                    Ok(value) => value,
                    Err(error) => {
                        result.metrics.probe_bytes =
                            result.metrics.probe_bytes.saturating_add(error.probe_bytes);
                        tracing::debug!(
                                target: "vein.audit",
                                event = "parquet_page_header_unavailable",
                                path = %path,
                                row_group = row_group_index,
                                column = %name,
                                offset,
                                probe_bytes = error.probe_bytes,
                                error = %error.message,
                                "retaining page range because its header could not be parsed"
                            );
                        chunk_valid = false;
                        break;
                    }
                };
            result.metrics.pages_scanned = result.metrics.pages_scanned.saturating_add(1);
            result.metrics.probe_bytes = result.metrics.probe_bytes.saturating_add(probe_bytes);
            result.metrics.header_bytes = result
                .metrics
                .header_bytes
                .saturating_add(u64::try_from(header.header_size).unwrap_or(u64::MAX));
            let Some(body_offset) = offset.checked_add(header.header_size as u64) else {
                chunk_valid = false;
                break;
            };
            let Some(next_offset) = body_offset
                .checked_add(header.compressed_page_size as u64)
                .filter(|next| *next > offset && *next <= chunk_end)
            else {
                chunk_valid = false;
                break;
            };

            if let Some(page) = header.data_page {
                let page_rows = page
                    .num_rows
                    .or_else(|| (*max_rep_level == 0).then_some(page.num_values));
                let Some(page_rows) = page_rows.filter(|rows| *rows > 0) else {
                    chunk_valid = false;
                    break;
                };
                let Some(next_row) = first_row.checked_add(page_rows) else {
                    chunk_valid = false;
                    break;
                };
                if next_row > row_count {
                    chunk_valid = false;
                    break;
                }
                let Some(page_size) = next_offset.checked_sub(offset) else {
                    chunk_valid = false;
                    break;
                };
                let (Ok(page_offset), Ok(page_size), Ok(first_row_index)) = (
                    i64::try_from(offset),
                    i32::try_from(page_size),
                    i64::try_from(first_row),
                ) else {
                    chunk_valid = false;
                    break;
                };
                let statistics = crate::parquet_statistics::column_statistics_from_page_header(
                    page.statistics.as_ref(),
                    arrow_type,
                    *physical_type,
                );
                if page.statistics.is_some() {
                    result.metrics.pages_with_statistics =
                        result.metrics.pages_with_statistics.saturating_add(1);
                }
                if statistics.min_max_complete == Some(true) {
                    result.metrics.pages_with_complete_bounds =
                        result.metrics.pages_with_complete_bounds.saturating_add(1);
                }
                pages.push(crate::parquet_statistics::PageHeaderRowStatistics {
                    first_row,
                    row_count: page_rows,
                    encoding: page.encoding,
                    statistics,
                });
                page_locations.push(parquet::file::page_index::offset_index::PageLocation {
                    offset: page_offset,
                    compressed_page_size: page_size,
                    first_row_index,
                });
                first_row = next_row;
            }
            offset = next_offset;
        }

        if chunk_valid && offset == chunk_end && first_row == row_count && !pages.is_empty() {
            row_group_pages.insert(name.clone(), pages);
            row_group_page_locations.insert(*column_ordinal, page_locations);
        } else {
            result.metrics.failed_chunks = result.metrics.failed_chunks.saturating_add(1);
        }
    }
    if !row_group_pages.is_empty() {
        result.pages.insert(row_group_index, row_group_pages);
        result
            .page_locations
            .insert(row_group_index, row_group_page_locations);
    }
}
