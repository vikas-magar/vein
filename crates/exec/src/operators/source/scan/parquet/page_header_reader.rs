//! Read and decode data-page headers for conservative page pruning.

use std::collections::HashMap;
use std::io::Cursor;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::Instant;

use arrow::datatypes::Schema;
use opendal::Operator;
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::file::metadata::ParquetMetaData;

use super::reader::{IoStats, ParquetFileReader, ParquetIoBackend};

const INITIAL_PAGE_HEADER_PROBE_BYTES: u64 = 32;
const MAX_PAGE_HEADER_PROBE_BYTES: u64 = 64 * 1024;

#[derive(Debug, Default)]
pub(super) struct PageHeaderReadMetrics {
    pub(super) columns_requested: u64,
    pub(super) column_row_group_pairs_requested: u64,
    pub(super) chunks_scanned: u64,
    pub(super) pages_scanned: u64,
    pub(super) pages_with_statistics: u64,
    pub(super) pages_with_complete_bounds: u64,
    pub(super) unsupported_row_mapping_chunks: u64,
    pub(super) header_bytes: u64,
    pub(super) probe_bytes: u64,
    pub(super) failed_chunks: u64,
    pub(super) elapsed_us: u64,
}

#[derive(Debug, Default)]
pub(super) struct PageHeaderReadResult {
    pub(super) pages: crate::parquet_statistics::PageHeaderStatistics,
    /// Complete data-page byte maps keyed by row group and Parquet leaf ordinal.
    /// They let the push decoder fetch the surviving pages without relying on
    /// a footer OffsetIndex.
    pub(super) page_locations: HeaderPageLocations,
    pub(super) metrics: PageHeaderReadMetrics,
}

pub(super) type HeaderPageLocations =
HashMap<usize, HashMap<usize, Vec<parquet::file::page_index::offset_index::PageLocation>>>;

/// Inputs needed to inspect data-page headers for selected predicate columns.
pub(super) struct PageHeaderReadRequest<'a> {
    pub(super) operator: &'a Operator,
    pub(super) path: &'a str,
    pub(super) absolute_path: &'a Path,
    pub(super) backend: ParquetIoBackend,
    pub(super) file_size_bytes: u64,
    pub(super) bytes_read: &'a Arc<AtomicU64>,
    pub(super) io: &'a Arc<IoStats>,
    pub(super) arrow_schema: &'a Schema,
    pub(super) metadata: &'a ParquetMetaData,
    pub(super) plan: &'a crate::parquet_statistics::PageHeaderReadPlan,
}

/// Reads data-page headers for predicate columns when page-index coverage is
/// missing or incomplete. Unsupported mappings or malformed chunks remain
/// conservative and are left for Arrow's normal row filter.
pub(super) async fn read_page_header_statistics(
    request: PageHeaderReadRequest<'_>,
) -> PageHeaderReadResult {
    let PageHeaderReadRequest {
        operator,
        path,
        absolute_path,
        backend,
        file_size_bytes,
        bytes_read,
        io,
        arrow_schema,
        metadata,
        plan,
    } = request;
    let started = Instant::now();
    let mut result = PageHeaderReadResult::default();
    let parquet_schema = metadata.file_metadata().schema_descr();
    let needed_columns = plan.column_names();
    let columns = needed_columns
        .iter()
        .filter_map(|name| {
            let (_, field) = arrow_schema.column_with_name(name)?;
            let converter = parquet::arrow::arrow_reader::statistics::StatisticsConverter::try_new(
                name,
                arrow_schema,
                parquet_schema,
            )
                .ok()?;
            let ordinal = converter.parquet_column_index()?;
            let descriptor = parquet_schema.column(ordinal);
            Some((
                (*name).to_string(),
                ordinal,
                field.data_type().clone(),
                descriptor.physical_type(),
                descriptor.max_rep_level(),
            ))
        })
        .collect::<Vec<_>>();
    result.metrics.columns_requested = u64::try_from(needed_columns.len()).unwrap_or(u64::MAX);
    result.metrics.column_row_group_pairs_requested =
        u64::try_from(plan.pair_count()).unwrap_or(u64::MAX);
    let mut reader = match ParquetFileReader::new(
        operator,
        path,
        absolute_path,
        backend,
        file_size_bytes,
        Arc::clone(bytes_read),
        Arc::clone(io),
    )
        .await
    {
        Ok(reader) => reader,
        Err(error) => {
            result.metrics.failed_chunks = u64::try_from(plan.pair_count()).unwrap_or(u64::MAX);
            tracing::debug!(
                target: "vein.audit",
                event = "parquet_page_header_reader_unavailable",
                path = %path,
                backend = backend.name(),
                error = %error,
                "page-header pruning unavailable; retaining all candidate rows"
            );
            result.metrics.elapsed_us = started.elapsed().as_micros() as u64;
            return result;
        }
    };

    for (row_group_index, requested_columns) in plan.requests() {
        let Some(row_group) = metadata.row_groups().get(*row_group_index) else {
            continue;
        };
        let Some(row_count) = usize::try_from(row_group.num_rows()).ok() else {
            continue;
        };
        let mut row_group_pages = HashMap::new();
        let mut row_group_page_locations = HashMap::new();
        for (name, column_ordinal, arrow_type, physical_type, max_rep_level) in &columns {
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
                    match read_page_header_at(&mut reader, offset, chunk_end, *physical_type).await
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
            result.pages.insert(*row_group_index, row_group_pages);
            result
                .page_locations
                .insert(*row_group_index, row_group_page_locations);
        }
    }

    result.metrics.elapsed_us = started.elapsed().as_micros() as u64;
    result
}

async fn read_page_header_at(
    reader: &mut ParquetFileReader,
    offset: u64,
    end: u64,
    physical_type: parquet::basic::Type,
) -> std::result::Result<(parquet::file::serialized_reader::PageHeaderInfo, u64), PageHeaderReadError>
{
    let available = end
        .checked_sub(offset)
        .filter(|available| *available > 0)
        .ok_or_else(|| PageHeaderReadError {
            message: "page header offset is outside the column chunk".to_string(),
            probe_bytes: 0,
        })?;
    let max_probe = available.min(MAX_PAGE_HEADER_PROBE_BYTES);
    let mut probe_size = INITIAL_PAGE_HEADER_PROBE_BYTES.min(max_probe);
    let mut probe_bytes_read = 0_u64;
    loop {
        let probe_end = offset
            .checked_add(probe_size)
            .ok_or_else(|| PageHeaderReadError {
                message: "page header probe range overflowed".to_string(),
                probe_bytes: probe_bytes_read,
            })?;
        let bytes =
            reader
                .get_bytes(offset..probe_end)
                .await
                .map_err(|error| PageHeaderReadError {
                    message: error.to_string(),
                    probe_bytes: probe_bytes_read,
                })?;
        probe_bytes_read =
            probe_bytes_read.saturating_add(u64::try_from(bytes.len()).unwrap_or(u64::MAX));
        let parse_error = match parquet::file::serialized_reader::read_page_header(
            &mut Cursor::new(bytes.as_ref()),
            physical_type,
        ) {
            Ok(header) if header.header_size > 0 && header.header_size <= bytes.len() => {
                return Ok((header, probe_bytes_read));
            }
            Ok(_) => "page header parser returned an invalid header length".to_string(),
            Err(error) => error.to_string(),
        };
        if probe_size >= max_probe {
            return Err(PageHeaderReadError {
                message: parse_error,
                probe_bytes: probe_bytes_read,
            });
        }
        probe_size = probe_size.saturating_mul(2).min(max_probe);
    }
}

#[derive(Debug)]
struct PageHeaderReadError {
    message: String,
    probe_bytes: u64,
}
