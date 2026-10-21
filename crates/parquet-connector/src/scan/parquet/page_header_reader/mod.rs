use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use arrow::datatypes::{DataType, Schema};
use parquet::file::metadata::ParquetMetaData;
use storage::ReadStorage;

use super::reader::{IoStats, ParquetFileReader, ParquetIoBackend};

mod chunk;
mod header;
mod scan;

pub(super) use scan::read_page_header_statistics;

pub(super) type PredicateColumn = (String, usize, DataType, parquet::basic::Type, i16);

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

/// Per-row-group inputs for page-header inspection.
pub(super) struct RowGroupPageReadRequest<'a> {
    pub(super) reader: &'a mut ParquetFileReader,
    pub(super) path: &'a str,
    pub(super) file_size_bytes: u64,
    pub(super) row_group_index: usize,
    pub(super) row_group: &'a parquet::file::metadata::RowGroupMetaData,
    pub(super) requested_columns: &'a std::collections::BTreeSet<String>,
    pub(super) columns: &'a [PredicateColumn],
}

/// Inputs needed to inspect data-page headers for selected predicate columns.
pub(super) struct PageHeaderReadRequest<'a> {
    pub(super) storage: &'a Arc<dyn ReadStorage>,
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
