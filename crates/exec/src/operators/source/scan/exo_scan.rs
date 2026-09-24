//! Vectorized scan operator for Vein Dual-Plane (.exo) storage format.
//!
//! Implements two-phase execution:
//! 1. Address Plane & Tombstone Scan: reads `.exo.tomb` to filter deleted rows,
//!    generating a dense or sparse active row index set.
//! 2. Late Materialization: only dereferences and materializes values for
//!    projected columns and surviving row slots directly into Arrow arrays.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use arrow::array::{
    ArrayRef, BinaryBuilder, BooleanBuilder, Date32Builder, Date64Array, Decimal128Array, Decimal256Array,
    FixedSizeBinaryBuilder,
    Float32Builder, Float64Builder, Int8Builder, Int16Builder, Int32Builder, Int64Builder, StringBuilder,
    UInt8Builder,
    UInt16Builder, UInt32Builder, UInt64Builder, Int64Array,
};
use arrow::datatypes::{DataType, TimeUnit};
use common::protocol::FieldSchema;
use common::Result;
use error::VeinError;
use exo::traits::{AddressReader, VaultReader};
use exo::{
    f32_from_canonical_native, f64_from_canonical, i8_from_canonical_native,
    i16_from_canonical_native, i32_from_canonical_native, i64_from_canonical,
    AddressBatch, AddressFilter, AddressGranuleMark, AddressHeader, AddressPredicateEvaluator, ExoAddressReader,
    classify_type_name, ExoVaultReader, PartReader, VaultGranuleMark, VaultHeader,
};
use exo::addr::{ADDR_FORMAT_VERSION, ADDR_HEADER_MAGIC, ADDR_TRAILER_MAGIC, HEADER_SIZE, MARK_SIZE};
use exo::vault::{
    VAULT_FORMAT_VERSION, VAULT_HEADER_MAGIC, VAULT_TRAILER_MAGIC, HEADER_SIZE as VAULT_HEADER_SIZE,
    MARK_SIZE as VAULT_MARK_SIZE, TRAILER_SIZE as VAULT_TRAILER_SIZE,
};

use crate::pipeline::{DataChunk, PipelineNode};

/// Scanner abstraction over either a Payload Vault or an inlined Address Plane.
pub enum ColumnScanner {
    /// Reader for variable-length payloads stored in .exo.vault.
    Vault(ExoVaultReader),
    /// Reader for fixed-width inlined primitives stored in .exo.addr.
    Address(ExoAddressReader),
}

/// Default batch size for vectorized pipeline execution (rows per DataChunk).
pub const DEFAULT_EXO_VECTOR_SIZE: usize = 2048;

/// Counters collected during an EXO scan.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ExoScanStats {
    /// Number of partitions visited.
    pub partitions_scanned: u64,
    /// Number of address granules inspected.
    pub granules_considered: u64,
    /// Number of granules rejected by min/max or nullability metadata.
    pub granules_pruned: u64,
    /// Number of active rows examined by the scan.
    pub rows_examined: u64,
    /// Number of rows rejected by address predicates.
    pub rows_filtered: u64,
    /// Number of rows emitted downstream.
    pub rows_emitted: u64,
}

/// Executes a vectorized scan over a list of .exo partition paths.
#[tracing::instrument(skip_all)]
pub async fn execute_exo_scan(
    paths: &[String],
    projection: Option<&[FieldSchema]>,
    next: &mut dyn PipelineNode<DataChunk>,
) -> Result<()> {
    execute_exo_scan_with_filter(paths, projection, None, next).await
}

/// Executes an Exo scan with an optional address-native filter.
#[tracing::instrument(skip_all)]
pub async fn execute_exo_scan_with_filter(
    paths: &[String],
    projection: Option<&[FieldSchema]>,
    filter: Option<&[AddressFilter]>,
    next: &mut dyn PipelineNode<DataChunk>,
) -> Result<()> {
    let mut stats = ExoScanStats::default();
    execute_exo_scan_with_filter_and_stats(paths, projection, filter, next, &mut stats).await
}

/// Executes an Exo scan and writes detailed counters into `stats`.
#[tracing::instrument(skip_all)]
pub async fn execute_exo_scan_with_filter_and_stats(
    paths: &[String],
    projection: Option<&[FieldSchema]>,
    filter: Option<&[AddressFilter]>,
    next: &mut dyn PipelineNode<DataChunk>,
    stats: &mut ExoScanStats,
) -> Result<()> {
    for path_str in paths {
        // Existing local paths are scanned from their filesystem representation.
        // Non-local references are catalog/VFS references and must not fall through
        // to filesystem discovery when VFS resolution fails.
        let is_local_path = !path_str.contains("://")
            && tokio::fs::try_exists(path_str)
                .await
                .map_err(|error| {
                    VeinError::io(format!("Failed to inspect EXO source '{path_str}'"), error)
                })?;
        let is_vfs_reference = path_str.starts_with("vfs://") || !is_local_path;

        if is_vfs_reference {
            let store = vfs::VfsTableStore::shared_or_default()
                .map_err(|error| VeinError::internal(format!("open VFS EXO source: {error}")))?;
            let table_id = vfs::VfsTableStore::resolve_table_id(path_str);
            let explicit_part = vfs::VfsTableStore::extract_part_id_from_uri(path_str);
            let part_ids = match explicit_part {
                Some(pid) => vec![pid],
                None => store
                    .resolve_table_parts(table_id)
                    .await
                    .map_err(|error| {
                        VeinError::internal(format!(
                            "resolve VFS EXO table '{path_str}' partitions: {error}"
                        ))
                    })?,
            };

            if part_ids.is_empty() {
                return Err(VeinError::not_found(format!(
                    "VFS EXO table '{path_str}' has no visible partitions"
                )));
            }

            for part_id in part_ids {
                if scan_vfs_address_partition(
                    &store,
                    table_id,
                    part_id,
                    projection,
                    filter,
                    next,
                    stats,
                )
                .await?
                {
                    continue;
                }

                let part_reader = store
                    .open_partition_reader(table_id, part_id)
                    .await
                    .map_err(|error| {
                        VeinError::internal(format!(
                            "open VFS EXO table '{path_str}' partition {part_id}: {error}"
                        ))
                    })?;
                tracing::info!(
                    table_id = table_id,
                    part_id = part_id,
                    rows = part_reader.total_rows(),
                    "Scanning native VFS memory-mapped block partition"
                );
                scan_part_reader_with_filter_and_stats(
                    &part_reader,
                    projection,
                    filter,
                    next,
                    stats,
                )?;
            }
            continue;
        }

        // Local filesystem path scan for folders containing .exo.addr files.
        let path = Path::new(path_str);
        let base_dir = resolve_part_dir(path)?;

        // A table is a directory of partitions (part_000000, part_000001, ...)
        let mut part_dirs = Vec::new();
        if base_dir.is_dir() {
            if let Ok(entries) = std::fs::read_dir(&base_dir) {
                for entry in entries.flatten() {
                    let file_name = entry.file_name().to_string_lossy().to_string();
                    if file_name.starts_with("part_") && entry.path().is_dir() {
                        part_dirs.push(entry.path());
                    }
                }
            }
        }

        if part_dirs.is_empty() {
            part_dirs.push(base_dir);
        } else {
            part_dirs.sort_by_key(|p| extract_part_id(p));
        }

        for part_dir in part_dirs {
            let part_id = extract_part_id(&part_dir);

            tracing::info!(
                path = %path_str,
                part_dir = ?part_dir,
                part_id = part_id,
                "Scanning .exo columnar partition from filesystem"
            );

            let part_reader = PartReader::open(&part_dir, part_id)
                .map_err(|e| VeinError::internal(format!("Failed to open .exo part at {:?}: {}", part_dir, e)))?;

            scan_part_reader_with_filter_and_stats(&part_reader, projection, filter, next, stats)?;
        }
    }

    tracing::info!(
        target: "vein.audit",
        event = "exo_scan_completed",
        partitions_scanned = stats.partitions_scanned,
        granules_considered = stats.granules_considered,
        granules_pruned = stats.granules_pruned,
        rows_examined = stats.rows_examined,
        rows_filtered = stats.rows_filtered,
        rows_emitted = stats.rows_emitted,
        "Completed EXO scan"
    );
    Ok(())
}


enum VfsColumnIndex {
    Address {
        header: AddressHeader,
        marks: Vec<AddressGranuleMark>,
    },
    Vault {
        header: VaultHeader,
        marks: Vec<VaultGranuleMark>,
        address_header: AddressHeader,
        address_marks: Vec<AddressGranuleMark>,
    },
}

fn data_type_uses_vault(data_type: Option<&str>) -> bool {
    data_type
        .and_then(classify_type_name)
        .map(|class| class.uses_vault())
        .unwrap_or(false)
}

fn vfs_index_shape(index: &VfsColumnIndex) -> (u64, usize, usize) {
    match index {
        VfsColumnIndex::Address { header, marks } => {
            (header.total_rows, header.granule_size as usize, marks.len())
        }
        VfsColumnIndex::Vault { header, marks, .. } => {
            (header.total_rows, header.granule_size as usize, marks.len())
        }
    }
}

async fn scan_vfs_mixed_partition(
    store: &vfs::VfsTableStore,
    table_id: u32,
    part_id: u32,
    projection: Option<&[FieldSchema]>,
    filter: Option<&[AddressFilter]>,
    next: &mut dyn PipelineNode<DataChunk>,
    stats: &mut ExoScanStats,
) -> Result<bool> {
    let schema = store
        .read_table_schema(table_id)
        .await
        .map_err(|e| VeinError::internal(e.to_string()))?;
    let target_fields: Vec<(String, Option<String>)> = match projection {
        Some(fields) => fields
            .iter()
            .map(|field| (field.name.clone(), Some(field.data_type.clone())))
            .collect(),
        None => schema
            .fields
            .iter()
            .map(|field| (field.name.clone(), Some(format!("{:?}", field.data_type.to_arrow()))))
            .collect(),
    };
    if target_fields.is_empty() {
        return Ok(false);
    }

    let mut indexes = Vec::with_capacity(target_fields.len());
    for (column_name, data_type) in &target_fields {
        let has_vault = data_type_uses_vault(data_type.as_deref()) && store
            .partition_plane_exists(
                table_id,
                part_id,
                column_name,
                vfs::PlaneType::PayloadVault,
            )
            .await
            .map_err(|e| VeinError::internal(e.to_string()))?;
        let index = if has_vault {
            let (header, marks) = store
                .read_vault_index(table_id, part_id, column_name)
                .await
                .map_err(|e| VeinError::internal(e.to_string()))?;
            let (address_header, address_marks) = store
                .read_address_index(table_id, part_id, column_name)
                .await
                .map_err(|e| VeinError::internal(e.to_string()))?;
            VfsColumnIndex::Vault {
                header,
                marks,
                address_header,
                address_marks,
            }
        } else {
            let (header, marks) = store
                .read_address_index(table_id, part_id, column_name)
                .await
                .map_err(|e| VeinError::internal(e.to_string()))?;
            VfsColumnIndex::Address { header, marks }
        };
        indexes.push((column_name.clone(), data_type.clone(), index));
    }

    let mut predicate_indexes = Vec::new();
    if let Some(filters) = filter {
        for address_filter in filters {
            if predicate_indexes
                .iter()
                .any(|(column_name, _, _): &(String, AddressHeader, Vec<AddressGranuleMark>)| {
                    column_name == &address_filter.column
                })
            {
                continue;
            }
            let (header, marks) = store
                .read_address_index(table_id, part_id, &address_filter.column)
                .await
                .map_err(|e| VeinError::internal(e.to_string()))?;
            predicate_indexes.push((address_filter.column.clone(), header, marks));
        }
    }

    let tombstones = match store
        .read_partition_plane_all(
            table_id,
            part_id,
            "tombstones",
            vfs::PlaneType::Tombstone,
        )
        .await
    {
        Ok(bytes) if !bytes.is_empty() => exo::TombstoneMask::from_bytes(&bytes).unwrap_or_default(),
        _ => exo::TombstoneMask::new(),
    };

    let (total_rows, granule_size, total_granules) = vfs_index_shape(&indexes[0].2);
    for (_, _, index) in indexes.iter().skip(1) {
        let (rows, size, granules) = vfs_index_shape(index);
        if rows != total_rows || size != granule_size || granules != total_granules {
            return Err(VeinError::internal("VFS columns have inconsistent vault/address shapes"));
        }
    }
    stats.partitions_scanned += 1;
    stats.granules_considered += total_granules as u64;

    'mixed_granules: for granule_idx in 0..total_granules {
        if let Some(filters) = filter {
            for address_filter in filters {
                if let Some((_, _, marks)) = predicate_indexes
                    .iter()
                    .find(|(column_name, _, _)| column_name == &address_filter.column)
                {
                    if !address_filter.predicate.may_match(&marks[granule_idx]) {
                        stats.granules_pruned += 1;
                        continue 'mixed_granules;
                    }
                }
            }
        }

        let granule_start = granule_idx * granule_size;
        let row_count = ((total_rows as usize).saturating_sub(granule_start)).min(granule_size);
        let mut filtered_slots: Vec<u32> = (0..row_count as u32)
            .filter(|slot| {
                !tombstones.is_deleted(granule_start.saturating_add(*slot as usize) as u32)
            })
            .collect();
        stats.rows_examined += filtered_slots.len() as u64;

        if let Some(filters) = filter {
            for address_filter in filters {
                if let Some((_, header, marks)) = predicate_indexes
                    .iter()
                    .find(|(column_name, _, _)| column_name == &address_filter.column)
                {
                    let mark = &marks[granule_idx];
                    let bytes = store
                        .read_address_granule(table_id, part_id, &address_filter.column, mark)
                        .await
                        .map_err(|e| VeinError::internal(e.to_string()))?;
                    let reader_bytes = build_single_granule_reader_bytes(header, mark, &bytes, row_count)?;
                    let reader = ExoAddressReader::from_bytes(reader_bytes)
                        .map_err(|e| VeinError::internal(e.to_string()))?;
                    let mut scanner = ColumnScanner::Address(reader);
                    let before = filtered_slots.len();
                    filtered_slots =
                        filter_address_slots(&mut scanner, &filtered_slots, &address_filter.predicate)?;
                    stats.rows_filtered += (before - filtered_slots.len()) as u64;
                    if filtered_slots.is_empty() {
                        stats.granules_pruned += 1;
                        continue 'mixed_granules;
                    }
                }
            }
        }

        let mut vectors = Vec::with_capacity(indexes.len());
        let mut address_columns = Vec::with_capacity(indexes.len());
        for (column_name, data_type, index) in &indexes {
            let (mut scanner, address_batch) = match index {
                VfsColumnIndex::Address { header, marks } => {
                    let mark = &marks[granule_idx];
                    let bytes = store
                        .read_address_granule(table_id, part_id, column_name, mark)
                        .await
                        .map_err(|e| VeinError::internal(e.to_string()))?;
                    let reader_bytes = build_single_granule_reader_bytes(header, mark, &bytes, row_count)?;
                    let reader = ExoAddressReader::from_bytes(reader_bytes)
                        .map_err(|e| VeinError::internal(e.to_string()))?;
                    let address_batch = AddressBatch::from_reader(&reader, &filtered_slots)
                        .map_err(|e| VeinError::internal(format!("failed to read address sidecar: {e}")))?;
                    (ColumnScanner::Address(reader), Some(address_batch))
                }
                VfsColumnIndex::Vault {
                    header,
                    marks,
                    address_header,
                    address_marks,
                } => {
                    let mark = &marks[granule_idx];
                    let bytes = store
                        .read_vault_granule(table_id, part_id, column_name, mark)
                        .await
                        .map_err(|e| VeinError::internal(e.to_string()))?;
                    let reader_bytes = build_single_vault_reader_bytes(header, mark, &bytes, row_count)?;
                    let address_mark = &address_marks[granule_idx];
                    let address_bytes = store
                        .read_address_granule(table_id, part_id, column_name, address_mark)
                        .await
                        .map_err(|e| VeinError::internal(e.to_string()))?;
                    let address_reader_bytes = build_single_granule_reader_bytes(
                        address_header,
                        address_mark,
                        &address_bytes,
                        row_count,
                    )?;
                    let address_reader = ExoAddressReader::from_bytes(address_reader_bytes)
                        .map_err(|e| VeinError::internal(e.to_string()))?;
                    let address_batch = AddressBatch::from_reader(&address_reader, &filtered_slots)
                        .map_err(|e| VeinError::internal(format!("failed to read address sidecar: {e}")))?;
                    let vault_reader = ExoVaultReader::from_bytes(reader_bytes)
                        .map_err(|e| VeinError::internal(e.to_string()))?;
                    (ColumnScanner::Vault(vault_reader), Some(address_batch))
                }
            };
            vectors.push(materialize_scanner_array(
                &mut scanner,
                &filtered_slots,
                column_name,
                data_type.as_deref(),
            )?);
            address_columns.push(address_batch);
        }
        stats.rows_emitted += filtered_slots.len() as u64;
        next.push(DataChunk::from_vectors_with_addresses(vectors, address_columns)?)?;
    }

    Ok(true)
}

/// Scans a VFS partition using address indexes and selected granule reads when
/// every projected column is address-only. Returns false when vault materialization
/// is required and the established full-buffer path should be used.
async fn scan_vfs_address_partition(
    store: &vfs::VfsTableStore,
    table_id: u32,
    part_id: u32,
    projection: Option<&[FieldSchema]>,
    filter: Option<&[AddressFilter]>,
    next: &mut dyn PipelineNode<DataChunk>,
    stats: &mut ExoScanStats,
) -> Result<bool> {
    let schema = store
        .read_table_schema(table_id)
        .await
        .map_err(|e| VeinError::internal(e.to_string()))?;
    let target_fields: Vec<(String, Option<String>)> = match projection {
        Some(fields) => fields
            .iter()
            .map(|field| (field.name.clone(), Some(field.data_type.clone())))
            .collect(),
        None => schema
            .fields
            .iter()
            .map(|field| (field.name.clone(), Some(format!("{:?}", field.data_type.to_arrow()))))
            .collect(),
    };
    if target_fields.is_empty() {
        return Ok(false);
    }

    for (column_name, data_type) in &target_fields {
        if data_type_uses_vault(data_type.as_deref()) && store
            .partition_plane_exists(
                table_id,
                part_id,
                column_name,
                vfs::PlaneType::PayloadVault,
            )
            .await
            .map_err(|e| VeinError::internal(e.to_string()))?
        {
            return scan_vfs_mixed_partition(store, table_id, part_id, projection, filter, next, stats)
                .await;
        }
    }

    let mut indexes = Vec::with_capacity(target_fields.len());
    for (column_name, data_type) in &target_fields {
        let (header, marks) = store
            .read_address_index(table_id, part_id, column_name)
            .await
            .map_err(|e| VeinError::internal(e.to_string()))?;
        indexes.push((column_name.clone(), data_type.clone(), header, marks));
    }

    let tombstones = match store
        .read_partition_plane_all(
            table_id,
            part_id,
            "tombstones",
            vfs::PlaneType::Tombstone,
        )
        .await
    {
        Ok(bytes) if !bytes.is_empty() => exo::TombstoneMask::from_bytes(&bytes).unwrap_or_default(),
        _ => exo::TombstoneMask::new(),
    };

    let total_granules = indexes[0].3.len();
    stats.partitions_scanned += 1;
    stats.granules_considered += total_granules as u64;
    for (_, _, _, marks) in indexes.iter().skip(1) {
        if marks.len() != total_granules {
            return Err(VeinError::internal("Address columns have different granule counts"));
        }
    }

    'granules: for granule_idx in 0..total_granules {
        let first_header = &indexes[0].2;
        let first_mark = &indexes[0].3[granule_idx];
        let row_count = (first_mark.uncompressed_len as usize) / first_header.slot_width as usize;

        if let Some(filters) = filter {
            for address_filter in filters {
                if let Some((_, _, _, marks)) = indexes
                    .iter()
                    .find(|(column_name, _, _, _)| column_name == &address_filter.column)
                {
                    if !address_filter.predicate.may_match(&marks[granule_idx]) {
                        stats.granules_pruned += 1;
                        continue 'granules;
                    }
                }
            }
        }

        let slots: Vec<u32> = (0..row_count as u32)
            .filter(|slot| {
                let global_slot = granule_idx
                    .saturating_mul(first_header.granule_size as usize)
                    .saturating_add(*slot as usize) as u32;
                !tombstones.is_deleted(global_slot)
            })
            .collect();
        if slots.is_empty() {
            continue;
        }
        stats.rows_examined += slots.len() as u64;

        let mut filtered_slots = slots;
        if let Some(filters) = filter {
            for address_filter in filters {
                if let Some((column_name, _, header, marks)) = indexes
                    .iter()
                    .find(|(column_name, _, _, _)| column_name == &address_filter.column)
                {
                    let mark = &marks[granule_idx];
                    let bytes = store
                        .read_address_granule(table_id, part_id, column_name, mark)
                        .await
                        .map_err(|e| VeinError::internal(e.to_string()))?;
                    let reader_bytes = build_single_granule_reader_bytes(header, mark, &bytes, row_count)?;
                    let reader = ExoAddressReader::from_bytes(reader_bytes)
                        .map_err(|e| VeinError::internal(e.to_string()))?;
                    let mut scanner = ColumnScanner::Address(reader);
                    let before_filter = filtered_slots.len();
                    filtered_slots =
                        filter_address_slots(&mut scanner, &filtered_slots, &address_filter.predicate)?;
                    stats.rows_filtered += (before_filter - filtered_slots.len()) as u64;
                    if filtered_slots.is_empty() {
                        stats.granules_pruned += 1;
                        continue 'granules;
                    }
                }
            }
        }

        let mut vectors = Vec::with_capacity(indexes.len());
        let mut address_columns = Vec::with_capacity(indexes.len());
        for (column_name, data_type, header, marks) in &indexes {
            let mark = &marks[granule_idx];
            let bytes = store
                .read_address_granule(table_id, part_id, column_name, mark)
                .await
                .map_err(|e| VeinError::internal(e.to_string()))?;
            let reader_bytes = build_single_granule_reader_bytes(header, mark, &bytes, row_count)?;
            let mut scanner = ColumnScanner::Address(
                ExoAddressReader::from_bytes(reader_bytes)
                    .map_err(|e| VeinError::internal(e.to_string()))?,
            );
            let address_batch = match &mut scanner {
                ColumnScanner::Address(reader) => AddressBatch::from_reader(reader, &filtered_slots)
                    .map_err(|e| VeinError::internal(e.to_string()))?,
                ColumnScanner::Vault(_) => {
                    return Err(VeinError::internal("address-only scan created a vault scanner"));
                }
            };
            vectors.push(materialize_scanner_array(
                &mut scanner,
                &filtered_slots,
                column_name,
                data_type.as_deref(),
            )?);
            address_columns.push(Some(address_batch));
        }
        stats.rows_emitted += filtered_slots.len() as u64;
        next.push(DataChunk::from_vectors_with_addresses(vectors, address_columns)?)?;
    }

    Ok(true)
}



fn read_address_sidecars(
    readers: &mut [ExoAddressReader],
    slots: &[u32],
) -> Result<Vec<Option<AddressBatch>>> {
    readers
        .iter_mut()
        .map(|reader| {
            AddressBatch::from_reader(reader, slots)
                .map(Some)
                .map_err(|e| VeinError::internal(format!("failed to read address sidecar: {e}")))
        })
        .collect()
}

fn filter_address_slots(
    scanner: &mut ColumnScanner,
    slots: &[u32],
    predicate: &exo::AddressPredicate,
) -> Result<Vec<u32>> {
    let ColumnScanner::Address(reader) = scanner else {
        return Ok(slots.to_vec());
    };
    filter_address_reader_slots(reader, slots, predicate)
}

fn filter_address_reader_slots(
    reader: &mut ExoAddressReader,
    slots: &[u32],
    predicate: &exo::AddressPredicate,
) -> Result<Vec<u32>> {
    let batch = AddressBatch::from_reader(reader, slots)
        .map_err(|e| VeinError::internal(e.to_string()))?;
    let filtered = slots
        .iter()
        .copied()
        .zip(batch.iter())
        .filter_map(|(slot, (value, valid))| predicate.matches(value, valid).then_some(slot))
        .collect();
    Ok(filtered)
}

fn build_single_vault_reader_bytes(
    header: &VaultHeader,
    mark: &VaultGranuleMark,
    bytes: &[u8],
    row_count: usize,
) -> Result<Vec<u8>> {
    if bytes.len() != mark.compressed_len as usize {
        return Err(VeinError::internal("Vault granule length mismatch"));
    }
    let synthetic_header = VaultHeader {
        magic: *VAULT_HEADER_MAGIC,
        version: VAULT_FORMAT_VERSION,
        codec_flags: header.codec_flags,
        total_rows: row_count as u64,
        granule_size: row_count as u32,
        total_granules: 1,
        marks_offset: (VAULT_HEADER_SIZE + bytes.len()) as u64,
        checksum: 0,
    };
    let synthetic_mark = VaultGranuleMark {
        byte_offset: VAULT_HEADER_SIZE as u64,
        ..*mark
    };
    let mut output = Vec::with_capacity(
        VAULT_HEADER_SIZE + bytes.len() + VAULT_MARK_SIZE + VAULT_TRAILER_SIZE,
    );
    output.extend_from_slice(&synthetic_header.encode());
    output.extend_from_slice(bytes);
    output.extend_from_slice(&synthetic_mark.encode());
    output.extend_from_slice(&[0u8; 4]);
    output.extend_from_slice(VAULT_TRAILER_MAGIC);
    Ok(output)
}

fn build_single_granule_reader_bytes(
    header: &AddressHeader,
    mark: &AddressGranuleMark,
    bytes: &[u8],
    row_count: usize,
) -> Result<Vec<u8>> {
    if bytes.len() != mark.compressed_len as usize {
        return Err(VeinError::internal("Address granule length mismatch"));
    }
    let synthetic_mark = AddressGranuleMark {
        byte_offset: HEADER_SIZE as u64,
        ..*mark
    };
    let synthetic_header = AddressHeader {
        magic: *ADDR_HEADER_MAGIC,
        version: ADDR_FORMAT_VERSION,
        codec_flags: header.codec_flags,
        total_rows: row_count as u64,
        granule_size: row_count as u32,
        total_granules: 1,
        marks_offset: (HEADER_SIZE + bytes.len()) as u64,
        checksum: 0,
        slot_width: header.slot_width,
        _reserved: [0; 27],
    };
    let mut output = Vec::with_capacity(
        HEADER_SIZE + bytes.len() + MARK_SIZE + std::mem::size_of::<u32>() + ADDR_TRAILER_MAGIC.len(),
    );
    output.extend_from_slice(&synthetic_header.encode());
    output.extend_from_slice(bytes);
    output.extend_from_slice(&synthetic_mark.encode());
    output.extend_from_slice(&1u32.to_le_bytes());
    output.extend_from_slice(ADDR_TRAILER_MAGIC);
    Ok(output)
}

/// Executes vectorized late-materialization scanning over a single [`PartReader`].
pub fn scan_part_reader(
    part_reader: &PartReader,
    projection: Option<&[FieldSchema]>,
    next: &mut dyn PipelineNode<DataChunk>,
) -> Result<()> {
    scan_part_reader_with_filter(part_reader, projection, None, next)
}

/// Scans a part reader and applies an optional address predicate during materialization.
pub fn scan_part_reader_with_filter(
    part_reader: &PartReader,
    projection: Option<&[FieldSchema]>,
    filter: Option<&[AddressFilter]>,
    next: &mut dyn PipelineNode<DataChunk>,
) -> Result<()> {
    let mut stats = ExoScanStats::default();
    scan_part_reader_with_filter_and_stats(part_reader, projection, filter, next, &mut stats)
}

/// Scans a part reader and updates detailed scan counters.
pub fn scan_part_reader_with_filter_and_stats(
    part_reader: &PartReader,
    projection: Option<&[FieldSchema]>,
    filter: Option<&[AddressFilter]>,
    next: &mut dyn PipelineNode<DataChunk>,
    stats: &mut ExoScanStats,
) -> Result<()> {
    let total_rows = part_reader.total_rows();
    if total_rows == 0 {
        return Ok(());
    }

    // 1. Check if all rows are tombstoned
    let tombstones = part_reader.tombstones();
    if tombstones.deleted_count() >= total_rows {
        tracing::info!(part_id = part_reader.part_id(), "All rows tombstoned, skipping part");
        return Ok(());
    }
    stats.partitions_scanned += 1;
    stats.rows_examined += total_rows.saturating_sub(tombstones.deleted_count());

    // 2. Determine Projected Columns and their Schemas
    let available_cols = part_reader.column_names();
    let target_fields: Vec<(String, Option<String>)> = match projection {
        Some(proj) => {
            let mut fields = Vec::with_capacity(proj.len());
            for field in proj {
                if !available_cols.contains(&field.name) {
                    return Err(VeinError::internal(format!(
                        "Projected column '{}' not found in .exo part. Available columns: {:?}",
                        field.name, available_cols
                    )));
                }
                fields.push((field.name.clone(), Some(field.data_type.clone())));
            }
            fields
        }
        None => available_cols.iter().map(|col| (col.clone(), None)).collect(),
    };

    // 3. Pre-open column Scanners (Vault or Address) once per partition
    let mut column_scanners: Vec<(String, Option<String>, ColumnScanner)> = Vec::with_capacity(target_fields.len());
    let mut address_readers = Vec::with_capacity(target_fields.len());
    for (col_name, data_type) in &target_fields {
        let address_reader = part_reader
            .open_address_reader(col_name)
            .map_err(|e| VeinError::internal(format!("Failed to open address reader for column '{}': {}", col_name, e)))?;
        address_readers.push(address_reader);

        if part_reader.has_vault(col_name) {
            let reader = part_reader
                .open_vault_reader(col_name)
                .map_err(|e| VeinError::internal(format!("Failed to open vault for column '{}': {}", col_name, e)))?;
            column_scanners.push((col_name.clone(), data_type.clone(), ColumnScanner::Vault(reader)));
        } else {
            let reader = part_reader
                .open_address_reader(col_name)
                .map_err(|e| VeinError::internal(format!("Failed to open address reader for column '{}': {}", col_name, e)))?;
            column_scanners.push((col_name.clone(), data_type.clone(), ColumnScanner::Address(reader)));
        }
    }

    let mut predicate_scanners: Vec<(String, ExoAddressReader)> = Vec::new();
    if let Some(filters) = filter {
        for address_filter in filters {
            if predicate_scanners
                .iter()
                .any(|(column_name, _)| column_name == &address_filter.column)
            {
                continue;
            }
            let reader = part_reader
                .open_address_reader(&address_filter.column)
                .map_err(|e| VeinError::internal(format!("Failed to open predicate address column '{}': {}", address_filter.column, e)))?;
            predicate_scanners.push((address_filter.column.clone(), reader));
        }
    }

    // 4. Bounded Vectorized Streaming (O(1) memory) & Late Materialization
    let mut chunk_slots: Vec<u32> = Vec::with_capacity(DEFAULT_EXO_VECTOR_SIZE);
    let mut rows_processed = 0;
    let total_u32 = total_rows.min(u32::MAX as u64) as u32;
    let granule_size = predicate_scanners
        .first()
        .map(|(_, reader)| reader.granule_size())
        .unwrap_or(DEFAULT_EXO_VECTOR_SIZE);
    let total_predicate_granules = predicate_scanners
        .first()
        .map(|(_, reader)| reader.total_granules())
        .unwrap_or(0);
    let mut granule_may_match = vec![true; total_predicate_granules];
    if !predicate_scanners.is_empty() {
        stats.granules_considered += total_predicate_granules as u64;
        for granule_idx in 0..total_predicate_granules {
            if let Some(filters) = filter {
                for address_filter in filters {
                    if let Some((_, reader)) = predicate_scanners
                        .iter()
                        .find(|(column_name, _)| column_name == &address_filter.column)
                    {
                        let mark = reader
                            .granule_mark(granule_idx)
                            .map_err(|e| VeinError::internal(e.to_string()))?;
                        if !address_filter.predicate.may_match(mark) {
                            granule_may_match[granule_idx] = false;
                            stats.granules_pruned += 1;
                            break;
                        }
                    }
                }
            }
        }
    }

    for row_id in 0..total_u32 {
        let granule_idx = (row_id as usize) / granule_size;
        if granule_idx < granule_may_match.len() && !granule_may_match[granule_idx] {
            continue;
        }
        if !tombstones.is_deleted(row_id) {
            chunk_slots.push(row_id);
            if chunk_slots.len() >= DEFAULT_EXO_VECTOR_SIZE {
                let filtered_slots = apply_address_filter(&mut column_scanners, &mut predicate_scanners, &chunk_slots, filter)?;
                stats.rows_filtered += (chunk_slots.len() - filtered_slots.len()) as u64;
                if !filtered_slots.is_empty() {
                    let mut vectors: Vec<ArrayRef> = Vec::with_capacity(column_scanners.len());
                    for (col_name, data_type, scanner) in &mut column_scanners {
                        let array = materialize_scanner_array(scanner, &filtered_slots, col_name, data_type.as_deref())?;
                        vectors.push(array);
                    }
                    rows_processed += filtered_slots.len();
                    stats.rows_emitted += filtered_slots.len() as u64;
                    let address_columns = read_address_sidecars(&mut address_readers, &filtered_slots)?;
                    let chunk = DataChunk::from_vectors_with_addresses(vectors, address_columns)?;
                    next.push(chunk)?;
                }
                chunk_slots.clear();
            }
        }
    }

    // Flush trailing chunk
    if !chunk_slots.is_empty() {
        let filtered_slots = apply_address_filter(&mut column_scanners, &mut predicate_scanners, &chunk_slots, filter)?;
        stats.rows_filtered += (chunk_slots.len() - filtered_slots.len()) as u64;
        if !filtered_slots.is_empty() {
            let mut vectors: Vec<ArrayRef> = Vec::with_capacity(column_scanners.len());
            for (col_name, data_type, scanner) in &mut column_scanners {
                let array = materialize_scanner_array(scanner, &filtered_slots, col_name, data_type.as_deref())?;
                vectors.push(array);
            }
            rows_processed += filtered_slots.len();
            stats.rows_emitted += filtered_slots.len() as u64;
            let address_columns = read_address_sidecars(&mut address_readers, &filtered_slots)?;
            let chunk = DataChunk::from_vectors_with_addresses(vectors, address_columns)?;
            next.push(chunk)?;
        }
        chunk_slots.clear();
    }

    tracing::info!(
        part_id = part_reader.part_id(),
        surviving_rows = rows_processed,
        total_rows = total_rows,
        "Completed scanning .exo partition"
    );

    Ok(())
}

fn apply_address_filter(
    column_scanners: &mut [(String, Option<String>, ColumnScanner)],
    predicate_scanners: &mut [(String, ExoAddressReader)],
    slots: &[u32],
    filter: Option<&[AddressFilter]>,
) -> Result<Vec<u32>> {
    let Some(filters) = filter else {
        return Ok(slots.to_vec());
    };
    let mut filtered_slots = slots.to_vec();
    for filter in filters {
        if let Some((_, reader)) = predicate_scanners
            .iter_mut()
            .find(|(column_name, _)| column_name == &filter.column)
        {
            filtered_slots =
                filter_address_reader_slots(reader, &filtered_slots, &filter.predicate)?;
        } else if let Some((_, _, scanner)) = column_scanners
            .iter_mut()
            .find(|(column_name, _, _)| column_name == &filter.column)
        {
            filtered_slots = filter_address_slots(scanner, &filtered_slots, &filter.predicate)?;
        } else {
            continue;
        }
        if filtered_slots.is_empty() {
            break;
        }
    }
    Ok(filtered_slots)
}

/// Helper to materialize a batch of slots from either a Payload Vault or an inlined Address Plane.
fn materialize_scanner_array(
    scanner: &mut ColumnScanner,
    slots: &[u32],
    col_name: &str,
    data_type: Option<&str>,
) -> Result<ArrayRef> {
    match scanner {
        ColumnScanner::Vault(vault_reader) => {
            materialize_column_array(vault_reader, slots, col_name, data_type)
        }
        ColumnScanner::Address(addr_reader) => {
            let slot_width = addr_reader.slot_width();
            match data_type {
                Some("Float64") => {
                    let mut float_builder = Float64Builder::with_capacity(slots.len());
                    for &slot in slots {
                        if !addr_reader
                            .is_valid(slot as u64)
                            .map_err(|e| VeinError::internal(e.to_string()))?
                        {
                            float_builder.append_null();
                            continue;
                        }
                        let vaddr = addr_reader
                            .read_vaddr(slot as u64)
                            .map_err(|e| VeinError::internal(e.to_string()))?;
                        float_builder.append_value(f64_from_canonical(vaddr));
                    }
                    Ok(Arc::new(float_builder.finish()) as ArrayRef)
                }
                Some("Float32") => {
                    let mut float_builder = Float32Builder::with_capacity(slots.len());
                    for &slot in slots {
                        if !addr_reader
                            .is_valid(slot as u64)
                            .map_err(|e| VeinError::internal(e.to_string()))?
                        {
                            float_builder.append_null();
                            continue;
                        }
                        let vaddr = addr_reader
                            .read_vaddr(slot as u64)
                            .map_err(|e| VeinError::internal(e.to_string()))?;
                        let val = if slot_width == 4 {
                            f32_from_canonical_native(vaddr as u32)
                        } else {
                            f64_from_canonical(vaddr) as f32
                        };
                        float_builder.append_value(val);
                    }
                    Ok(Arc::new(float_builder.finish()) as ArrayRef)
                }
                Some("Int32") => {
                    let mut int_builder = Int32Builder::with_capacity(slots.len());
                    for &slot in slots {
                        if !addr_reader
                            .is_valid(slot as u64)
                            .map_err(|e| VeinError::internal(e.to_string()))?
                        {
                            int_builder.append_null();
                            continue;
                        }
                        let vaddr = addr_reader
                            .read_vaddr(slot as u64)
                            .map_err(|e| VeinError::internal(e.to_string()))?;
                        let val = if slot_width == 4 {
                            i32_from_canonical_native(vaddr as u32)
                        } else {
                            i64_from_canonical(vaddr) as i32
                        };
                        int_builder.append_value(val);
                    }
                    Ok(Arc::new(int_builder.finish()) as ArrayRef)
                }
                Some("Date32") => {
                    let mut date_builder = Date32Builder::with_capacity(slots.len());
                    for &slot in slots {
                        if !addr_reader
                            .is_valid(slot as u64)
                            .map_err(|e| VeinError::internal(e.to_string()))?
                        {
                            date_builder.append_null();
                            continue;
                        }
                        let vaddr = addr_reader
                            .read_vaddr(slot as u64)
                            .map_err(|e| VeinError::internal(e.to_string()))?;
                        let val = if slot_width == 4 {
                            i32_from_canonical_native(vaddr as u32)
                        } else {
                            i64_from_canonical(vaddr) as i32
                        };
                        date_builder.append_value(val);
                    }
                    Ok(Arc::new(date_builder.finish()) as ArrayRef)
                }
                Some("Int16") => {
                    let mut int_builder = Int16Builder::with_capacity(slots.len());
                    for &slot in slots {
                        if !addr_reader
                            .is_valid(slot as u64)
                            .map_err(|e| VeinError::internal(e.to_string()))?
                        {
                            int_builder.append_null();
                            continue;
                        }
                        let vaddr = addr_reader
                            .read_vaddr(slot as u64)
                            .map_err(|e| VeinError::internal(e.to_string()))?;
                        let val = if slot_width == 2 {
                            i16_from_canonical_native(vaddr as u16)
                        } else {
                            i64_from_canonical(vaddr) as i16
                        };
                        int_builder.append_value(val);
                    }
                    Ok(Arc::new(int_builder.finish()) as ArrayRef)
                }
                Some("Int8") => {
                    let mut int_builder = Int8Builder::with_capacity(slots.len());
                    for &slot in slots {
                        if !addr_reader
                            .is_valid(slot as u64)
                            .map_err(|e| VeinError::internal(e.to_string()))?
                        {
                            int_builder.append_null();
                            continue;
                        }
                        let vaddr = addr_reader
                            .read_vaddr(slot as u64)
                            .map_err(|e| VeinError::internal(e.to_string()))?;
                        let val = if slot_width == 1 {
                            i8_from_canonical_native(vaddr as u8)
                        } else {
                            i64_from_canonical(vaddr) as i8
                        };
                        int_builder.append_value(val);
                    }
                    Ok(Arc::new(int_builder.finish()) as ArrayRef)
                }
                Some("UInt32") => {
                    let mut int_builder = UInt32Builder::with_capacity(slots.len());
                    for &slot in slots {
                        if !addr_reader
                            .is_valid(slot as u64)
                            .map_err(|e| VeinError::internal(e.to_string()))?
                        {
                            int_builder.append_null();
                            continue;
                        }
                        let vaddr = addr_reader
                            .read_vaddr(slot as u64)
                            .map_err(|e| VeinError::internal(e.to_string()))?;
                        int_builder.append_value(vaddr as u32);
                    }
                    Ok(Arc::new(int_builder.finish()) as ArrayRef)
                }
                Some("UInt16") => {
                    let mut int_builder = UInt16Builder::with_capacity(slots.len());
                    for &slot in slots {
                        if !addr_reader
                            .is_valid(slot as u64)
                            .map_err(|e| VeinError::internal(e.to_string()))?
                        {
                            int_builder.append_null();
                            continue;
                        }
                        let vaddr = addr_reader
                            .read_vaddr(slot as u64)
                            .map_err(|e| VeinError::internal(e.to_string()))?;
                        int_builder.append_value(vaddr as u16);
                    }
                    Ok(Arc::new(int_builder.finish()) as ArrayRef)
                }
                Some("UInt8") => {
                    let mut int_builder = UInt8Builder::with_capacity(slots.len());
                    for &slot in slots {
                        if !addr_reader
                            .is_valid(slot as u64)
                            .map_err(|e| VeinError::internal(e.to_string()))?
                        {
                            int_builder.append_null();
                            continue;
                        }
                        let vaddr = addr_reader
                            .read_vaddr(slot as u64)
                            .map_err(|e| VeinError::internal(e.to_string()))?;
                        int_builder.append_value(vaddr as u8);
                    }
                    Ok(Arc::new(int_builder.finish()) as ArrayRef)
                }
                Some("Boolean") => {
                    let mut bool_builder = BooleanBuilder::with_capacity(slots.len());
                    for &slot in slots {
                        if !addr_reader
                            .is_valid(slot as u64)
                            .map_err(|e| VeinError::internal(e.to_string()))?
                        {
                            bool_builder.append_null();
                            continue;
                        }
                        let vaddr = addr_reader
                            .read_vaddr(slot as u64)
                            .map_err(|e| VeinError::internal(e.to_string()))?;
                        bool_builder.append_value(vaddr != 0);
                    }
                    Ok(Arc::new(bool_builder.finish()) as ArrayRef)
                }
                Some("Int64") | None => {
                    let mut int_builder = Int64Builder::with_capacity(slots.len());
                    for &slot in slots {
                        if !addr_reader
                            .is_valid(slot as u64)
                            .map_err(|e| VeinError::internal(e.to_string()))?
                        {
                            int_builder.append_null();
                            continue;
                        }
                        let vaddr = addr_reader
                            .read_vaddr(slot as u64)
                            .map_err(|e| VeinError::internal(e.to_string()))?;
                        int_builder.append_value(i64_from_canonical(vaddr));
                    }
                    Ok(Arc::new(int_builder.finish()) as ArrayRef)
                }
                Some("UInt64") => {
                    let mut int_builder = UInt64Builder::with_capacity(slots.len());
                    for &slot in slots {
                        if !addr_reader
                            .is_valid(slot as u64)
                            .map_err(|e| VeinError::internal(e.to_string()))?
                        {
                            int_builder.append_null();
                            continue;
                        }
                        let vaddr = addr_reader
                            .read_vaddr(slot as u64)
                            .map_err(|e| VeinError::internal(e.to_string()))?;
                        int_builder.append_value(vaddr);
                    }
                    Ok(Arc::new(int_builder.finish()) as ArrayRef)
                }
                Some(other) => Err(VeinError::internal(format!(
                    "Unsupported data type '{other}' for .exo address column '{col_name}'"
                ))),
            }
        }
    }
}

/// Helper to materialize a batch of slots from a Payload Vault into an Arrow ArrayRef.
fn materialize_column_array(
    vault_reader: &mut ExoVaultReader,
    slots: &[u32],
    col_name: &str,
    data_type: Option<&str>,
) -> Result<ArrayRef> {
    match data_type {
        Some("Date64") => {
            let values = read_fixed_i64_values(vault_reader, slots, col_name)?;
            return Ok(Arc::new(Date64Array::from(values)) as ArrayRef);
        }
        Some(type_name) if type_name.starts_with("FixedSizeBinary(") => {
            let width = type_name
                .strip_prefix("FixedSizeBinary(")
                .and_then(|value| value.strip_suffix(')'))
                .and_then(|value| value.parse::<i32>().ok())
                .ok_or_else(|| VeinError::internal(format!("Invalid fixed binary type '{type_name}'")))?;
            let mut builder = FixedSizeBinaryBuilder::new(width);
            for &slot in slots {
                if vault_reader
                    .is_null(slot)
                    .map_err(|e| VeinError::internal(e.to_string()))?
                {
                    builder.append_null();
                } else {
                    let bytes = vault_reader
                        .read_fixed_bytes_slot(slot)
                        .map_err(|e| VeinError::internal(format!("Error reading fixed binary payload for column '{col_name}' at slot {slot}: {e}")))?;
                    builder
                        .append_value(bytes)
                        .map_err(|e| VeinError::internal(format!("Invalid fixed binary payload for column '{col_name}' at slot {slot}: {e}")))?;
                }
            }
            return Ok(Arc::new(builder.finish()) as ArrayRef);
        }
        Some(type_name)
            if type_name.starts_with("Timestamp(")
                || type_name.starts_with("Time64(")
                || type_name.starts_with("Duration(") =>
        {
            let values = read_fixed_i64_values(vault_reader, slots, col_name)?;
            let target_type = parse_temporal_type(type_name)?;
            let array = Int64Array::from(values);
            return arrow::compute::cast(&array, &target_type)
                .map_err(|e| VeinError::internal(format!("Failed to materialize '{type_name}' for column '{col_name}': {e}")));
        }
        Some(decimal_type) if decimal_type.starts_with("Decimal128(") => {
            let (precision, scale) = parse_decimal_type(decimal_type)?;
            let mut values = Vec::with_capacity(slots.len());
            for &slot in slots {
                if vault_reader
                    .is_null(slot)
                    .map_err(|e| VeinError::internal(e.to_string()))?
                {
                    values.push(None);
                    continue;
                }
                let bytes = vault_reader
                    .read_fixed_bytes_slot(slot)
                    .map_err(|e| VeinError::internal(format!("Error reading Decimal128 payload for column '{col_name}' at slot {slot}: {e}")))?;
                let raw: [u8; 16] = bytes.try_into().map_err(|_| {
                    VeinError::internal(format!("Invalid Decimal128 payload width for column '{col_name}' at slot {slot}"))
                })?;
                values.push(Some(i128::from_le_bytes(raw)));
            }
            let array = Decimal128Array::from(values)
                .with_precision_and_scale(precision, scale)
                .map_err(|e| VeinError::internal(format!("Invalid Decimal128 metadata for column '{col_name}': {e}")))?;
            return Ok(Arc::new(array) as ArrayRef);
        }
        Some(decimal_type) if decimal_type.starts_with("Decimal256(") => {
            let (precision, scale) = parse_decimal_type(decimal_type)?;
            let mut values = Vec::with_capacity(slots.len());
            for &slot in slots {
                if vault_reader
                    .is_null(slot)
                    .map_err(|e| VeinError::internal(e.to_string()))?
                {
                    values.push(None);
                    continue;
                }
                let bytes = vault_reader
                    .read_fixed_bytes_slot(slot)
                    .map_err(|e| VeinError::internal(format!("Error reading Decimal256 payload for column '{col_name}' at slot {slot}: {e}")))?;
                let raw: [u8; 32] = bytes.try_into().map_err(|_| {
                    VeinError::internal(format!("Invalid Decimal256 payload width for column '{col_name}' at slot {slot}"))
                })?;
                values.push(Some(arrow::datatypes::i256::from_le_bytes(raw)));
            }
            let array = Decimal256Array::from(values)
                .with_precision_and_scale(precision, scale)
                .map_err(|e| VeinError::internal(format!("Invalid Decimal256 metadata for column '{col_name}': {e}")))?;
            return Ok(Arc::new(array) as ArrayRef);
        }
        Some("Int64") => {
            let mut builder = Int64Builder::with_capacity(slots.len());
            for &slot in slots {
                if vault_reader
                    .is_null(slot)
                    .map_err(|e| VeinError::internal(e.to_string()))?
                {
                    builder.append_null();
                    continue;
                }
                let bytes = vault_reader
                    .read_fixed_bytes_slot(slot)
                    .map_err(|e| VeinError::internal(format!("Error reading Int64 payload for column '{col_name}' at slot {slot}: {e}")))?;
                let raw: [u8; 8] = bytes.try_into().map_err(|_| {
                    VeinError::internal(format!("Invalid Int64 payload width for column '{col_name}' at slot {slot}"))
                })?;
                builder.append_value(i64::from_le_bytes(raw));
            }
            return Ok(Arc::new(builder.finish()) as ArrayRef);
        }
        Some("UInt64") => {
            let mut builder = UInt64Builder::with_capacity(slots.len());
            for &slot in slots {
                if vault_reader
                    .is_null(slot)
                    .map_err(|e| VeinError::internal(e.to_string()))?
                {
                    builder.append_null();
                    continue;
                }
                let bytes = vault_reader
                    .read_fixed_bytes_slot(slot)
                    .map_err(|e| VeinError::internal(format!("Error reading UInt64 payload for column '{col_name}' at slot {slot}: {e}")))?;
                let raw: [u8; 8] = bytes.try_into().map_err(|_| {
                    VeinError::internal(format!("Invalid UInt64 payload width for column '{col_name}' at slot {slot}"))
                })?;
                builder.append_value(u64::from_le_bytes(raw));
            }
            return Ok(Arc::new(builder.finish()) as ArrayRef);
        }
        Some("Float64") => {
            let mut builder = Float64Builder::with_capacity(slots.len());
            for &slot in slots {
                if vault_reader
                    .is_null(slot)
                    .map_err(|e| VeinError::internal(e.to_string()))?
                {
                    builder.append_null();
                    continue;
                }
                let bytes = vault_reader
                    .read_fixed_bytes_slot(slot)
                    .map_err(|e| VeinError::internal(format!("Error reading Float64 payload for column '{col_name}' at slot {slot}: {e}")))?;
                let raw: [u8; 8] = bytes.try_into().map_err(|_| {
                    VeinError::internal(format!("Invalid Float64 payload width for column '{col_name}' at slot {slot}"))
                })?;
                builder.append_value(f64::from_le_bytes(raw));
            }
            return Ok(Arc::new(builder.finish()) as ArrayRef);
        }
        _ => {}
    }

    // Variable-width and non-native values use the existing borrowed payload path.
    // 1. Attempt high-speed String materialization
    let mut str_builder = StringBuilder::with_capacity(slots.len(), slots.len() * 32);
    match vault_reader.materialize_string_batch(slots, &mut str_builder) {
        Ok(_) => Ok(Arc::new(str_builder.finish()) as ArrayRef),
        Err(_) => {
            // 2. Fallback to BinaryArray if arbitrary non-UTF-8 bytes are present
            let mut bin_builder = BinaryBuilder::with_capacity(slots.len(), slots.len() * 32);
            for &slot in slots {
                let is_null = vault_reader
                    .is_null(slot)
                    .map_err(|e| VeinError::internal(format!("Error reading null mask for column '{}' at slot {}: {}", col_name, slot, e)))?;
                if is_null {
                    bin_builder.append_null();
                } else {
                    let bytes = vault_reader
                        .read_bytes_slot(slot)
                        .map_err(|e| VeinError::internal(format!("Error reading bytes for column '{}' at slot {}: {}", col_name, slot, e)))?;
                    bin_builder.append_value(bytes);
                }
            }
            Ok(Arc::new(bin_builder.finish()) as ArrayRef)
        }
    }
}

fn read_fixed_i64_values(
    vault_reader: &mut ExoVaultReader,
    slots: &[u32],
    col_name: &str,
) -> Result<Vec<Option<i64>>> {
    let mut values = Vec::with_capacity(slots.len());
    for &slot in slots {
        if vault_reader
            .is_null(slot)
            .map_err(|e| VeinError::internal(e.to_string()))?
        {
            values.push(None);
            continue;
        }
        let bytes = vault_reader
            .read_fixed_bytes_slot(slot)
            .map_err(|e| VeinError::internal(format!("Error reading temporal payload for column '{col_name}' at slot {slot}: {e}")))?;
        let raw: [u8; 8] = bytes.try_into().map_err(|_| {
            VeinError::internal(format!("Invalid temporal payload width for column '{col_name}' at slot {slot}"))
        })?;
        values.push(Some(i64::from_le_bytes(raw)));
    }
    Ok(values)
}

fn parse_temporal_type(type_name: &str) -> Result<DataType> {
    let (prefix, inner) = if let Some(inner) = type_name
        .strip_prefix("Timestamp(")
        .and_then(|value| value.strip_suffix(')'))
    {
        ("Timestamp", inner)
    } else if let Some(inner) = type_name
        .strip_prefix("Time64(")
        .and_then(|value| value.strip_suffix(')'))
    {
        ("Time64", inner)
    } else if let Some(inner) = type_name
        .strip_prefix("Duration(")
        .and_then(|value| value.strip_suffix(')'))
    {
        ("Duration", inner)
    } else {
        return Err(VeinError::internal(format!("Invalid temporal type '{type_name}'")));
    };

    let mut fields = inner.split(',').map(str::trim);
    let first = fields.next().unwrap_or_default();

    // VeinDataType stores timestamps as Timestamp(timezone), while Arrow's
    // debug form is Timestamp(unit, timezone). Vein timestamps use microseconds.
    if prefix == "Timestamp" && (first == "None" || first.starts_with('"')) {
        let timezone = (first != "None").then(|| Arc::<str>::from(first.trim_matches('"')));
        return Ok(DataType::Timestamp(TimeUnit::Microsecond, timezone));
    }

    let unit = match first {
        "Second" => TimeUnit::Second,
        "Millisecond" => TimeUnit::Millisecond,
        "Microsecond" => TimeUnit::Microsecond,
        "Nanosecond" => TimeUnit::Nanosecond,
        other => return Err(VeinError::internal(format!("Unsupported temporal unit '{other}'"))),
    };

    let timezone = if prefix == "Timestamp" {
        fields
            .next()
            .and_then(|value| value.strip_prefix("Some("))
            .and_then(|value| value.strip_suffix(')'))
            .map(|value| Arc::<str>::from(value.trim_matches('"')))
    } else {
        None
    };

    match prefix {
        "Timestamp" => Ok(DataType::Timestamp(unit, timezone)),
        "Time64" => Ok(DataType::Time64(unit)),
        "Duration" => Ok(DataType::Duration(unit)),
        _ => Err(VeinError::internal(format!("Unsupported temporal type '{type_name}'"))),
    }
}

fn parse_decimal_type(type_name: &str) -> Result<(u8, i8)> {
    let inner = type_name
        .strip_prefix("Decimal128(")
        .or_else(|| type_name.strip_prefix("Decimal256("))
        .and_then(|value| value.strip_suffix(')'))
        .ok_or_else(|| VeinError::internal(format!("Invalid decimal type '{type_name}'")))?;
    let (precision, scale) = inner
        .split_once(',')
        .ok_or_else(|| VeinError::internal(format!("Invalid decimal type '{type_name}'")))?;
    let precision = precision
        .trim()
        .parse::<u8>()
        .map_err(|e| VeinError::internal(format!("Invalid decimal precision: {e}")))?;
    let scale = scale
        .trim()
        .parse::<i8>()
        .map_err(|e| VeinError::internal(format!("Invalid decimal scale: {e}")))?;
    Ok((precision, scale))
}

/// Resolves the directory of the partition or table.
fn resolve_part_dir(path: &Path) -> Result<PathBuf> {
    if path.exists() {
        if path.is_dir() {
            return Ok(path.to_path_buf());
        } else if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() && parent.is_dir() {
                return Ok(parent.to_path_buf());
            }
        }
    }

    let data_cand = Path::new("data").join(path);
    if data_cand.exists() && data_cand.is_dir() {
        return Ok(data_cand);
    }

    Err(VeinError::not_found(format!(
        "Table partition directory {:?} not found on disk (also checked {:?})",
        path, data_cand
    )))
}

/// Extracts numeric partition ID from path (e.g. "part_000042" -> 42), or defaults to 0.
fn extract_part_id(path: &Path) -> u32 {
    let name = path
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("");

    if let Some(num_str) = name.strip_prefix("part_") {
        num_str.parse::<u32>().unwrap_or(0)
    } else {
        0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::StringArray;
    use common::Result;
    use exo::PartWriter;
    use tempfile::tempdir;

    struct CollectTestNode {
        chunks: Vec<DataChunk>,
    }

    impl CollectTestNode {
        fn new() -> Self {
            Self { chunks: Vec::new() }
        }
    }

    impl PipelineNode<DataChunk> for CollectTestNode {
        fn push(&mut self, batch: DataChunk) -> Result<()> {
            self.chunks.push(batch);
            Ok(())
        }

        fn flush(&mut self) -> Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_execute_exo_scan_end_to_end() -> Result<()> {
        let dir = tempdir().map_err(|e| VeinError::internal(e.to_string()))?;
        let part_id = 7;
        const GRANULE_SIZE: usize = 256;
        const TOTAL_ROWS: usize = 1000;

        let ids: Vec<String> = (0..TOTAL_ROWS).map(|i| format!("ID_{:04}", i)).collect();
        let statuses: Vec<String> = (0..TOTAL_ROWS)
            .map(|i| if i % 2 == 0 { "ACTIVE".into() } else { "PENDING".into() })
            .collect();
        let notes: Vec<String> = (0..TOTAL_ROWS).map(|i| format!("Note for record {}", i)).collect();

        // 1. Write the partition
        let desc = {
            let mut writer = PartWriter::create(dir.path(), part_id, GRANULE_SIZE)
                .map_err(|e| VeinError::internal(e.to_string()))?;
            writer
                .write_string_column("id", &ids)
                .map_err(|e| VeinError::internal(e.to_string()))?;
            writer
                .write_string_column("status", &statuses)
                .map_err(|e| VeinError::internal(e.to_string()))?;
            writer
                .write_string_column("notes", &notes)
                .map_err(|e| VeinError::internal(e.to_string()))?;
            writer.finish().map_err(|e| VeinError::internal(e.to_string()))?
        };

        // 2. Mark specific rows as tombstoned (delete row 0 and row 100)
        {
            let mut part_reader = PartReader::open(&desc.path, part_id)
                .map_err(|e| VeinError::internal(e.to_string()))?;
            part_reader.tombstones_mut().mark_deleted(0);
            part_reader.tombstones_mut().mark_deleted(100);
            part_reader.save_tombstones().map_err(|e| VeinError::internal(e.to_string()))?;
        }

        // 3. Scan without projection (all columns)
        let mut collector = CollectTestNode::new();
        let part_path_str = desc.path.to_string_lossy().to_string();
        execute_exo_scan(&[part_path_str.clone()], None, &mut collector).await?;

        assert_eq!(collector.chunks.len(), 1);
        let chunk = &collector.chunks[0];
        assert_eq!(chunk.num_columns(), 3);
        assert_eq!(chunk.num_rows(), TOTAL_ROWS - 2);

        // Verify row 0 was skipped and first emitted row is ID_0001
        let id_col = chunk.column(0).as_any().downcast_ref::<StringArray>().expect("downcast to StringArray");
        assert_eq!(id_col.value(0), "ID_0001");

        // 4. Scan with projection (only "id" and "status")
        let mut proj_collector = CollectTestNode::new();
        let proj = vec![
            FieldSchema {
                field_id: 0,
                name: "id".to_string(),
                data_type: "Utf8".to_string(),
            },
            FieldSchema {
                field_id: 1,
                name: "status".to_string(),
                data_type: "Utf8".to_string(),
            },
        ];
        execute_exo_scan(&[part_path_str], Some(&proj), &mut proj_collector).await?;

        assert_eq!(proj_collector.chunks.len(), 1);
        let proj_chunk = &proj_collector.chunks[0];
        assert_eq!(proj_chunk.num_columns(), 2);
        assert_eq!(proj_chunk.num_rows(), TOTAL_ROWS - 2);

        Ok(())
    }

    #[tokio::test]
    async fn test_execute_exo_scan_numeric_address_columns() -> Result<()> {
        use arrow::array::{Float64Array, Int64Array};
        use exo::ToCanonicalAddress;

        let dir = tempdir().map_err(|e| VeinError::internal(e.to_string()))?;
        let part_id = 1;
        const TOTAL_ROWS: usize = 100;

        let ids: Vec<u64> = (1..=TOTAL_ROWS as i64).map(|i| i.to_canonical()).collect();
        let amounts: Vec<u64> = (1..=TOTAL_ROWS)
            .map(|i| ((i as f64) * 1.5).to_canonical())
            .collect();

        // 1. Write the partition with address-only columns
        let desc = {
            let mut writer = PartWriter::create(dir.path(), part_id, 64)
                .map_err(|e| VeinError::internal(e.to_string()))?;
            writer
                .write_raw_address_column("id", &ids)
                .map_err(|e| VeinError::internal(e.to_string()))?;
            writer
                .write_raw_address_column("amount", &amounts)
                .map_err(|e| VeinError::internal(e.to_string()))?;
            writer.finish().map_err(|e| VeinError::internal(e.to_string()))?
        };

        // 2. Scan with authoritative schema projection
        let mut collector = CollectTestNode::new();
        let part_path_str = desc.path.to_string_lossy().to_string();
        let proj = vec![
            FieldSchema {
                field_id: 0,
                name: "id".to_string(),
                data_type: "Int64".to_string(),
            },
            FieldSchema {
                field_id: 1,
                name: "amount".to_string(),
                data_type: "Float64".to_string(),
            },
        ];

        execute_exo_scan(&[part_path_str], Some(&proj), &mut collector).await?;

        assert_eq!(collector.chunks.len(), 1);
        let chunk = &collector.chunks[0];
        assert_eq!(chunk.num_columns(), 2);
        assert_eq!(chunk.num_rows(), TOTAL_ROWS);

        let id_col = chunk.column(0).as_any().downcast_ref::<Int64Array>().expect("downcast to Int64Array");
        assert_eq!(id_col.value(0), 1);
        assert_eq!(id_col.value(99), 100);

        let amount_col = chunk.column(1).as_any().downcast_ref::<Float64Array>().expect("downcast to Float64Array");
        assert_eq!(amount_col.value(0), 1.5);
        assert_eq!(amount_col.value(99), 150.0);

        Ok(())
    }

    #[tokio::test]
    async fn test_execute_exo_scan_multi_partition_table_directory() -> Result<()> {
        let table_dir = tempdir().map_err(|e| VeinError::internal(e.to_string()))?;
        const ROWS_PER_PART: usize = 500;

        // Partition 0
        {
            let ids: Vec<String> = (0..ROWS_PER_PART).map(|i| format!("PART0_{:04}", i)).collect();
            let mut writer = PartWriter::create(table_dir.path(), 0, 256)
                .map_err(|e| VeinError::internal(e.to_string()))?;
            writer
                .write_string_column("id", &ids)
                .map_err(|e| VeinError::internal(e.to_string()))?;
            writer.finish().map_err(|e| VeinError::internal(e.to_string()))?;
        }

        // Partition 1
        {
            let ids: Vec<String> = (0..ROWS_PER_PART).map(|i| format!("PART1_{:04}", i)).collect();
            let mut writer = PartWriter::create(table_dir.path(), 1, 256)
                .map_err(|e| VeinError::internal(e.to_string()))?;
            writer
                .write_string_column("id", &ids)
                .map_err(|e| VeinError::internal(e.to_string()))?;
            writer.finish().map_err(|e| VeinError::internal(e.to_string()))?;
        }

        // Scan the entire table directory directly (no .exo extension, no part_ in query)
        let mut collector = CollectTestNode::new();
        let table_path_str = table_dir.path().to_string_lossy().to_string();
        execute_exo_scan(&[table_path_str], None, &mut collector).await?;

        // Total rows across both partitions should be 1000
        let total_scanned_rows: usize = collector.chunks.iter().map(|c| c.num_rows()).sum();
        assert_eq!(total_scanned_rows, 1000);

        // Verify data from both partitions
        let first_chunk = &collector.chunks[0];
        let id_col = first_chunk.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(id_col.value(0), "PART0_0000");

        let last_chunk = collector.chunks.last().unwrap();
        let last_id_col = last_chunk.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(last_id_col.value(last_chunk.num_rows() - 1), "PART1_0499");

        Ok(())
    }

    #[tokio::test]
    async fn test_execute_exo_scan_native_sub_64bit_types() -> Result<()> {
        use arrow::array::{BooleanArray, Float32Array, Int8Array, Int16Array, Int32Array};

        let dir = tempdir().map_err(|e| VeinError::internal(e.to_string()))?;
        const TOTAL_ROWS: usize = 100;

        let col_i32_vals: Vec<i32> = (0..TOTAL_ROWS as i32).map(|i| i * 10 - 500).collect();
        let col_f32_vals: Vec<f32> = (0..TOTAL_ROWS).map(|i| (i as f32) * 1.5 - 25.0).collect();
        let col_i16_vals: Vec<i16> = (0..TOTAL_ROWS as i16).map(|i| i * 2 - 100).collect();
        let col_i8_vals: Vec<i8> = (0..TOTAL_ROWS as i8).map(|i| i - 50).collect();
        let col_bool_vals: Vec<bool> = (0..TOTAL_ROWS).map(|i| i % 2 == 0).collect();

        {
            let mut writer = PartWriter::create(dir.path(), 0, 64)
                .map_err(|e| VeinError::internal(e.to_string()))?;
            writer.write_i32_column("col_i32", &col_i32_vals)
                .map_err(|e| VeinError::internal(e.to_string()))?;
            writer.write_f32_column("col_f32", &col_f32_vals)
                .map_err(|e| VeinError::internal(e.to_string()))?;
            writer.write_i16_column("col_i16", &col_i16_vals)
                .map_err(|e| VeinError::internal(e.to_string()))?;
            writer.write_i8_column("col_i8", &col_i8_vals)
                .map_err(|e| VeinError::internal(e.to_string()))?;
            writer.write_bool_column("col_bool", &col_bool_vals)
                .map_err(|e| VeinError::internal(e.to_string()))?;
            writer.finish().map_err(|e| VeinError::internal(e.to_string()))?;
        }

        let proj = vec![
            FieldSchema { field_id: 0, name: "col_i32".into(), data_type: "Int32".into() },
            FieldSchema { field_id: 1, name: "col_f32".into(), data_type: "Float32".into() },
            FieldSchema { field_id: 2, name: "col_i16".into(), data_type: "Int16".into() },
            FieldSchema { field_id: 3, name: "col_i8".into(), data_type: "Int8".into() },
            FieldSchema { field_id: 4, name: "col_bool".into(), data_type: "Boolean".into() },
        ];

        let mut collector = CollectTestNode::new();
        let table_path_str = dir.path().to_string_lossy().to_string();
        execute_exo_scan(&[table_path_str], Some(&proj), &mut collector).await?;

        assert_eq!(collector.chunks.len(), 1);
        let chunk = &collector.chunks[0];
        assert_eq!(chunk.num_rows(), TOTAL_ROWS);

        let i32_col = chunk.column(0).as_any().downcast_ref::<Int32Array>().expect("downcast Int32Array");
        let f32_col = chunk.column(1).as_any().downcast_ref::<Float32Array>().expect("downcast Float32Array");
        let i16_col = chunk.column(2).as_any().downcast_ref::<Int16Array>().expect("downcast Int16Array");
        let i8_col = chunk.column(3).as_any().downcast_ref::<Int8Array>().expect("downcast Int8Array");
        let bool_col = chunk.column(4).as_any().downcast_ref::<BooleanArray>().expect("downcast BooleanArray");

        for i in 0..TOTAL_ROWS {
            assert_eq!(i32_col.value(i), col_i32_vals[i]);
            assert_eq!(f32_col.value(i), col_f32_vals[i]);
            assert_eq!(i16_col.value(i), col_i16_vals[i]);
            assert_eq!(i8_col.value(i), col_i8_vals[i]);
            assert_eq!(bool_col.value(i), col_bool_vals[i]);
        }

        Ok(())
    }
}
