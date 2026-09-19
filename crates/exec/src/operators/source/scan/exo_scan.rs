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
    ArrayRef, BinaryBuilder, BooleanBuilder, Date32Builder, Float32Builder, Float64Builder,
    Int8Builder, Int16Builder, Int32Builder, Int64Builder, StringBuilder, UInt8Builder,
    UInt16Builder, UInt32Builder, UInt64Builder,
};
use common::protocol::FieldSchema;
use common::Result;
use error::VeinError;
use exo::traits::{AddressReader, VaultReader};
use exo::{
    f32_from_canonical_native, f64_from_canonical, i8_from_canonical_native,
    i16_from_canonical_native, i32_from_canonical_native, i64_from_canonical, ExoAddressReader,
    ExoVaultReader, PartReader,
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

/// Executes a vectorized scan over a list of .exo partition paths.
#[tracing::instrument(skip_all)]
pub async fn execute_exo_scan(
    paths: &[String],
    projection: Option<&[FieldSchema]>,
    next: &mut dyn PipelineNode<DataChunk>,
) -> Result<()> {
    let vfs_store = vfs::VfsTableStore::shared_or_default().ok();

    for path_str in paths {
        // 1. Check if this is a native VFS table or URI (e.g. "vfs://tbl_1", "vfs://my_table", or table name)
        let is_vfs_candidate = path_str.starts_with("vfs://") || !Path::new(path_str).exists();
        if is_vfs_candidate {
            if let Some(store) = &vfs_store {
                let table_id = vfs::VfsTableStore::resolve_table_id(path_str);
                let explicit_part = vfs::VfsTableStore::extract_part_id_from_uri(path_str);
                let part_ids = match explicit_part {
                    Some(pid) => vec![pid],
                    None => {
                        let parts = store.resolve_table_parts(table_id).await.unwrap_or_default();
                        if parts.is_empty() {
                            vec![0]
                        } else {
                            parts
                        }
                    }
                };

                let mut scanned_any = false;
                for part_id in part_ids {
                    if let Ok(part_reader) = store.open_partition_reader(table_id, part_id).await {
                        tracing::info!(
                            table_id = table_id,
                            part_id = part_id,
                            rows = part_reader.total_rows(),
                            "Scanning native VFS memory-mapped block partition"
                        );
                        scan_part_reader(&part_reader, projection, next)?;
                        scanned_any = true;
                    }
                }
                if scanned_any {
                    continue;
                }
            }
        }

        // 2. Legacy filesystem path scan (for filesystem folders containing .exo.addr files)
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

            scan_part_reader(&part_reader, projection, next)?;
        }
    }

    Ok(())
}

/// Executes vectorized late-materialization scanning over a single [`PartReader`].
pub fn scan_part_reader(
    part_reader: &PartReader,
    projection: Option<&[FieldSchema]>,
    next: &mut dyn PipelineNode<DataChunk>,
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
    for (col_name, data_type) in &target_fields {
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

    // 4. Bounded Vectorized Streaming (O(1) memory) & Late Materialization
    let mut chunk_slots: Vec<u32> = Vec::with_capacity(DEFAULT_EXO_VECTOR_SIZE);
    let mut rows_processed = 0;
    let total_u32 = total_rows.min(u32::MAX as u64) as u32;

    for row_id in 0..total_u32 {
        if !tombstones.is_deleted(row_id) {
            chunk_slots.push(row_id);
            if chunk_slots.len() >= DEFAULT_EXO_VECTOR_SIZE {
                let mut vectors: Vec<ArrayRef> = Vec::with_capacity(column_scanners.len());
                for (col_name, data_type, scanner) in &mut column_scanners {
                    let array = materialize_scanner_array(scanner, &chunk_slots, col_name, data_type.as_deref())?;
                    vectors.push(array);
                }
                rows_processed += chunk_slots.len();
                let chunk = DataChunk::from_vectors(vectors);
                next.push(chunk)?;
                chunk_slots.clear();
            }
        }
    }

    // Flush trailing chunk
    if !chunk_slots.is_empty() {
        let mut vectors: Vec<ArrayRef> = Vec::with_capacity(column_scanners.len());
        for (col_name, data_type, scanner) in &mut column_scanners {
            let array = materialize_scanner_array(scanner, &chunk_slots, col_name, data_type.as_deref())?;
            vectors.push(array);
        }
        rows_processed += chunk_slots.len();
        let chunk = DataChunk::from_vectors(vectors);
        next.push(chunk)?;
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

/// Helper to materialize a batch of slots from either a Payload Vault or an inlined Address Plane.
fn materialize_scanner_array(
    scanner: &mut ColumnScanner,
    slots: &[u32],
    col_name: &str,
    data_type: Option<&str>,
) -> Result<ArrayRef> {
    match scanner {
        ColumnScanner::Vault(vault_reader) => {
            materialize_column_array(vault_reader, slots, col_name)
        }
        ColumnScanner::Address(addr_reader) => {
            let slot_width = addr_reader.slot_width();
            match data_type {
                Some("Float64") => {
                    let mut float_builder = Float64Builder::with_capacity(slots.len());
                    for &slot in slots {
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
) -> Result<ArrayRef> {
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
