//! Vectorized scan operator for Vein Dual-Plane (.exo) storage format.
//!
//! Implements two-phase execution:
//! 1. Address Plane & Tombstone Scan: reads `.exo.tomb` to filter deleted rows,
//!    generating a dense or sparse active row index set.
//! 2. Late Materialization: only dereferences and materializes values for
//!    projected columns and surviving row slots directly into Arrow arrays.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use arrow::array::{ArrayRef, BinaryBuilder, StringBuilder};
use common::Result;
use error::VeinError;
use exo::traits::VaultReader;
use exo::{ExoVaultReader, PartReader};

use crate::pipeline::{DataChunk, PipelineNode};

/// Default batch size for vectorized pipeline execution (rows per DataChunk).
pub const DEFAULT_EXO_VECTOR_SIZE: usize = 2048;

/// Executes a vectorized scan over a list of .exo partition paths.
#[tracing::instrument(skip_all)]
pub async fn execute_exo_scan(
    paths: &[String],
    projection: Option<&[String]>,
    next: &mut dyn PipelineNode<DataChunk>,
) -> Result<()> {
    for path_str in paths {
        let path = Path::new(path_str);
        let part_dir = resolve_part_dir(path)?;
        let part_id = extract_part_id(&part_dir);

        tracing::info!(
            path = %path_str,
            part_id = part_id,
            "Scanning .exo columnar partition"
        );

        let part_reader = PartReader::open(&part_dir, part_id)
            .map_err(|e| VeinError::internal(format!("Failed to open .exo part at {:?}: {}", part_dir, e)))?;

        let total_rows = part_reader.total_rows();
        if total_rows == 0 {
            continue;
        }

        // 1. Tombstone Mask Filtering: build active surviving row indices
        let tombstones = part_reader.tombstones();
        let active_indices: Vec<u32> = (0..(total_rows as u32))
            .filter(|&row_id| !tombstones.is_deleted(row_id))
            .collect();

        if active_indices.is_empty() {
            tracing::info!(part_id = part_id, "All rows tombstoned, skipping part");
            continue;
        }

        // 2. Determine Projected Columns
        let available_cols = part_reader.column_names();
        let target_columns: Vec<String> = match projection {
            Some(proj) => {
                for col in proj {
                    if !available_cols.contains(col) {
                        return Err(VeinError::internal(format!(
                            "Projected column '{}' not found in .exo part {:?}. Available columns: {:?}",
                            col, part_dir, available_cols
                        )));
                    }
                }
                proj.to_vec()
            }
            None => available_cols.to_vec(),
        };

        // 3. Vectorized Chunking & Late Materialization
        let mut rows_processed = 0;
        for chunk_slots in active_indices.chunks(DEFAULT_EXO_VECTOR_SIZE) {
            let mut vectors: Vec<ArrayRef> = Vec::with_capacity(target_columns.len());

            for col_name in &target_columns {
                let mut vault_reader = part_reader
                    .open_vault_reader(col_name)
                    .map_err(|e| VeinError::internal(format!("Failed to open vault for column '{}': {}", col_name, e)))?;

                let array = materialize_column_array(&mut vault_reader, chunk_slots, col_name)?;
                vectors.push(array);
            }

            rows_processed += chunk_slots.len();
            let chunk = DataChunk::from_vectors(vectors);
            next.push(chunk)?;
        }

        tracing::info!(
            part_id = part_id,
            surviving_rows = rows_processed,
            total_rows = total_rows,
            "Completed scanning .exo partition"
        );
    }

    Ok(())
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

/// Resolves the directory of the partition.
fn resolve_part_dir(path: &Path) -> Result<PathBuf> {
    if path.is_dir() {
        Ok(path.to_path_buf())
    } else if let Some(parent) = path.parent() {
        if parent.as_os_str().is_empty() {
            Ok(PathBuf::from("."))
        } else {
            Ok(parent.to_path_buf())
        }
    } else {
        Err(VeinError::internal(format!(
            "Cannot resolve parent partition directory for path: {:?}",
            path
        )))
    }
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
        let proj = vec!["id".to_string(), "status".to_string()];
        execute_exo_scan(&[part_path_str], Some(&proj), &mut proj_collector).await?;

        assert_eq!(proj_collector.chunks.len(), 1);
        let proj_chunk = &proj_collector.chunks[0];
        assert_eq!(proj_chunk.num_columns(), 2);
        assert_eq!(proj_chunk.num_rows(), TOTAL_ROWS - 2);

        Ok(())
    }
}
