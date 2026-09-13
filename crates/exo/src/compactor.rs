//! MergeTree Compactor (`compactor.rs`) physical implementation.
//!
//! Merges multiple immutable Parts into a unified, defragmented Part.
//! Purges tombstoned (deleted or superseded) slots from disk, reclaiming
//! physical NVMe storage and re-indexing slot IDs into contiguous 0..M-1 space.
//!
//! Enforces strict zero-unwrap policy and multi-column row consistency.

use std::path::Path;
use std::time::Instant;

use crate::error::{ExoError, ExoResult};
use crate::part::{PartDescriptor, PartReader, PartWriter};
use crate::traits::{AddressReader, VaultReader};

/// Summary report returned upon successful compaction.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CompactionStats {
    /// Number of source parts merged.
    pub source_parts_count: usize,
    /// Total rows scanned from source parts before filtering.
    pub input_rows: u64,
    /// Total tombstoned rows purged from disk.
    pub tombstoned_rows_purged: u64,
    /// Total active rows written to the consolidated merged Part.
    pub output_rows: u64,
    /// Wall-clock time elapsed during the compaction pass.
    pub elapsed: std::time::Duration,
}

/// The MergeTree Compactor engine.
pub struct PartCompactor;

impl PartCompactor {
    /// Consolidates multiple source parts into a new immutable merged Part.
    ///
    /// Purges all tombstoned slots and re-packs all column arrays contiguously.
    /// Operates directly on canonical addresses and raw byte payloads with zero UTF-8 assumptions.
    pub fn compact(
        target_parent_dir: &Path,
        merged_part_id: u32,
        granule_size: usize,
        source_parts: &[PartReader],
    ) -> ExoResult<(PartDescriptor, CompactionStats)> {
        let start_time = Instant::now();

        if source_parts.is_empty() {
            return Err(ExoError::CodecError(
                "Cannot perform compaction with an empty list of source parts".to_string(),
            ));
        }

        // 1. Validate that all participating parts share the exact same column schema
        let canonical_columns = source_parts[0].column_names();
        for part in source_parts.iter().skip(1) {
            if part.column_names() != canonical_columns {
                return Err(ExoError::CodecError(format!(
                    "Cannot compact parts with schema divergence: Part {} has {:?}, but Part {} has {:?}",
                    source_parts[0].part_id(),
                    canonical_columns,
                    part.part_id(),
                    part.column_names()
                )));
            }
        }

        let mut input_rows = 0u64;
        let mut tombstoned_rows_purged = 0u64;

        for part in source_parts {
            input_rows += part.total_rows();
            tombstoned_rows_purged += part.tombstones().deleted_count();
        }

        let expected_output_rows = input_rows.saturating_sub(tombstoned_rows_purged);

        // 2. Initialize the new PartWriter
        let mut writer = PartWriter::create(target_parent_dir, merged_part_id, granule_size)?;

        // 3. Compact each column across all participating parts (binary-agnostic, zero UTF-8 assumptions)
        for col_name in canonical_columns {
            let mut active_vaddrs = Vec::with_capacity(expected_output_rows as usize);
            let mut active_payloads = Vec::with_capacity(expected_output_rows as usize);

            for part in source_parts {
                let tombstones = part.tombstones();
                let addr_reader = part.open_address_reader(col_name)?;
                let mut vault_reader = part.open_vault_reader(col_name)?;
                let total_rows = part.total_rows() as u32;

                let part_granule_size = addr_reader.granule_size();
                let mut granule_buf = vec![0u64; part_granule_size];

                for g in 0..addr_reader.total_granules() {
                    let count = addr_reader.read_granule(g, &mut granule_buf)?;
                    let g_start_slot = (g * part_granule_size) as u32;

                    for local_idx in 0..count {
                        let slot = g_start_slot + (local_idx as u32);
                        if slot >= total_rows {
                            break;
                        }
                        if tombstones.is_deleted(slot) {
                            continue; // Purged from disk!
                        }

                        let vaddr = granule_buf[local_idx];
                        let payload = vault_reader.read_opt_bytes_slot(slot)?.map(|b| b.to_vec());
                        active_vaddrs.push(vaddr);
                        active_payloads.push(payload);
                    }
                }
            }

            writer.write_raw_column(col_name, &active_vaddrs, &active_payloads)?;
        }

        // 4. Atomically commit the compacted Part
        let descriptor = writer.finish()?;

        let stats = CompactionStats {
            source_parts_count: source_parts.len(),
            input_rows,
            tombstoned_rows_purged,
            output_rows: descriptor.total_rows,
            elapsed: start_time.elapsed(),
        };

        Ok((descriptor, stats))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_part_compaction_lifecycle() -> ExoResult<()> {
        let dir = tempdir()?;
        const GRANULE_SIZE: usize = 256;

        // 1. Create Part 0 (1,000 orders)
        let part0_desc = {
            let mut writer = PartWriter::create(dir.path(), 0, GRANULE_SIZE)?;
            let ids: Vec<String> = (0..1000).map(|i| format!("ORD_{:05}", i)).collect();
            let amounts: Vec<String> = (0..1000).map(|i| format!("{:.2}", 50.0 + (i as f64))).collect();
            writer.write_string_column("order_id", &ids)?;
            writer.write_string_column("amount", &amounts)?;
            writer.finish()?
        };

        // 2. Create Part 1 (500 orders)
        let part1_desc = {
            let mut writer = PartWriter::create(dir.path(), 1, GRANULE_SIZE)?;
            let ids: Vec<String> = (1000..1500).map(|i| format!("ORD_{:05}", i)).collect();
            let amounts: Vec<String> = (1000..1500).map(|i| format!("{:.2}", 100.0 + (i as f64))).collect();
            writer.write_string_column("order_id", &ids)?;
            writer.write_string_column("amount", &amounts)?;
            writer.finish()?
        };

        // 3. Open PartReaders and apply tombstones
        let mut part0_reader = PartReader::open(&part0_desc.path, 0)?;
        let mut part1_reader = PartReader::open(&part1_desc.path, 1)?;

        // Delete 100 rows in Part 0 (slots 50..150)
        part0_reader.tombstones_mut().mark_range_deleted(50..150);
        part0_reader.save_tombstones()?;

        // Delete 50 rows in Part 1 (slots 10..60)
        part1_reader.tombstones_mut().mark_range_deleted(10..60);
        part1_reader.save_tombstones()?;

        assert_eq!(part0_reader.active_rows(), 900);
        assert_eq!(part1_reader.active_rows(), 450);

        // 4. Run Compaction
        let source_parts = [part0_reader, part1_reader];
        let (merged_desc, stats) = PartCompactor::compact(dir.path(), 999, GRANULE_SIZE, &source_parts)?;

        assert_eq!(stats.source_parts_count, 2);
        assert_eq!(stats.input_rows, 1500);
        assert_eq!(stats.tombstoned_rows_purged, 150);
        assert_eq!(stats.output_rows, 1350);
        assert_eq!(merged_desc.total_rows, 1350);

        // 5. Verify the merged Part contents
        let merged_reader = PartReader::open(&merged_desc.path, 999)?;
        assert_eq!(merged_reader.total_rows(), 1350);
        assert_eq!(merged_reader.active_rows(), 1350);
        assert_eq!(merged_reader.tombstones().deleted_count(), 0);

        let mut id_vault = merged_reader.open_vault_reader("order_id")?;
        let mut amt_vault = merged_reader.open_vault_reader("amount")?;

        // Slot 0 should be ORD_00000
        assert_eq!(id_vault.read_str_slot(0)?, "ORD_00000");
        assert_eq!(amt_vault.read_str_slot(0)?, "50.00");

        // Slot 49 should be ORD_00049
        assert_eq!(id_vault.read_str_slot(49)?, "ORD_00049");

        // Slot 50 should now be ORD_00150 (since 50..150 were deleted!)
        assert_eq!(id_vault.read_str_slot(50)?, "ORD_00150");
        assert_eq!(amt_vault.read_str_slot(50)?, "200.00");

        // Total rows exactly 1350
        assert_eq!(id_vault.total_rows(), 1350);

        Ok(())
    }

    #[test]
    fn test_compaction_with_arbitrary_binary_blobs_and_sql_nulls() -> ExoResult<()> {
        let dir = tempdir()?;
        const GRANULE_SIZE: usize = 64;

        // Arbitrary non-UTF8 binary blobs: 0xFF, 0x00, 0xFE are invalid UTF-8 byte sequences
        let blob0: Vec<Vec<u8>> = (0..200)
            .map(|i| vec![0xFF, 0xFE, 0x00, (i & 0xFF) as u8, ((i >> 8) & 0xFF) as u8])
            .collect();
        let nullables0: Vec<Option<String>> = (0..200)
            .map(|i| if i % 4 == 0 { None } else { Some(format!("Val_{}", i)) })
            .collect();

        let part0_desc = {
            let mut writer = PartWriter::create(dir.path(), 10, GRANULE_SIZE)?;
            writer.write_bytes_column("embedding_blob", &blob0)?;
            writer.write_opt_string_column("nullable_tag", &nullables0)?;
            writer.finish()?
        };

        let blob1: Vec<Vec<u8>> = (200..300)
            .map(|i| vec![0xFF, 0xFE, 0x00, (i & 0xFF) as u8, ((i >> 8) & 0xFF) as u8])
            .collect();
        let nullables1: Vec<Option<String>> = (200..300)
            .map(|i| if i % 4 == 0 { None } else { Some(format!("Val_{}", i)) })
            .collect();

        let part1_desc = {
            let mut writer = PartWriter::create(dir.path(), 11, GRANULE_SIZE)?;
            writer.write_bytes_column("embedding_blob", &blob1)?;
            writer.write_opt_string_column("nullable_tag", &nullables1)?;
            writer.finish()?
        };

        // Tombstone slots in Part 0 (delete slots 50..70 = 20 rows)
        let mut part0_reader = PartReader::open(&part0_desc.path, 10)?;
        part0_reader.tombstones_mut().mark_range_deleted(50..70);
        part0_reader.save_tombstones()?;

        let part1_reader = PartReader::open(&part1_desc.path, 11)?;

        // Compact both parts
        let source_parts = [part0_reader, part1_reader];
        let (merged_desc, stats) = PartCompactor::compact(dir.path(), 888, GRANULE_SIZE, &source_parts)?;

        assert_eq!(stats.input_rows, 300);
        assert_eq!(stats.tombstoned_rows_purged, 20);
        assert_eq!(stats.output_rows, 280);

        let merged_reader = PartReader::open(&merged_desc.path, 888)?;
        let mut blob_vault = merged_reader.open_vault_reader("embedding_blob")?;
        let mut tag_vault = merged_reader.open_vault_reader("nullable_tag")?;

        // Slot 0 check
        assert_eq!(blob_vault.read_bytes_slot(0)?, &blob0[0][..]);
        assert!(tag_vault.is_null(0)?); // 0 % 4 == 0 -> None

        // Slot 1 check
        assert_eq!(blob_vault.read_bytes_slot(1)?, &blob0[1][..]);
        assert!(!tag_vault.is_null(1)?);
        assert_eq!(tag_vault.read_opt_str_slot(1)?, Some("Val_1"));

        // Slot 50 in merged corresponds to index 70 from part 0
        assert_eq!(blob_vault.read_bytes_slot(50)?, &blob0[70][..]);

        Ok(())
    }
}
