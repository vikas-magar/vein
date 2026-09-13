//! Multi-Column Part Engine (`part_{part_id}/`).
//!
//! Coordinates all column files (`.exo.addr`, `.exo.vault`, `tombstones.exo.tomb`)
//! that belong to an immutable table partition.
//!
//! Enforces the fundamental columnar invariant:
//! All columns in a Part must have the exact same total row count.
//! Strict zero-unwrap policy is enforced.

use std::collections::HashSet;
use std::fs::{create_dir_all, read_dir, rename};
use std::path::{Path, PathBuf};

use crate::addr::{ExoAddressReader, ExoAddressWriter};
use crate::coord::ToCanonicalAddress;
use crate::error::{ExoError, ExoResult};
use crate::tombstone::TombstoneMask;
use crate::traits::{AddressReader, AddressWriter, VaultWriter};
use crate::vault::{ExoVaultReader, ExoVaultWriter};

/// Metadata descriptor for an immutable Part.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PartDescriptor {
    /// Unique identifier of this Part.
    pub part_id: u32,
    /// Directory path on the filesystem.
    pub path: PathBuf,
    /// Total rows in this Part.
    pub total_rows: u64,
    /// Names of all columns present in this Part.
    pub column_names: Vec<String>,
    /// Configured granule size for this Part.
    pub granule_size: usize,
}

/// Writer for constructing a new immutable Part directory.
pub struct PartWriter {
    parent_dir: PathBuf,
    part_id: u32,
    tmp_path: PathBuf,
    final_path: PathBuf,
    granule_size: usize,
    total_rows: Option<u64>,
    columns_written: HashSet<String>,
}

impl PartWriter {
    /// Initializes a new Part writer, creating a temporary working directory.
    pub fn create(parent_dir: &Path, part_id: u32, granule_size: usize) -> ExoResult<Self> {
        let tmp_name = format!(".tmp_part_{:06}", part_id);
        let final_name = format!("part_{:06}", part_id);

        let tmp_path = parent_dir.join(tmp_name);
        let final_path = parent_dir.join(final_name);

        if tmp_path.exists() {
            let _ = std::fs::remove_dir_all(&tmp_path);
        }
        create_dir_all(&tmp_path)?;

        Ok(Self {
            parent_dir: parent_dir.to_path_buf(),
            part_id,
            tmp_path,
            final_path,
            granule_size,
            total_rows: None,
            columns_written: HashSet::new(),
        })
    }

    /// Returns the parent directory containing parts for this table.
    pub fn parent_dir(&self) -> &Path {
        &self.parent_dir
    }

    /// Writes both `.exo.addr` and `.exo.vault` for a string column.
    ///
    /// Validates that the row count matches all previously written columns in this Part.
    pub fn write_string_column(
        &mut self,
        col_name: &str,
        values: &[String],
    ) -> ExoResult<()> {
        let row_count = values.len() as u64;

        // Invariant check: verify all columns have identical row counts
        if let Some(expected) = self.total_rows {
            if row_count != expected {
                return Err(ExoError::CodecError(format!(
                    "Column row count mismatch in part {}: expected {} rows, but column '{}' has {} rows",
                    self.part_id, expected, col_name, row_count
                )));
            }
        } else {
            self.total_rows = Some(row_count);
        }

        let addr_path = self.tmp_path.join(format!("{col_name}.exo.addr"));
        let vault_path = self.tmp_path.join(format!("{col_name}.exo.vault"));

        // 1. Write Plane 1: .exo.addr
        let mut addr_writer = ExoAddressWriter::create(&addr_path, self.granule_size)?;
        for s in values {
            let vaddr = s.as_str().to_canonical();
            addr_writer.append_vaddr(vaddr)?;
        }
        addr_writer.finish()?;

        // 2. Write Plane 2: .exo.vault
        let mut vault_writer = ExoVaultWriter::create(&vault_path, self.granule_size)?;
        vault_writer.append_batch_strings(values)?;
        vault_writer.finish()?;

        self.columns_written.insert(col_name.to_string());
        Ok(())
    }

    /// Writes both `.exo.addr` and `.exo.vault` for an optional string column (supporting SQL NULLs).
    pub fn write_opt_string_column(
        &mut self,
        col_name: &str,
        values: &[Option<String>],
    ) -> ExoResult<()> {
        let row_count = values.len() as u64;

        if let Some(expected) = self.total_rows {
            if row_count != expected {
                return Err(ExoError::CodecError(format!(
                    "Column row count mismatch in part {}: expected {} rows, but column '{}' has {} rows",
                    self.part_id, expected, col_name, row_count
                )));
            }
        } else {
            self.total_rows = Some(row_count);
        }

        let addr_path = self.tmp_path.join(format!("{col_name}.exo.addr"));
        let vault_path = self.tmp_path.join(format!("{col_name}.exo.vault"));

        let mut addr_writer = ExoAddressWriter::create(&addr_path, self.granule_size)?;
        for s in values {
            let vaddr = match s {
                Some(str_val) => str_val.as_str().to_canonical(),
                None => crate::coord::NULL_VADDR,
            };
            addr_writer.append_vaddr(vaddr)?;
        }
        addr_writer.finish()?;

        let mut vault_writer = ExoVaultWriter::create(&vault_path, self.granule_size)?;
        vault_writer.append_batch_opt_strings(values)?;
        vault_writer.finish()?;

        self.columns_written.insert(col_name.to_string());
        Ok(())
    }

    /// Writes a binary blob column without UTF-8 constraints.
    pub fn write_bytes_column(
        &mut self,
        col_name: &str,
        values: &[Vec<u8>],
    ) -> ExoResult<()> {
        let row_count = values.len() as u64;

        if let Some(expected) = self.total_rows {
            if row_count != expected {
                return Err(ExoError::CodecError(format!(
                    "Column row count mismatch in part {}: expected {} rows, but column '{}' has {} rows",
                    self.part_id, expected, col_name, row_count
                )));
            }
        } else {
            self.total_rows = Some(row_count);
        }

        let addr_path = self.tmp_path.join(format!("{col_name}.exo.addr"));
        let vault_path = self.tmp_path.join(format!("{col_name}.exo.vault"));

        let mut addr_writer = ExoAddressWriter::create(&addr_path, self.granule_size)?;
        for b in values {
            addr_writer.append_vaddr(b.as_slice().to_canonical())?;
        }
        addr_writer.finish()?;

        let mut vault_writer = ExoVaultWriter::create(&vault_path, self.granule_size)?;
        for b in values {
            vault_writer.append_bytes(b)?;
        }
        vault_writer.finish()?;

        self.columns_written.insert(col_name.to_string());
        Ok(())
    }

    /// Writes a column directly from pre-computed canonical addresses and optional byte payloads.
    ///
    /// Ideal for compaction, query materialization, and binary payloads where re-hashing
    /// is unnecessary and UTF-8 assumptions must not be made.
    pub fn write_raw_column(
        &mut self,
        col_name: &str,
        vaddrs: &[u64],
        payloads: &[Option<Vec<u8>>],
    ) -> ExoResult<()> {
        if vaddrs.len() != payloads.len() {
            return Err(ExoError::CodecError(format!(
                "write_raw_column mismatch: {} vaddrs != {} payloads",
                vaddrs.len(),
                payloads.len()
            )));
        }

        let row_count = vaddrs.len() as u64;
        if let Some(expected) = self.total_rows {
            if row_count != expected {
                return Err(ExoError::CodecError(format!(
                    "Column row count mismatch in part {}: expected {} rows, but column '{}' has {} rows",
                    self.part_id, expected, col_name, row_count
                )));
            }
        } else {
            self.total_rows = Some(row_count);
        }

        let addr_path = self.tmp_path.join(format!("{col_name}.exo.addr"));
        let vault_path = self.tmp_path.join(format!("{col_name}.exo.vault"));

        let mut addr_writer = ExoAddressWriter::create(&addr_path, self.granule_size)?;
        addr_writer.append_batch(vaddrs)?;
        addr_writer.finish()?;

        let mut vault_writer = ExoVaultWriter::create(&vault_path, self.granule_size)?;
        for p in payloads {
            vault_writer.append_opt_bytes(p.as_deref())?;
        }
        vault_writer.finish()?;

        self.columns_written.insert(col_name.to_string());
        Ok(())
    }

    /// Writes a generic typed column implementing `ToCanonicalAddress` and `ToString`.
    pub fn write_typed_column<T: ToCanonicalAddress + ToString>(
        &mut self,
        col_name: &str,
        values: &[T],
    ) -> ExoResult<()> {
        let string_values: Vec<String> = values.iter().map(|v| v.to_string()).collect();
        let row_count = values.len() as u64;

        if let Some(expected) = self.total_rows {
            if row_count != expected {
                return Err(ExoError::CodecError(format!(
                    "Column row count mismatch in part {}: expected {} rows, but column '{}' has {} rows",
                    self.part_id, expected, col_name, row_count
                )));
            }
        } else {
            self.total_rows = Some(row_count);
        }

        let addr_path = self.tmp_path.join(format!("{col_name}.exo.addr"));
        let vault_path = self.tmp_path.join(format!("{col_name}.exo.vault"));

        // Write .exo.addr
        let mut addr_writer = ExoAddressWriter::create(&addr_path, self.granule_size)?;
        for val in values {
            addr_writer.append_vaddr(val.to_canonical())?;
        }
        addr_writer.finish()?;

        // Write .exo.vault
        let mut vault_writer = ExoVaultWriter::create(&vault_path, self.granule_size)?;
        vault_writer.append_batch_strings(&string_values)?;
        vault_writer.finish()?;

        self.columns_written.insert(col_name.to_string());
        Ok(())
    }

    /// Commits the Part by atomically renaming the temporary directory to the final part directory.
    pub fn finish(self) -> ExoResult<PartDescriptor> {
        if self.columns_written.is_empty() {
            return Err(ExoError::CodecError(
                "Cannot commit an empty Part with zero columns".to_string(),
            ));
        }

        if self.final_path.exists() {
            let _ = std::fs::remove_dir_all(&self.final_path);
        }

        // Atomic directory rename guarantees all-or-nothing part creation
        rename(&self.tmp_path, &self.final_path)?;

        let mut column_names: Vec<String> = self.columns_written.into_iter().collect();
        column_names.sort();

        Ok(PartDescriptor {
            part_id: self.part_id,
            path: self.final_path,
            total_rows: self.total_rows.unwrap_or(0),
            column_names,
            granule_size: self.granule_size,
        })
    }
}

/// Reader for accessing columns and tombstones of an existing Part.
pub struct PartReader {
    part_id: u32,
    path: PathBuf,
    total_rows: u64,
    column_names: Vec<String>,
    tombstones: TombstoneMask,
}

impl PartReader {
    /// Opens an existing Part directory and discovers its columns and tombstones.
    pub fn open(path: &Path, part_id: u32) -> ExoResult<Self> {
        if !path.exists() || !path.is_dir() {
            return Err(ExoError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("Part directory {:?} does not exist", path),
            )));
        }

        // 1. Discover all columns by scanning for .exo.addr files
        let mut column_names = Vec::new();
        for entry in read_dir(path)? {
            let entry = entry?;
            let file_name = entry.file_name().to_string_lossy().to_string();
            if file_name.ends_with(".exo.addr") {
                let col_name = file_name.trim_end_matches(".exo.addr").to_string();
                column_names.push(col_name);
            }
        }
        column_names.sort();

        if column_names.is_empty() {
            return Err(ExoError::CodecError(format!(
                "Part {:?} contains no .exo.addr column files",
                path
            )));
        }

        // 2. Read first column header to determine total_rows
        let first_col_addr = path.join(format!("{}.exo.addr", column_names[0]));
        let first_reader = ExoAddressReader::open(&first_col_addr)?;
        let total_rows = first_reader.total_rows();

        // 3. Load tombstones
        let tomb_path = path.join("tombstones.exo.tomb");
        let tombstones = TombstoneMask::load(&tomb_path)?;

        Ok(Self {
            part_id,
            path: path.to_path_buf(),
            total_rows,
            column_names,
            tombstones,
        })
    }

    #[inline(always)]
    pub fn part_id(&self) -> u32 {
        self.part_id
    }

    #[inline(always)]
    pub fn path(&self) -> &Path {
        &self.path
    }

    #[inline(always)]
    pub fn total_rows(&self) -> u64 {
        self.total_rows
    }

    /// Number of active (non-deleted) rows in this Part.
    #[inline(always)]
    pub fn active_rows(&self) -> u64 {
        self.total_rows.saturating_sub(self.tombstones.deleted_count())
    }

    #[inline(always)]
    pub fn column_names(&self) -> &[String] {
        &self.column_names
    }

    #[inline(always)]
    pub fn tombstones(&self) -> &TombstoneMask {
        &self.tombstones
    }

    #[inline(always)]
    pub fn tombstones_mut(&mut self) -> &mut TombstoneMask {
        &mut self.tombstones
    }

    /// Saves updated tombstones to disk on the Address Plane.
    pub fn save_tombstones(&self) -> ExoResult<usize> {
        let tomb_path = self.path.join("tombstones.exo.tomb");
        self.tombstones.save(&tomb_path)
    }

    /// Opens an `ExoAddressReader` for the specified column in this Part.
    pub fn open_address_reader(&self, col_name: &str) -> ExoResult<ExoAddressReader> {
        let addr_path = self.path.join(format!("{col_name}.exo.addr"));
        ExoAddressReader::open(&addr_path)
    }

    /// Opens an `ExoVaultReader` for the specified column in this Part.
    pub fn open_vault_reader(&self, col_name: &str) -> ExoResult<ExoVaultReader> {
        let vault_path = self.path.join(format!("{col_name}.exo.vault"));
        ExoVaultReader::open(&vault_path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::traits::VaultReader;
    use tempfile::tempdir;

    #[test]
    fn test_multi_column_part_writer_and_reader() -> ExoResult<()> {
        let dir = tempdir()?;
        const GRANULE_SIZE: usize = 512;
        const NUM_ROWS: usize = 1500;

        let ids: Vec<i64> = (0..NUM_ROWS as i64).collect();
        let codes: Vec<String> = (0..NUM_ROWS).map(|i| format!("CUST_{:05}", i % 100)).collect();
        let amounts: Vec<f64> = (0..NUM_ROWS).map(|i| 10.0 + (i as f64) * 0.5).collect();

        // 1. Write multi-column Part
        let descriptor = {
            let mut writer = PartWriter::create(dir.path(), 42, GRANULE_SIZE)?;
            writer.write_typed_column("order_id", &ids)?;
            writer.write_string_column("customer_code", &codes)?;
            writer.write_typed_column("amount", &amounts)?;
            writer.finish()?
        };

        assert_eq!(descriptor.part_id, 42);
        assert_eq!(descriptor.total_rows, NUM_ROWS as u64);
        assert_eq!(descriptor.column_names, vec!["amount", "customer_code", "order_id"]);

        // 2. Open PartReader
        let mut reader = PartReader::open(&descriptor.path, 42)?;
        assert_eq!(reader.part_id(), 42);
        assert_eq!(reader.total_rows(), NUM_ROWS as u64);
        assert_eq!(reader.active_rows(), NUM_ROWS as u64);

        // 3. Test reading columns
        let id_addr = reader.open_address_reader("order_id")?;
        let code_addr = reader.open_address_reader("customer_code")?;
        let mut code_vault = reader.open_vault_reader("customer_code")?;

        assert_eq!(id_addr.total_rows(), NUM_ROWS as u64);
        assert_eq!(code_addr.total_rows(), NUM_ROWS as u64);
        assert_eq!(code_vault.read_str_slot(0)?, "CUST_00000");
        assert_eq!(code_vault.read_str_slot(1499)?, "CUST_00099");

        // 4. Apply lightweight delete via Tombstones
        reader.tombstones_mut().mark_deleted(0);
        reader.tombstones_mut().mark_range_deleted(100..200); // 100 slots deleted
        reader.save_tombstones()?;

        assert_eq!(reader.active_rows(), (NUM_ROWS - 101) as u64);

        // Reload PartReader from disk
        let reloaded = PartReader::open(&descriptor.path, 42)?;
        assert_eq!(reloaded.active_rows(), (NUM_ROWS - 101) as u64);
        assert!(reloaded.tombstones().is_deleted(0));
        assert!(reloaded.tombstones().is_deleted(150));
        assert!(!reloaded.tombstones().is_deleted(1));

        Ok(())
    }

    #[test]
    fn test_part_writer_rejects_row_count_mismatches() -> ExoResult<()> {
        let dir = tempdir()?;
        let mut writer = PartWriter::create(dir.path(), 1, 512)?;

        let col1 = vec!["A".to_string(), "B".to_string(), "C".to_string()];
        let col2 = vec!["X".to_string(), "Y".to_string()]; // only 2 rows!

        writer.write_string_column("col1", &col1)?;

        match writer.write_string_column("col2", &col2) {
            Err(ExoError::CodecError(msg)) => {
                assert!(msg.contains("Column row count mismatch"));
            }
            res => panic!("Expected row count mismatch error, got: {:?}", res),
        }

        Ok(())
    }
}
