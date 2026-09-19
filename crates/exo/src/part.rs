//! Multi-Column Part Engine (`part_{part_id}/`).
//!
//! Coordinates all column files (`.exo.addr`, `.exo.vault`, `tombstones.exo.tomb`)
//! that belong to an immutable table partition.
//!
//! Enforces the fundamental columnar invariant:
//! All columns in a Part must have the exact same total row count.
//! Strict zero-unwrap policy is enforced.

use std::collections::HashSet;
use std::fs::{create_dir_all, read_dir, rename, File};
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

/// Target destination mode for PartWriter: either filesystem directory or in-memory buffers.
enum PartWriterMode {
    Disk {
        parent_dir: PathBuf,
        tmp_path: PathBuf,
        final_path: PathBuf,
    },
    Memory {
        addr_buffers: std::collections::HashMap<String, Vec<u8>>,
        vault_buffers: std::collections::HashMap<String, Vec<u8>>,
    },
}

/// Metadata and in-memory column buffers for an immutable Part constructed in RAM.
#[derive(Clone, Debug)]
pub struct PartBufferDescriptor {
    pub part_id: u32,
    pub total_rows: u64,
    pub column_names: Vec<String>,
    pub granule_size: usize,
    pub addr_buffers: std::collections::HashMap<String, Vec<u8>>,
    pub vault_buffers: std::collections::HashMap<String, Vec<u8>>,
}

/// Writer for constructing a new immutable Part directory on disk or in RAM.
pub struct PartWriter {
    mode: PartWriterMode,
    part_id: u32,
    granule_size: usize,
    total_rows: Option<u64>,
    columns_written: HashSet<String>,
}

impl PartWriter {
    /// Initializes a new Part writer on the filesystem, creating a temporary working directory.
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
            mode: PartWriterMode::Disk {
                parent_dir: parent_dir.to_path_buf(),
                tmp_path,
                final_path,
            },
            part_id,
            granule_size,
            total_rows: None,
            columns_written: HashSet::new(),
        })
    }

    /// Initializes a pure in-memory Part writer without touching the local filesystem.
    pub fn create_in_memory(part_id: u32, granule_size: usize) -> Self {
        Self {
            mode: PartWriterMode::Memory {
                addr_buffers: std::collections::HashMap::new(),
                vault_buffers: std::collections::HashMap::new(),
            },
            part_id,
            granule_size,
            total_rows: None,
            columns_written: HashSet::new(),
        }
    }

    /// Returns the parent directory containing parts for this table.
    pub fn parent_dir(&self) -> &Path {
        match &self.mode {
            PartWriterMode::Disk { parent_dir, .. } => parent_dir,
            PartWriterMode::Memory { .. } => Path::new(""),
        }
    }

    #[inline]
    fn check_or_set_total_rows(&mut self, col_name: &str, row_count: u64) -> ExoResult<()> {
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
        Ok(())
    }

    fn write_column_data(
        &mut self,
        col_name: &str,
        slot_width: u8,
        write_addr: impl FnOnce(&mut dyn AddressWriter) -> ExoResult<()>,
        write_vault: Option<impl FnOnce(&mut dyn VaultWriter) -> ExoResult<()>>,
    ) -> ExoResult<()> {
        match &mut self.mode {
            PartWriterMode::Disk { tmp_path, .. } => {
                let addr_path = tmp_path.join(format!("{col_name}.exo.addr"));
                let mut addr_writer = ExoAddressWriter::create_with_slot_width(&addr_path, self.granule_size, slot_width)?;
                write_addr(&mut addr_writer)?;
                addr_writer.finish()?;

                if let Some(write_vault_fn) = write_vault {
                    let vault_path = tmp_path.join(format!("{col_name}.exo.vault"));
                    let mut vault_writer = ExoVaultWriter::create(&vault_path, self.granule_size)?;
                    write_vault_fn(&mut vault_writer)?;
                    vault_writer.finish()?;
                }
            }
            PartWriterMode::Memory { addr_buffers, vault_buffers } => {
                let mut addr_writer = ExoAddressWriter::create_in_memory(self.granule_size, slot_width)?;
                write_addr(&mut addr_writer)?;
                let (_, addr_bytes) = addr_writer.finish_into_bytes()?;
                addr_buffers.insert(col_name.to_string(), addr_bytes);

                if let Some(write_vault_fn) = write_vault {
                    let mut vault_writer = ExoVaultWriter::create_in_memory(self.granule_size)?;
                    write_vault_fn(&mut vault_writer)?;
                    let (_, vault_bytes) = vault_writer.finish_into_bytes()?;
                    vault_buffers.insert(col_name.to_string(), vault_bytes);
                }
            }
        }
        self.columns_written.insert(col_name.to_string());
        Ok(())
    }

    /// Writes both `.exo.addr` and `.exo.vault` for a string column.
    pub fn write_string_column(
        &mut self,
        col_name: &str,
        values: &[String],
    ) -> ExoResult<()> {
        self.check_or_set_total_rows(col_name, values.len() as u64)?;
        self.write_column_data(
            col_name,
            8,
            |addr| {
                for s in values {
                    addr.append_vaddr(s.as_str().to_canonical())?;
                }
                Ok(())
            },
            Some(|vault: &mut dyn VaultWriter| {
                vault.append_batch_strings(values)
            }),
        )
    }

    /// Writes both `.exo.addr` and `.exo.vault` for an optional string column (supporting SQL NULLs).
    pub fn write_opt_string_column(
        &mut self,
        col_name: &str,
        values: &[Option<String>],
    ) -> ExoResult<()> {
        self.check_or_set_total_rows(col_name, values.len() as u64)?;
        self.write_column_data(
            col_name,
            8,
            |addr| {
                for s in values {
                    let vaddr = match s {
                        Some(str_val) => str_val.as_str().to_canonical(),
                        None => crate::coord::NULL_VADDR,
                    };
                    addr.append_vaddr(vaddr)?;
                }
                Ok(())
            },
            Some(|vault: &mut dyn VaultWriter| {
                vault.append_batch_opt_strings(values)
            }),
        )
    }

    /// Writes a binary blob column without UTF-8 constraints.
    pub fn write_bytes_column(
        &mut self,
        col_name: &str,
        values: &[Vec<u8>],
    ) -> ExoResult<()> {
        self.check_or_set_total_rows(col_name, values.len() as u64)?;
        self.write_column_data(
            col_name,
            8,
            |addr| {
                for b in values {
                    addr.append_vaddr(b.as_slice().to_canonical())?;
                }
                Ok(())
            },
            Some(|vault: &mut dyn VaultWriter| {
                for b in values {
                    vault.append_bytes(b)?;
                }
                Ok(())
            }),
        )
    }

    /// Writes a column directly from pre-computed canonical addresses and optional byte payloads.
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
        self.check_or_set_total_rows(col_name, vaddrs.len() as u64)?;
        self.write_column_data(
            col_name,
            8,
            |addr| addr.append_batch(vaddrs),
            Some(|vault: &mut dyn VaultWriter| {
                for p in payloads {
                    vault.append_opt_bytes(p.as_deref())?;
                }
                Ok(())
            }),
        )
    }

    /// Writes an address-only column directly from pre-computed canonical addresses with no vault.
    pub fn write_raw_address_column(
        &mut self,
        col_name: &str,
        vaddrs: &[u64],
    ) -> ExoResult<()> {
        self.write_raw_address_column_with_slot_width(col_name, vaddrs, 8)
    }

    /// Writes an address-only column with a specified slot width (1, 2, 4, or 8 bytes) and no vault.
    pub fn write_raw_address_column_with_slot_width(
        &mut self,
        col_name: &str,
        vaddrs: &[u64],
        slot_width: u8,
    ) -> ExoResult<()> {
        self.check_or_set_total_rows(col_name, vaddrs.len() as u64)?;
        self.write_column_data(
            col_name,
            slot_width,
            |addr| addr.append_batch(vaddrs),
            None::<fn(&mut dyn VaultWriter) -> ExoResult<()>>,
        )
    }

    /// Writes a generic typed column implementing `ToCanonicalAddress` and `ToString`.
    pub fn write_typed_column<T: ToCanonicalAddress + ToString>(
        &mut self,
        col_name: &str,
        values: &[T],
    ) -> ExoResult<()> {
        let string_values: Vec<String> = values.iter().map(|v| v.to_string()).collect();
        self.check_or_set_total_rows(col_name, values.len() as u64)?;
        self.write_column_data(
            col_name,
            8,
            |addr| {
                for val in values {
                    addr.append_vaddr(val.to_canonical())?;
                }
                Ok(())
            },
            Some(|vault: &mut dyn VaultWriter| {
                vault.append_batch_strings(&string_values)
            }),
        )
    }

    /// Writes a primitive column directly to `.exo.addr` without creating any `.exo.vault`.
    pub fn write_primitive_column<T: ToCanonicalAddress>(
        &mut self,
        col_name: &str,
        values: &[T],
    ) -> ExoResult<()> {
        self.check_or_set_total_rows(col_name, values.len() as u64)?;
        self.write_column_data(
            col_name,
            8,
            |addr| {
                for val in values {
                    addr.append_vaddr(val.to_canonical())?;
                }
                Ok(())
            },
            None::<fn(&mut dyn VaultWriter) -> ExoResult<()>>,
        )
    }

    /// Writes a primitive column with a specific codec.
    pub fn write_primitive_column_with_codec<T: ToCanonicalAddress>(
        &mut self,
        col_name: &str,
        values: &[T],
        _codec_flags: u16,
    ) -> ExoResult<()> {
        self.write_primitive_column(col_name, values)
    }

    /// Writes an i32 column directly to `.exo.addr` with 4-byte native slot width.
    pub fn write_i32_column(
        &mut self,
        col_name: &str,
        values: &[i32],
    ) -> ExoResult<()> {
        self.check_or_set_total_rows(col_name, values.len() as u64)?;
        self.write_column_data(
            col_name,
            4,
            |addr| {
                for &val in values {
                    addr.append_vaddr(crate::coord::canonical_from_i32_native(val) as u64)?;
                }
                Ok(())
            },
            None::<fn(&mut dyn VaultWriter) -> ExoResult<()>>,
        )
    }

    /// Writes a u32 column directly to `.exo.addr` with 4-byte native slot width.
    pub fn write_u32_column(
        &mut self,
        col_name: &str,
        values: &[u32],
    ) -> ExoResult<()> {
        self.check_or_set_total_rows(col_name, values.len() as u64)?;
        self.write_column_data(
            col_name,
            4,
            |addr| {
                for &val in values {
                    addr.append_vaddr(val as u64)?;
                }
                Ok(())
            },
            None::<fn(&mut dyn VaultWriter) -> ExoResult<()>>,
        )
    }

    /// Writes an f32 column directly to `.exo.addr` with 4-byte native slot width.
    pub fn write_f32_column(
        &mut self,
        col_name: &str,
        values: &[f32],
    ) -> ExoResult<()> {
        self.check_or_set_total_rows(col_name, values.len() as u64)?;
        self.write_column_data(
            col_name,
            4,
            |addr| {
                for &val in values {
                    addr.append_vaddr(crate::coord::canonical_from_f32_native(val) as u64)?;
                }
                Ok(())
            },
            None::<fn(&mut dyn VaultWriter) -> ExoResult<()>>,
        )
    }

    /// Writes an i16 column directly to `.exo.addr` with 2-byte native slot width.
    pub fn write_i16_column(
        &mut self,
        col_name: &str,
        values: &[i16],
    ) -> ExoResult<()> {
        self.check_or_set_total_rows(col_name, values.len() as u64)?;
        self.write_column_data(
            col_name,
            2,
            |addr| {
                for &val in values {
                    addr.append_vaddr(crate::coord::canonical_from_i16_native(val) as u64)?;
                }
                Ok(())
            },
            None::<fn(&mut dyn VaultWriter) -> ExoResult<()>>,
        )
    }

    /// Writes a u16 column directly to `.exo.addr` with 2-byte native slot width.
    pub fn write_u16_column(
        &mut self,
        col_name: &str,
        values: &[u16],
    ) -> ExoResult<()> {
        self.check_or_set_total_rows(col_name, values.len() as u64)?;
        self.write_column_data(
            col_name,
            2,
            |addr| {
                for &val in values {
                    addr.append_vaddr(val as u64)?;
                }
                Ok(())
            },
            None::<fn(&mut dyn VaultWriter) -> ExoResult<()>>,
        )
    }

    /// Writes an i8 column directly to `.exo.addr` with 1-byte native slot width.
    pub fn write_i8_column(
        &mut self,
        col_name: &str,
        values: &[i8],
    ) -> ExoResult<()> {
        self.check_or_set_total_rows(col_name, values.len() as u64)?;
        self.write_column_data(
            col_name,
            1,
            |addr| {
                for &val in values {
                    addr.append_vaddr(crate::coord::canonical_from_i8_native(val) as u64)?;
                }
                Ok(())
            },
            None::<fn(&mut dyn VaultWriter) -> ExoResult<()>>,
        )
    }

    /// Writes a u8 column directly to `.exo.addr` with 1-byte native slot width.
    pub fn write_u8_column(
        &mut self,
        col_name: &str,
        values: &[u8],
    ) -> ExoResult<()> {
        self.check_or_set_total_rows(col_name, values.len() as u64)?;
        self.write_column_data(
            col_name,
            1,
            |addr| {
                for &val in values {
                    addr.append_vaddr(val as u64)?;
                }
                Ok(())
            },
            None::<fn(&mut dyn VaultWriter) -> ExoResult<()>>,
        )
    }

    /// Writes a boolean column directly to `.exo.addr` with 1-byte native slot width.
    pub fn write_bool_column(
        &mut self,
        col_name: &str,
        values: &[bool],
    ) -> ExoResult<()> {
        self.check_or_set_total_rows(col_name, values.len() as u64)?;
        self.write_column_data(
            col_name,
            1,
            |addr| {
                for &val in values {
                    addr.append_vaddr(if val { 1 } else { 0 })?;
                }
                Ok(())
            },
            None::<fn(&mut dyn VaultWriter) -> ExoResult<()>>,
        )
    }

    /// Commits the Part by atomically renaming the temporary directory to the final part directory.
    pub fn finish(self) -> ExoResult<PartDescriptor> {
        if self.columns_written.is_empty() {
            return Err(ExoError::CodecError(
                "Cannot commit an empty Part with zero columns".to_string(),
            ));
        }

        match self.mode {
            PartWriterMode::Disk { tmp_path, final_path, .. } => {
                if final_path.exists() {
                    let _ = std::fs::remove_dir_all(&final_path);
                }

                if let Ok(tmp_dir) = File::open(&tmp_path) {
                    let _ = tmp_dir.sync_all();
                }

                rename(&tmp_path, &final_path)?;

                if let Ok(final_dir) = File::open(&final_path) {
                    let _ = final_dir.sync_all();
                }
                if let Some(parent) = final_path.parent() {
                    if let Ok(parent_dir) = File::open(parent) {
                        let _ = parent_dir.sync_all();
                    }
                }

                let mut column_names: Vec<String> = self.columns_written.into_iter().collect();
                column_names.sort();

                Ok(PartDescriptor {
                    part_id: self.part_id,
                    path: final_path,
                    total_rows: self.total_rows.unwrap_or(0),
                    column_names,
                    granule_size: self.granule_size,
                })
            }
            PartWriterMode::Memory { .. } => {
                Err(ExoError::CodecError(
                    "PartWriter created in memory must be finished using finish_in_memory()".to_string(),
                ))
            }
        }
    }

    /// Commits an in-memory Part and returns all encoded column buffers.
    pub fn finish_in_memory(self) -> ExoResult<PartBufferDescriptor> {
        if self.columns_written.is_empty() {
            return Err(ExoError::CodecError(
                "Cannot commit an empty Part with zero columns".to_string(),
            ));
        }

        match self.mode {
            PartWriterMode::Memory { addr_buffers, vault_buffers } => {
                let mut column_names: Vec<String> = self.columns_written.into_iter().collect();
                column_names.sort();

                Ok(PartBufferDescriptor {
                    part_id: self.part_id,
                    total_rows: self.total_rows.unwrap_or(0),
                    column_names,
                    granule_size: self.granule_size,
                    addr_buffers,
                    vault_buffers,
                })
            }
            PartWriterMode::Disk { .. } => {
                Err(ExoError::CodecError(
                    "PartWriter created on disk must be finished using finish()".to_string(),
                ))
            }
        }
    }
}

/// Target data source for PartReader: either filesystem directory or in-memory column buffers.
#[derive(Clone, Debug)]
pub enum PartDataSource {
    Disk(PathBuf),
    Memory {
        addr_buffers: std::collections::HashMap<String, Vec<u8>>,
        vault_buffers: std::collections::HashMap<String, Vec<u8>>,
    },
}

/// Reader for accessing columns and tombstones of an existing Part.
pub struct PartReader {
    part_id: u32,
    source: PartDataSource,
    total_rows: u64,
    column_names: Vec<String>,
    tombstones: TombstoneMask,
}

impl PartReader {
    /// Opens an existing Part directory on disk and discovers its columns and tombstones.
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
            source: PartDataSource::Disk(path.to_path_buf()),
            total_rows,
            column_names,
            tombstones,
        })
    }

    /// Constructs a `PartReader` directly from in-memory column buffers (e.g. read from VFS blocks).
    pub fn from_buffers(
        part_id: u32,
        addr_buffers: std::collections::HashMap<String, Vec<u8>>,
        vault_buffers: std::collections::HashMap<String, Vec<u8>>,
        tombstones: Option<TombstoneMask>,
    ) -> ExoResult<Self> {
        if addr_buffers.is_empty() {
            return Err(ExoError::CodecError(format!(
                "In-memory Part {} contains no address column buffers",
                part_id
            )));
        }

        let mut column_names: Vec<String> = addr_buffers.keys().cloned().collect();
        column_names.sort();

        let first_col_bytes = addr_buffers.get(&column_names[0]).unwrap();
        let first_reader = ExoAddressReader::from_bytes(first_col_bytes.clone())?;
        let total_rows = first_reader.total_rows();

        let tombstones = tombstones.unwrap_or_else(TombstoneMask::new);

        Ok(Self {
            part_id,
            source: PartDataSource::Memory {
                addr_buffers,
                vault_buffers,
            },
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
        match &self.source {
            PartDataSource::Disk(p) => p.as_path(),
            PartDataSource::Memory { .. } => Path::new(""),
        }
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

    /// Saves updated tombstones to disk or in-memory.
    pub fn save_tombstones(&self) -> ExoResult<usize> {
        match &self.source {
            PartDataSource::Disk(p) => {
                let tomb_path = p.join("tombstones.exo.tomb");
                self.tombstones.save(&tomb_path)
            }
            PartDataSource::Memory { .. } => {
                let bytes = self.tombstones.to_bytes()?;
                Ok(bytes.len())
            }
        }
    }

    /// Opens an `ExoAddressReader` for the specified column in this Part.
    pub fn open_address_reader(&self, col_name: &str) -> ExoResult<ExoAddressReader> {
        match &self.source {
            PartDataSource::Disk(p) => {
                let addr_path = p.join(format!("{col_name}.exo.addr"));
                ExoAddressReader::open(&addr_path)
            }
            PartDataSource::Memory { addr_buffers, .. } => {
                let bytes = addr_buffers.get(col_name).ok_or_else(|| {
                    ExoError::Io(std::io::Error::new(
                        std::io::ErrorKind::NotFound,
                        format!("Column '{col_name}.exo.addr' not found in Part {}", self.part_id),
                    ))
                })?;
                ExoAddressReader::from_bytes(bytes.clone())
            }
        }
    }

    /// Opens an `ExoVaultReader` for the specified column in this Part.
    pub fn open_vault_reader(&self, col_name: &str) -> ExoResult<ExoVaultReader> {
        match &self.source {
            PartDataSource::Disk(p) => {
                let vault_path = p.join(format!("{col_name}.exo.vault"));
                ExoVaultReader::open(&vault_path)
            }
            PartDataSource::Memory { vault_buffers, .. } => {
                let bytes = vault_buffers.get(col_name).ok_or_else(|| {
                    ExoError::Io(std::io::Error::new(
                        std::io::ErrorKind::NotFound,
                        format!("Column '{col_name}.exo.vault' not found in Part {}", self.part_id),
                    ))
                })?;
                ExoVaultReader::from_bytes(bytes.clone())
            }
        }
    }

    /// Checks if an `.exo.vault` file or buffer exists for the specified column.
    #[inline(always)]
    pub fn has_vault(&self, col_name: &str) -> bool {
        match &self.source {
            PartDataSource::Disk(p) => p.join(format!("{col_name}.exo.vault")).exists(),
            PartDataSource::Memory { vault_buffers, .. } => vault_buffers.contains_key(col_name),
        }
    }

    /// Reads an inlined primitive scalar value directly from `.exo.addr` without vault I/O.
    pub fn read_primitive_slot(&self, col_name: &str, row_idx: u64) -> ExoResult<u64> {
        let addr_reader = self.open_address_reader(col_name)?;
        addr_reader.read_vaddr(row_idx)
    }

    /// Reads an inlined i32 value directly from `.exo.addr`.
    pub fn read_i32_slot(&self, col_name: &str, row_idx: u64) -> ExoResult<i32> {
        let addr_reader = self.open_address_reader(col_name)?;
        let vaddr = addr_reader.read_vaddr(row_idx)?;
        if addr_reader.slot_width() == 4 {
            Ok(crate::coord::i32_from_canonical_native(vaddr as u32))
        } else {
            Ok(crate::coord::i64_from_canonical(vaddr) as i32)
        }
    }

    /// Reads an inlined f32 value directly from `.exo.addr`.
    pub fn read_f32_slot(&self, col_name: &str, row_idx: u64) -> ExoResult<f32> {
        let addr_reader = self.open_address_reader(col_name)?;
        let vaddr = addr_reader.read_vaddr(row_idx)?;
        if addr_reader.slot_width() == 4 {
            Ok(crate::coord::f32_from_canonical_native(vaddr as u32))
        } else {
            Ok(crate::coord::f64_from_canonical(vaddr) as f32)
        }
    }

    /// Reads an inlined i16 value directly from `.exo.addr`.
    pub fn read_i16_slot(&self, col_name: &str, row_idx: u64) -> ExoResult<i16> {
        let addr_reader = self.open_address_reader(col_name)?;
        let vaddr = addr_reader.read_vaddr(row_idx)?;
        if addr_reader.slot_width() == 2 {
            Ok(crate::coord::i16_from_canonical_native(vaddr as u16))
        } else {
            Ok(crate::coord::i64_from_canonical(vaddr) as i16)
        }
    }

    /// Reads an inlined i8 value directly from `.exo.addr`.
    pub fn read_i8_slot(&self, col_name: &str, row_idx: u64) -> ExoResult<i8> {
        let addr_reader = self.open_address_reader(col_name)?;
        let vaddr = addr_reader.read_vaddr(row_idx)?;
        if addr_reader.slot_width() == 1 {
            Ok(crate::coord::i8_from_canonical_native(vaddr as u8))
        } else {
            Ok(crate::coord::i64_from_canonical(vaddr) as i8)
        }
    }

    /// Reads an inlined boolean value directly from `.exo.addr`.
    pub fn read_bool_slot(&self, col_name: &str, row_idx: u64) -> ExoResult<bool> {
        let addr_reader = self.open_address_reader(col_name)?;
        let vaddr = addr_reader.read_vaddr(row_idx)?;
        Ok(vaddr != 0)
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

    #[test]
    fn test_primitive_inlining_zero_vault() -> ExoResult<()> {
        let dir = tempdir()?;
        let mut writer = PartWriter::create(dir.path(), 99, 256)?;

        let ids: Vec<i64> = vec![-100, -1, 0, 42, 1_000_000, i64::MAX];
        let values: Vec<f64> = vec![-50.5, 0.0, 1.25, 99.99, 500.0, 10000.0];

        writer.write_primitive_column("id", &ids)?;
        writer.write_primitive_column("value", &values)?;
        let descriptor = writer.finish()?;

        let reader = PartReader::open(&descriptor.path, 99)?;
        assert_eq!(reader.total_rows(), 6);

        // Invariant: zero bytes in vault because vault doesn't exist for primitives!
        assert!(!reader.has_vault("id"));
        assert!(!reader.has_vault("value"));

        // Direct reading from address plane:
        for (i, &expected_id) in ids.iter().enumerate() {
            let vaddr = reader.read_primitive_slot("id", i as u64)?;
            let reconstructed = crate::coord::i64_from_canonical(vaddr);
            assert_eq!(reconstructed, expected_id);
        }

        for (i, &expected_val) in values.iter().enumerate() {
            let vaddr = reader.read_primitive_slot("value", i as u64)?;
            let reconstructed = crate::coord::f64_from_canonical(vaddr);
            assert_eq!(reconstructed, expected_val);
        }

        Ok(())
    }

    #[test]
    fn test_native_sub_64bit_part_writer_and_reader() -> ExoResult<()> {
        let dir = tempdir()?;
        let mut writer = PartWriter::create(dir.path(), 100, 256)?;

        let i32_vals = vec![i32::MIN, -1000, -1, 0, 1, 42, 999999, i32::MAX];
        let f32_vals = vec![-100.5f32, -0.001f32, 0.0f32, 1.25f32, 42.0f32, 1000.5f32, 99999.0f32, f32::MAX];
        let i16_vals = vec![i16::MIN, -500, -1, 0, 1, 100, 5000, i16::MAX];
        let i8_vals = vec![i8::MIN, -10, -1, 0, 1, 50, 100, i8::MAX];
        let bool_vals = vec![false, true, false, false, true, true, false, true];

        writer.write_i32_column("col_i32", &i32_vals)?;
        writer.write_f32_column("col_f32", &f32_vals)?;
        writer.write_i16_column("col_i16", &i16_vals)?;
        writer.write_i8_column("col_i8", &i8_vals)?;
        writer.write_bool_column("col_bool", &bool_vals)?;

        let descriptor = writer.finish()?;
        assert_eq!(descriptor.total_rows, 8);

        let reader = PartReader::open(&descriptor.path, 100)?;
        assert_eq!(reader.total_rows(), 8);

        // Verify slot widths
        let i32_r = reader.open_address_reader("col_i32")?;
        assert_eq!(i32_r.slot_width(), 4);

        let f32_r = reader.open_address_reader("col_f32")?;
        assert_eq!(f32_r.slot_width(), 4);

        let i16_r = reader.open_address_reader("col_i16")?;
        assert_eq!(i16_r.slot_width(), 2);

        let i8_r = reader.open_address_reader("col_i8")?;
        assert_eq!(i8_r.slot_width(), 1);

        let bool_r = reader.open_address_reader("col_bool")?;
        assert_eq!(bool_r.slot_width(), 1);

        // Verify values using typed read methods
        for (i, &expected) in i32_vals.iter().enumerate() {
            assert_eq!(reader.read_i32_slot("col_i32", i as u64)?, expected);
        }
        for (i, &expected) in f32_vals.iter().enumerate() {
            assert_eq!(reader.read_f32_slot("col_f32", i as u64)?, expected);
        }
        for (i, &expected) in i16_vals.iter().enumerate() {
            assert_eq!(reader.read_i16_slot("col_i16", i as u64)?, expected);
        }
        for (i, &expected) in i8_vals.iter().enumerate() {
            assert_eq!(reader.read_i8_slot("col_i8", i as u64)?, expected);
        }
        for (i, &expected) in bool_vals.iter().enumerate() {
            assert_eq!(reader.read_bool_slot("col_bool", i as u64)?, expected);
        }

        Ok(())
    }

    #[test]
    fn test_part_writer_and_reader_in_memory() -> ExoResult<()> {
        const GRANULE_SIZE: usize = 256;
        let mut writer = PartWriter::create_in_memory(99, GRANULE_SIZE);

        let ids: Vec<u64> = vec![1, 2, 3, 4, 5];
        let names = vec![
            "alice".to_string(),
            "bob".to_string(),
            "charlie".to_string(),
            "david".to_string(),
            "eve".to_string(),
        ];
        let scores: Vec<i32> = vec![100, 200, 300, 400, 500];

        writer.write_primitive_column("id", &ids)?;
        writer.write_string_column("name", &names)?;
        writer.write_i32_column("score", &scores)?;

        let desc = writer.finish_in_memory()?;
        assert_eq!(desc.part_id, 99);
        assert_eq!(desc.total_rows, 5);
        assert_eq!(desc.column_names, vec!["id", "name", "score"]);
        assert!(desc.addr_buffers.contains_key("id"));
        assert!(desc.addr_buffers.contains_key("name"));
        assert!(desc.addr_buffers.contains_key("score"));
        assert!(desc.vault_buffers.contains_key("name"));

        let reader = PartReader::from_buffers(99, desc.addr_buffers, desc.vault_buffers, None)?;
        assert_eq!(reader.part_id(), 99);
        assert_eq!(reader.total_rows(), 5);

        for (i, &expected_id) in ids.iter().enumerate() {
            assert_eq!(reader.read_primitive_slot("id", i as u64)?, expected_id);
        }
        let mut vault_reader = reader.open_vault_reader("name")?;
        for (i, expected_name) in names.iter().enumerate() {
            assert_eq!(vault_reader.read_str_slot(i as u32)?, expected_name.as_str());
        }
        for (i, &expected_score) in scores.iter().enumerate() {
            assert_eq!(reader.read_i32_slot("score", i as u64)?, expected_score);
        }

        Ok(())
    }
}
