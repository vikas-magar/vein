//! Payload Vault (`.exo.vault`) physical storage format implementation.
//!
//! Stores variable-length raw payloads (strings, text, binary attributes)
//! chunked into ClickHouse-style Granules with tail Marks.
//!
//! Implements $O(1)$ slot dereferencing and vectorized Arrow batch extraction.
//! Strict zero-unwrap policy is enforced.

use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crate::error::{ExoError, ExoResult};
use crate::mark_spool::MarkSpool;
use crate::traits::{StorageBuffer, VaultReader, VaultWriter};

pub const VAULT_HEADER_MAGIC: &[u8; 4] = b"EXOV";
pub const VAULT_TRAILER_MAGIC: &[u8; 4] = b"VEOF";
pub const VAULT_FORMAT_VERSION: u16 = 1;
pub const DEFAULT_VAULT_GRANULE_SIZE: usize = 8192;
pub const HEADER_SIZE: usize = 64;
pub const MARK_SIZE: usize = 16;
pub const TRAILER_SIZE: usize = 8;
pub const GRANULE_HEADER_SIZE: usize = 16;

/// Fixed 64-byte file header for `.exo.vault` files.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct VaultHeader {
    pub magic: [u8; 4],
    pub version: u16,
    pub codec_flags: u16,
    pub total_rows: u64,
    pub granule_size: u32,
    pub total_granules: u32,
    pub marks_offset: u64,
    pub checksum: u32,
}

impl VaultHeader {
    pub fn encode(&self) -> [u8; HEADER_SIZE] {
        let mut buf = [0u8; HEADER_SIZE];
        buf[0..4].copy_from_slice(&self.magic);
        buf[4..6].copy_from_slice(&self.version.to_le_bytes());
        buf[6..8].copy_from_slice(&self.codec_flags.to_le_bytes());
        buf[8..16].copy_from_slice(&self.total_rows.to_le_bytes());
        buf[16..20].copy_from_slice(&self.granule_size.to_le_bytes());
        buf[20..24].copy_from_slice(&self.total_granules.to_le_bytes());
        buf[24..32].copy_from_slice(&self.marks_offset.to_le_bytes());
        let checksum = crc32fast::hash(&buf[0..32]);
        buf[32..36].copy_from_slice(&checksum.to_le_bytes());
        buf
    }

    pub fn decode(bytes: &[u8; HEADER_SIZE]) -> ExoResult<Self> {
        let mut magic = [0u8; 4];
        magic.copy_from_slice(&bytes[0..4]);
        if &magic != VAULT_HEADER_MAGIC {
            return Err(ExoError::CorruptedMagic {
                expected: "EXOV",
                found: magic,
            });
        }

        let version = u16::from_le_bytes(bytes[4..6].try_into().unwrap_or([0u8; 2]));
        if version != VAULT_FORMAT_VERSION {
            return Err(ExoError::UnsupportedVersion { version });
        }

        let checksum = u32::from_le_bytes(bytes[32..36].try_into().unwrap_or([0u8; 4]));
        let calculated = crc32fast::hash(&bytes[0..32]);
        if checksum != 0 && calculated != checksum {
            return Err(ExoError::ChecksumMismatch {
                expected: checksum,
                actual: calculated,
                offset: 0,
            });
        }

        let codec_flags = u16::from_le_bytes(bytes[6..8].try_into().unwrap_or([0u8; 2]));
        let total_rows = u64::from_le_bytes(bytes[8..16].try_into().unwrap_or([0u8; 8]));
        let granule_size = u32::from_le_bytes(bytes[16..20].try_into().unwrap_or([0u8; 4]));
        let total_granules = u32::from_le_bytes(bytes[20..24].try_into().unwrap_or([0u8; 4]));
        let marks_offset = u64::from_le_bytes(bytes[24..32].try_into().unwrap_or([0u8; 8]));

        Ok(Self {
            magic,
            version,
            codec_flags,
            total_rows,
            granule_size,
            total_granules,
            marks_offset,
            checksum,
        })
    }
}

/// Tail Mark for a Payload Vault Granule (16 bytes).
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct VaultGranuleMark {
    /// Byte offset of the granule on disk.
    pub byte_offset: u64,
    /// Length of compressed granule bytes on disk.
    pub compressed_len: u32,
    /// Length of uncompressed granule data (offsets + raw payloads).
    pub uncompressed_len: u32,
}

impl VaultGranuleMark {
    pub fn encode(&self) -> [u8; MARK_SIZE] {
        let mut buf = [0u8; MARK_SIZE];
        buf[0..8].copy_from_slice(&self.byte_offset.to_le_bytes());
        buf[8..12].copy_from_slice(&self.compressed_len.to_le_bytes());
        buf[12..16].copy_from_slice(&self.uncompressed_len.to_le_bytes());
        buf
    }

    pub fn decode(bytes: &[u8; MARK_SIZE]) -> Self {
        let byte_offset = u64::from_le_bytes(bytes[0..8].try_into().unwrap_or([0u8; 8]));
        let compressed_len = u32::from_le_bytes(bytes[8..12].try_into().unwrap_or([0u8; 4]));
        let uncompressed_len = u32::from_le_bytes(bytes[12..16].try_into().unwrap_or([0u8; 4]));
        Self {
            byte_offset,
            compressed_len,
            uncompressed_len,
        }
    }
}

/// Writer for generating `.exo.vault` files.
/// Writer for generating `.exo.vault` files.
pub struct ExoVaultWriter<W: Write + Seek = File> {
    writer: W,
    path: Option<PathBuf>,
    granule_size: usize,
    offsets: Vec<u32>,
    payload_arena: Vec<u8>,
    validity: Vec<bool>,
    marks: Vec<VaultGranuleMark>,
    mark_spool: Option<MarkSpool>,
    mark_count: u32,
    total_rows: u64,
}

impl ExoVaultWriter<File> {
    /// Creates a new `.exo.vault` writer at the specified path.
    pub fn create(path: &Path, granule_size: usize) -> ExoResult<Self> {
        let mut file = File::create(path)?;
        let placeholder = [0u8; HEADER_SIZE];
        file.write_all(&placeholder)?;

        Ok(Self {
            writer: file,
            path: Some(path.to_path_buf()),
            granule_size,
            offsets: Vec::with_capacity(granule_size),
            payload_arena: Vec::with_capacity(granule_size * 64),
            validity: Vec::with_capacity(granule_size),
            marks: Vec::new(),
            mark_spool: Some(MarkSpool::create_near(path, "vault-marks")?),
            mark_count: 0,
            total_rows: 0,
        })
    }

    /// Returns the target path being written.
    pub fn path(&self) -> &Path {
        self.path.as_deref().expect("File writer always has path")
    }
}

impl ExoVaultWriter<std::io::Cursor<Vec<u8>>> {
    /// Creates a new in-memory `.exo.vault` writer that buffers directly in RAM.
    pub fn create_in_memory(granule_size: usize) -> ExoResult<Self> {
        let mut cursor = std::io::Cursor::new(Vec::new());
        let placeholder = [0u8; HEADER_SIZE];
        cursor.write_all(&placeholder)?;

        Ok(Self {
            writer: cursor,
            path: None,
            granule_size,
            offsets: Vec::with_capacity(granule_size),
            payload_arena: Vec::with_capacity(granule_size * 64),
            validity: Vec::with_capacity(granule_size),
            marks: Vec::new(),
            mark_spool: None,
            mark_count: 0,
            total_rows: 0,
        })
    }

    /// Finishes writing into memory and returns `(total_rows, buffer_bytes)`.
    pub fn finish_into_bytes(mut self) -> ExoResult<(u64, Vec<u8>)> {
        let rows = self.finish_internal()?;
        Ok((rows, self.writer.into_inner()))
    }
}

impl<W: Write + Seek> ExoVaultWriter<W> {
    /// Flushes the buffered rows for the current granule into a discrete block.
    fn flush_granule(&mut self) -> ExoResult<()> {
        if self.offsets.len() == 0 {
            return Ok(());
        }

        let byte_offset = self.writer.stream_position()?;
        let num_rows = self.offsets.len();
        let has_nulls = self.validity.iter().any(|&v| !v);

        // 1. Build validity bitmask if granule contains nulls
        let validity_bytes_len = if has_nulls { (num_rows + 7) / 8 } else { 0 };
        let mut validity_bytes = vec![0u8; validity_bytes_len];
        if has_nulls {
            for (i, &is_valid) in self.validity.iter().enumerate() {
                if is_valid {
                    validity_bytes[i / 8] |= 1 << (i % 8);
                }
            }
        }

        let offsets_bytes_len = (num_rows * 4) as u32;
        let payload_bytes_len = self.payload_arena.len() as u32;
        let total_uncompressed_len =
            (validity_bytes_len as u32) + offsets_bytes_len + payload_bytes_len;

        // 3. Write 16-byte Granule Block Header
        let mut granule_header = [0u8; GRANULE_HEADER_SIZE];
        granule_header[0] = 0; // Codec: 0 = None (FSST / ZSTD in future module)
        granule_header[1] = if has_nulls { 1 } else { 0 }; // Flags: bit 0 = has_nulls
        granule_header[4..8].copy_from_slice(&offsets_bytes_len.to_le_bytes());
        granule_header[8..12].copy_from_slice(&payload_bytes_len.to_le_bytes());

        // CRC32-C Checksum of validity + offsets + payload
        let mut hasher = crc32fast::Hasher::new();
        if has_nulls {
            hasher.update(&validity_bytes);
        }
        for off in &self.offsets {
            hasher.update(&off.to_le_bytes());
        }
        hasher.update(&self.payload_arena);
        let crc = hasher.finalize();
        granule_header[12..16].copy_from_slice(&crc.to_le_bytes());

        self.writer.write_all(&granule_header)?;

        // 4. Write validity mask (if present)
        if has_nulls {
            self.writer.write_all(&validity_bytes)?;
        }

        // 5. Write offsets table
        for off in &self.offsets {
            self.writer.write_all(&off.to_le_bytes())?;
        }

        // 6. Write raw payload
        self.writer.write_all(&self.payload_arena)?;

        let block_disk_len = (GRANULE_HEADER_SIZE as u32) + total_uncompressed_len;

        let mark = VaultGranuleMark {
            byte_offset,
            compressed_len: block_disk_len,
            uncompressed_len: total_uncompressed_len,
        };
        self.append_mark(mark)?;

        self.offsets.clear();
        self.payload_arena.clear();
        self.validity.clear();
        Ok(())
    }

    fn append_mark(&mut self, mark: VaultGranuleMark) -> ExoResult<()> {
        if let Some(spool) = &mut self.mark_spool {
            spool.append(&mark.encode())?;
        } else {
            self.marks.push(mark);
        }
        self.mark_count = self
            .mark_count
            .checked_add(1)
            .ok_or_else(|| ExoError::CodecError("Vault granule mark count exceeds u32".into()))?;
        Ok(())
    }

    fn finish_internal(&mut self) -> ExoResult<u64> {
        self.flush_granule()?;

        let marks_offset = self.writer.stream_position()?;

        if let Some(spool) = &mut self.mark_spool {
            spool.copy_to(&mut self.writer)?;
        } else {
            for mark in &self.marks {
                self.writer.write_all(&mark.encode())?;
            }
        }

        let marks_count = self.mark_count;
        self.writer.write_all(&marks_count.to_le_bytes())?;
        self.writer.write_all(VAULT_TRAILER_MAGIC)?;

        // Seek to 0 and write finalized header
        self.writer.seek(SeekFrom::Start(0))?;
        let header = VaultHeader {
            magic: *VAULT_HEADER_MAGIC,
            version: VAULT_FORMAT_VERSION,
            codec_flags: 0,
            total_rows: self.total_rows,
            granule_size: self.granule_size as u32,
            total_granules: self.mark_count,
            marks_offset,
            checksum: 0,
        };

        self.writer.write_all(&header.encode())?;
        let _ = self.writer.flush();

        Ok(self.total_rows)
    }
}

impl<W: Write + Seek> VaultWriter for ExoVaultWriter<W> {
    fn append_opt_bytes(&mut self, payload: Option<&[u8]>) -> ExoResult<()> {
        self.offsets.push(self.payload_arena.len() as u32);
        match payload {
            Some(p) => {
                self.payload_arena.extend_from_slice(p);
                self.validity.push(true);
            }
            None => {
                self.validity.push(false);
            }
        }
        self.total_rows += 1;

        if self.offsets.len() >= self.granule_size {
            self.flush_granule()?;
        }
        Ok(())
    }

    fn append_bytes(&mut self, payload: &[u8]) -> ExoResult<()> {
        self.append_opt_bytes(Some(payload))
    }

    fn append_null(&mut self) -> ExoResult<()> {
        self.append_opt_bytes(None)
    }

    fn append_str(&mut self, val: &str) -> ExoResult<()> {
        self.append_bytes(val.as_bytes())
    }

    fn append_opt_str(&mut self, val: Option<&str>) -> ExoResult<()> {
        self.append_opt_bytes(val.map(|s| s.as_bytes()))
    }

    fn append_batch_strings(&mut self, vals: &[String]) -> ExoResult<()> {
        for s in vals {
            self.append_str(s)?;
        }
        Ok(())
    }

    fn append_batch_opt_strings(&mut self, vals: &[Option<String>]) -> ExoResult<()> {
        for s in vals {
            self.append_opt_str(s.as_deref())?;
        }
        Ok(())
    }

    fn finish(mut self) -> ExoResult<u64> {
        self.finish_internal()
    }
}

/// In-memory decoded granule holding offsets table and raw payload bytes.
struct CachedVaultGranule {
    granule_idx: usize,
    rows_in_granule: usize,
    validity_mask: Option<Vec<u8>>,
    offsets: Vec<u32>,
    payload: Vec<u8>,
}

/// Production-grade dereferencing reader for `.exo.vault` files backed by memory mapping or RAM buffers.
pub struct ExoVaultReader {
    _file: Option<File>,
    data: StorageBuffer,
    header: VaultHeader,
    marks: Vec<VaultGranuleMark>,
    cached_granule: Option<CachedVaultGranule>,
}

impl ExoVaultReader {
    /// Opens an `.exo.vault` file and loads its tail marks into RAM using zero-copy memory mapping.
    pub fn open(path: &Path) -> ExoResult<Self> {
        let file = OpenOptions::new().read(true).open(path)?;
        let mmap = unsafe { memmap2::MmapOptions::new().map(&file)? };
        Self::from_storage_buffer(Some(file), StorageBuffer::Mmap(mmap))
    }

    /// Constructs an `ExoVaultReader` directly from in-memory bytes (e.g. from VFS blocks).
    pub fn from_bytes(bytes: Vec<u8>) -> ExoResult<Self> {
        Self::from_storage_buffer(None, StorageBuffer::Bytes(bytes))
    }

    fn from_storage_buffer(file: Option<File>, data: StorageBuffer) -> ExoResult<Self> {
        let file_len = data.len();

        if file_len < HEADER_SIZE + TRAILER_SIZE {
            return Err(ExoError::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "Buffer too small to be a valid .exo.vault stream",
            )));
        }

        // 1. Read header
        let mut header_buf = [0u8; HEADER_SIZE];
        header_buf.copy_from_slice(&data[0..HEADER_SIZE]);
        let header = VaultHeader::decode(&header_buf)?;

        // 2. Validate trailer
        let trailer_offset = file_len - TRAILER_SIZE;
        let mut trailer_magic = [0u8; 4];
        trailer_magic.copy_from_slice(&data[trailer_offset + 4..file_len]);
        if &trailer_magic != VAULT_TRAILER_MAGIC {
            return Err(ExoError::CorruptedMagic {
                expected: "VEOF",
                found: trailer_magic,
            });
        }

        // 3. Load marks table
        let total_granules = header.total_granules as usize;
        let marks_byte_len = total_granules * MARK_SIZE;
        let marks_start = header.marks_offset as usize;
        let marks_end = marks_start + marks_byte_len;

        if marks_end > trailer_offset {
            return Err(ExoError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Marks table offset exceeds buffer bounds",
            )));
        }

        let mut marks = Vec::with_capacity(total_granules);
        for chunk in data[marks_start..marks_end].chunks_exact(MARK_SIZE) {
            let mut buf = [0u8; MARK_SIZE];
            buf.copy_from_slice(chunk);
            marks.push(VaultGranuleMark::decode(&buf));
        }

        Ok(Self {
            _file: file,
            data,
            header,
            marks,
            cached_granule: None,
        })
    }

    /// Ensures the specified granule is loaded in the cache from buffer bytes.
    fn load_granule(&mut self, granule_idx: usize) -> ExoResult<()> {
        if let Some(ref cached) = self.cached_granule {
            if cached.granule_idx == granule_idx {
                return Ok(());
            }
        }

        if granule_idx >= self.marks.len() {
            return Err(ExoError::InvalidGranuleIndex {
                index: granule_idx,
                total_granules: self.marks.len(),
            });
        }

        let mark = &self.marks[granule_idx];
        let start = mark.byte_offset as usize;
        let end = start + (mark.compressed_len as usize);

        if end > self.data.len() || start >= end {
            return Err(ExoError::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "Vault granule offset out of buffer bounds",
            )));
        }

        let granule_bytes = &self.data[start..end];
        let g_hdr = &granule_bytes[0..GRANULE_HEADER_SIZE];

        let flags = g_hdr[1];
        let has_nulls = (flags & 1) != 0;
        let offsets_bytes_len =
            u32::from_le_bytes(g_hdr[4..8].try_into().unwrap_or([0u8; 4])) as usize;
        let payload_bytes_len =
            u32::from_le_bytes(g_hdr[8..12].try_into().unwrap_or([0u8; 4])) as usize;
        let expected_crc = u32::from_le_bytes(g_hdr[12..16].try_into().unwrap_or([0u8; 4]));

        let num_rows = offsets_bytes_len / 4;
        let validity_bytes_len = if has_nulls { (num_rows + 7) / 8 } else { 0 };

        let validity_start = GRANULE_HEADER_SIZE;
        let validity_end = validity_start + validity_bytes_len;
        let offsets_start = validity_end;
        let offsets_end = offsets_start + offsets_bytes_len;
        let payload_start = offsets_end;
        let payload_end = payload_start + payload_bytes_len;

        if payload_end > granule_bytes.len() {
            return Err(ExoError::CodecError(
                "Granule payload exceeds block length".to_string(),
            ));
        }

        // Verify CRC32-C Checksum
        let mut hasher = crc32fast::Hasher::new();
        if has_nulls {
            hasher.update(&granule_bytes[validity_start..validity_end]);
        }
        hasher.update(&granule_bytes[offsets_start..offsets_end]);
        hasher.update(&granule_bytes[payload_start..payload_end]);
        let actual_crc = hasher.finalize();

        if actual_crc != expected_crc {
            return Err(ExoError::ChecksumMismatch {
                expected: expected_crc,
                actual: actual_crc,
                offset: mark.byte_offset,
            });
        }

        let validity_mask = if has_nulls {
            Some(granule_bytes[validity_start..validity_end].to_vec())
        } else {
            None
        };

        let mut offsets = Vec::with_capacity(num_rows);
        for chunk in granule_bytes[offsets_start..offsets_end].chunks_exact(4) {
            offsets.push(u32::from_le_bytes(chunk.try_into().unwrap_or([0u8; 4])));
        }

        self.cached_granule = Some(CachedVaultGranule {
            granule_idx,
            rows_in_granule: num_rows,
            validity_mask,
            offsets,
            payload: granule_bytes[payload_start..payload_end].to_vec(),
        });

        Ok(())
    }
}

impl VaultReader for ExoVaultReader {
    #[inline(always)]
    fn total_rows(&self) -> u64 {
        self.header.total_rows
    }

    fn is_null(&mut self, slot_id: u32) -> ExoResult<bool> {
        if (slot_id as u64) >= self.header.total_rows {
            return Err(ExoError::InvalidSlotIndex {
                slot: slot_id,
                total_rows: self.header.total_rows,
            });
        }

        let granule_size = self.header.granule_size as usize;
        let granule_idx = (slot_id as usize) / granule_size;
        let row_in_granule = (slot_id as usize) % granule_size;

        self.load_granule(granule_idx)?;

        let cached = match self.cached_granule {
            Some(ref c) => c,
            None => {
                return Err(ExoError::Io(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "Granule cache failed to populate",
                )));
            }
        };

        if let Some(ref mask) = cached.validity_mask {
            let byte_idx = row_in_granule / 8;
            let bit_idx = row_in_granule % 8;
            let is_valid = (mask[byte_idx] & (1 << bit_idx)) != 0;
            Ok(!is_valid)
        } else {
            Ok(false)
        }
    }

    fn read_bytes_slot(&mut self, slot_id: u32) -> ExoResult<&[u8]> {
        if (slot_id as u64) >= self.header.total_rows {
            return Err(ExoError::InvalidSlotIndex {
                slot: slot_id,
                total_rows: self.header.total_rows,
            });
        }

        let granule_size = self.header.granule_size as usize;
        let granule_idx = (slot_id as usize) / granule_size;
        let row_in_granule = (slot_id as usize) % granule_size;

        self.load_granule(granule_idx)?;

        let cached = match self.cached_granule {
            Some(ref c) => c,
            None => {
                return Err(ExoError::Io(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "Granule cache failed to populate",
                )));
            }
        };

        let start = cached.offsets[row_in_granule] as usize;
        let end = if row_in_granule + 1 < cached.rows_in_granule {
            cached.offsets[row_in_granule + 1] as usize
        } else {
            cached.payload.len()
        };

        if end > cached.payload.len() || start > end {
            return Err(ExoError::CodecError(
                "Corrupted offset boundaries in granule".to_string(),
            ));
        }

        Ok(&cached.payload[start..end])
    }

    fn read_str_slot(&mut self, slot_id: u32) -> ExoResult<&str> {
        let bytes = self.read_bytes_slot(slot_id)?;
        std::str::from_utf8(bytes)
            .map_err(|e| ExoError::CodecError(format!("Invalid UTF-8 at slot {}: {}", slot_id, e)))
    }

    fn read_opt_bytes_slot(&mut self, slot_id: u32) -> ExoResult<Option<&[u8]>> {
        if self.is_null(slot_id)? {
            Ok(None)
        } else {
            self.read_bytes_slot(slot_id).map(Some)
        }
    }

    fn read_opt_str_slot(&mut self, slot_id: u32) -> ExoResult<Option<&str>> {
        if self.is_null(slot_id)? {
            Ok(None)
        } else {
            self.read_str_slot(slot_id).map(Some)
        }
    }

    fn materialize_string_batch(
        &mut self,
        slots: &[u32],
        builder: &mut arrow::array::StringBuilder,
    ) -> ExoResult<()> {
        for &slot in slots {
            if self.is_null(slot)? {
                builder.append_null();
            } else {
                let s = self.read_str_slot(slot)?;
                builder.append_value(s);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Array;
    use tempfile::tempdir;

    #[test]
    fn test_vault_writer_reader_roundtrip() -> ExoResult<()> {
        let dir = tempdir()?;
        let file_path = dir.path().join("customer_company.exo.vault");

        const GRANULE_SIZE: usize = 500;
        const TOTAL_ROWS: usize = 1200;

        let sample_companies: Vec<String> = (0..TOTAL_ROWS)
            .map(|i| format!("Enterprise Global Corp #{:05} Ltd. Headquarters", i))
            .collect();

        // 1. Write values
        {
            let mut writer = ExoVaultWriter::create(&file_path, GRANULE_SIZE)?;
            writer.append_batch_strings(&sample_companies)?;
            let written = writer.finish()?;
            assert_eq!(written, TOTAL_ROWS as u64);
        }

        // 2. Open via reader
        let mut reader = ExoVaultReader::open(&file_path)?;
        assert_eq!(reader.total_rows(), TOTAL_ROWS as u64);

        // 3. Test arbitrary random-access slot reads
        assert_eq!(reader.read_str_slot(0)?, sample_companies[0]);
        assert_eq!(reader.read_str_slot(499)?, sample_companies[499]);
        assert_eq!(reader.read_str_slot(500)?, sample_companies[500]); // Granule boundary
        assert_eq!(reader.read_str_slot(1199)?, sample_companies[1199]); // Last row

        // 4. Test Arrow batch materialization
        let mut builder = arrow::array::StringBuilder::new();
        let target_slots = [0u32, 42, 499, 500, 777, 1199];
        reader.materialize_string_batch(&target_slots, &mut builder)?;
        let array = builder.finish();

        assert_eq!(array.len(), target_slots.len());
        for (i, &slot) in target_slots.iter().enumerate() {
            assert_eq!(array.value(i), sample_companies[slot as usize]);
        }

        // 5. Test out-of-bounds slot error
        match reader.read_str_slot(TOTAL_ROWS as u32) {
            Err(ExoError::InvalidSlotIndex { slot, total_rows }) => {
                assert_eq!(slot, TOTAL_ROWS as u32);
                assert_eq!(total_rows, TOTAL_ROWS as u64);
            }
            res => panic!("Expected InvalidSlotIndex error, got: {:?}", res),
        }

        Ok(())
    }

    #[test]
    fn test_vault_sql_nullability_and_arrow_nulls() -> ExoResult<()> {
        let dir = tempdir()?;
        let file_path = dir.path().join("nullable_comments.exo.vault");

        const GRANULE_SIZE: usize = 32;
        const TOTAL_ROWS: usize = 100;

        // Build test data with:
        // - Null values (None)
        // - Empty strings (Some("")) -> must NOT be null!
        // - Non-empty strings (Some("val"))
        let mut test_data: Vec<Option<String>> = Vec::with_capacity(TOTAL_ROWS);
        for i in 0..TOTAL_ROWS {
            if i % 3 == 0 {
                test_data.push(None); // SQL NULL
            } else if i % 3 == 1 {
                test_data.push(Some(String::new())); // Empty string ""
            } else {
                test_data.push(Some(format!("Comment #{:03}", i)));
            }
        }

        // 1. Write nullable column
        {
            let mut writer = ExoVaultWriter::create(&file_path, GRANULE_SIZE)?;
            writer.append_batch_opt_strings(&test_data)?;
            let written = writer.finish()?;
            assert_eq!(written, TOTAL_ROWS as u64);
        }

        // 2. Open via reader
        let mut reader = ExoVaultReader::open(&file_path)?;
        assert_eq!(reader.total_rows(), TOTAL_ROWS as u64);

        // 3. Verify each slot individually
        for (i, expected) in test_data.iter().enumerate() {
            let slot = i as u32;
            match expected {
                None => {
                    assert!(reader.is_null(slot)?, "Slot {} should be null", slot);
                    assert_eq!(reader.read_opt_str_slot(slot)?, None);
                }
                Some(s) => {
                    assert!(!reader.is_null(slot)?, "Slot {} should not be null", slot);
                    assert_eq!(reader.read_opt_str_slot(slot)?, Some(s.as_str()));
                    assert_eq!(reader.read_str_slot(slot)?, s.as_str());
                }
            }
        }

        // 4. Test Arrow batch materialization preserves null masks!
        let slots: Vec<u32> = (0..TOTAL_ROWS as u32).collect();
        let mut builder = arrow::array::StringBuilder::new();
        reader.materialize_string_batch(&slots, &mut builder)?;
        let array = builder.finish();

        assert_eq!(array.len(), TOTAL_ROWS);
        let expected_nulls = test_data.iter().filter(|v| v.is_none()).count();
        assert_eq!(array.null_count(), expected_nulls);

        for (i, expected) in test_data.iter().enumerate() {
            match expected {
                None => {
                    assert!(array.is_null(i), "Arrow array index {} should be null", i);
                }
                Some(s) => {
                    assert!(
                        !array.is_null(i),
                        "Arrow array index {} should not be null",
                        i
                    );
                    assert_eq!(array.value(i), s.as_str());
                }
            }
        }

        Ok(())
    }

    #[test]
    fn test_vault_in_memory_roundtrip() -> ExoResult<()> {
        const GRANULE_SIZE: usize = 128;
        let test_strings = vec![
            Some("alpha".to_string()),
            None,
            Some("beta".to_string()),
            Some("gamma long string that exceeds standard sizes 1234567890".to_string()),
            None,
        ];

        let mut writer = ExoVaultWriter::create_in_memory(GRANULE_SIZE)?;
        writer.append_batch_opt_strings(&test_strings)?;
        let (rows, bytes) = writer.finish_into_bytes()?;
        assert_eq!(rows, 5);

        let mut reader = ExoVaultReader::from_bytes(bytes)?;
        assert_eq!(reader.total_rows(), 5);

        assert_eq!(reader.read_opt_str_slot(0)?, Some("alpha"));
        assert_eq!(reader.read_opt_str_slot(1)?, None);
        assert_eq!(reader.read_opt_str_slot(2)?, Some("beta"));
        assert_eq!(
            reader.read_opt_str_slot(3)?,
            Some("gamma long string that exceeds standard sizes 1234567890")
        );
        assert_eq!(reader.read_opt_str_slot(4)?, None);

        Ok(())
    }
}
