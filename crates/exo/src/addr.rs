//! Address Plane (`.exo.addr`) physical storage format implementation.
//!
//! Stores dense arrays of 64-bit canonical value addresses (`vAddr: u64`)
//! with ClickHouse-style Granule Marks at the file tail.
//!
//! Zero-copy reading is supported via memory mapping (`memmap2::Mmap`).
//! Strict zero-unwrap policy is enforced.

use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crate::codec::ForCodec;
use crate::error::{ExoError, ExoResult};
use crate::mark_spool::MarkSpool;
use crate::traits::{AddressReader, AddressWriter, StorageBuffer};

pub const ADDR_HEADER_MAGIC: &[u8; 4] = b"EXOA";
pub const ADDR_TRAILER_MAGIC: &[u8; 4] = b"AEOF";
pub const ADDR_FORMAT_VERSION: u16 = 2;
pub const DEFAULT_GRANULE_SIZE: usize = 8192;
pub const HEADER_SIZE: usize = 64;
pub const MARK_SIZE: usize = 40;
pub const MARK_FLAG_HAS_NON_NULL: u32 = 1;
pub const MARK_FLAG_HAS_NULL: u32 = 2;
pub const TRAILER_SIZE: usize = 8;

pub const CODEC_RAW: u16 = 0;
pub const CODEC_FOR: u16 = 1;

const ADDRESS_VALUE_ALIGNMENT: usize = std::mem::align_of::<u64>();

fn aligned_value_offset(bitmap_len: usize) -> ExoResult<usize> {
    let alignment_mask = ADDRESS_VALUE_ALIGNMENT - 1;
    bitmap_len
        .checked_add(alignment_mask)
        .map(|len| len & !alignment_mask)
        .ok_or_else(|| ExoError::CodecError("Address validity bitmap length overflow".to_string()))
}

/// Fixed 64-byte file header for `.exo.addr` files.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AddressHeader {
    pub magic: [u8; 4],
    pub version: u16,
    pub codec_flags: u16,
    pub total_rows: u64,
    pub granule_size: u32,
    pub total_granules: u32,
    pub marks_offset: u64,
    pub checksum: u32,
    pub slot_width: u8,
    pub _reserved: [u8; 27],
}

impl AddressHeader {
    /// Encodes header into a fixed 64-byte buffer.
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
        buf[36] = if self.slot_width == 0 {
            8
        } else {
            self.slot_width
        };
        buf
    }

    /// Decodes header from a 64-byte slice and verifies validity.
    pub fn decode(bytes: &[u8; HEADER_SIZE]) -> ExoResult<Self> {
        let mut magic = [0u8; 4];
        magic.copy_from_slice(&bytes[0..4]);
        if &magic != ADDR_HEADER_MAGIC {
            return Err(ExoError::CorruptedMagic {
                expected: "EXOA",
                found: magic,
            });
        }

        let version = u16::from_le_bytes(bytes[4..6].try_into().unwrap_or([0u8; 2]));
        if version != ADDR_FORMAT_VERSION {
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
        let slot_width_raw = bytes[36];
        let slot_width = if slot_width_raw == 0 {
            8
        } else {
            slot_width_raw
        };

        Ok(Self {
            magic,
            version,
            codec_flags,
            total_rows,
            granule_size,
            total_granules,
            marks_offset,
            checksum,
            slot_width,
            _reserved: [0u8; 27],
        })
    }
}

/// Tail Mark for an Address Granule (32 bytes aligned).
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AddressGranuleMark {
    /// Byte offset of the granule within the file.
    pub byte_offset: u64,
    /// Length of compressed granule bytes on disk.
    pub compressed_len: u32,
    /// Length of uncompressed bytes (rows * 8).
    pub uncompressed_len: u32,
    /// Minimum non-null vAddr in this granule (used for range skipping).
    pub min_vaddr: u64,
    /// Maximum non-null vAddr in this granule (used for range skipping).
    pub max_vaddr: u64,
    /// Number of bytes from the granule start to the value payload.
    ///
    /// This includes zero padding after the validity bitmap so raw `u64` payloads
    /// begin at an 8-byte aligned address for zero-copy reads.
    pub validity_len: u32,
    /// Granule flags, including whether at least one value is non-null.
    pub flags: u32,
}

impl AddressGranuleMark {
    pub fn encode(&self) -> [u8; MARK_SIZE] {
        let mut buf = [0u8; MARK_SIZE];
        buf[0..8].copy_from_slice(&self.byte_offset.to_le_bytes());
        buf[8..12].copy_from_slice(&self.compressed_len.to_le_bytes());
        buf[12..16].copy_from_slice(&self.uncompressed_len.to_le_bytes());
        buf[16..24].copy_from_slice(&self.min_vaddr.to_le_bytes());
        buf[24..32].copy_from_slice(&self.max_vaddr.to_le_bytes());
        buf[32..36].copy_from_slice(&self.validity_len.to_le_bytes());
        buf[36..40].copy_from_slice(&self.flags.to_le_bytes());
        buf
    }

    pub fn decode(bytes: &[u8; MARK_SIZE]) -> Self {
        let byte_offset = u64::from_le_bytes(bytes[0..8].try_into().unwrap_or([0u8; 8]));
        let compressed_len = u32::from_le_bytes(bytes[8..12].try_into().unwrap_or([0u8; 4]));
        let uncompressed_len = u32::from_le_bytes(bytes[12..16].try_into().unwrap_or([0u8; 4]));
        let min_vaddr = u64::from_le_bytes(bytes[16..24].try_into().unwrap_or([0u8; 8]));
        let max_vaddr = u64::from_le_bytes(bytes[24..32].try_into().unwrap_or([0u8; 8]));
        let validity_len = u32::from_le_bytes(bytes[32..36].try_into().unwrap_or([0u8; 4]));
        let flags = u32::from_le_bytes(bytes[36..40].try_into().unwrap_or([0u8; 4]));
        Self {
            byte_offset,
            compressed_len,
            uncompressed_len,
            min_vaddr,
            max_vaddr,
            validity_len,
            flags,
        }
    }
}

/// Writer for generating `.exo.addr` files.
pub struct ExoAddressWriter<W: Write + Seek = File> {
    writer: W,
    path: Option<PathBuf>,
    granule_size: usize,
    codec_flags: u16,
    slot_width: u8,
    buffer: Vec<u8>,
    validity: Vec<u8>,
    rows_in_granule: usize,
    marks: Vec<AddressGranuleMark>,
    mark_spool: Option<MarkSpool>,
    mark_count: u32,
    total_rows: u64,
    min_vaddr: u64,
    max_vaddr: u64,
    has_non_null: bool,
    has_null: bool,
}

impl ExoAddressWriter<File> {
    /// Creates a new `.exo.addr` writer at the specified path with default RAW codec and 8-byte slots.
    pub fn create(path: &Path, granule_size: usize) -> ExoResult<Self> {
        Self::create_with_codec_and_slot_width(path, granule_size, CODEC_RAW, 8)
    }

    /// Creates a new `.exo.addr` writer with Frame-of-Reference (FoR) bit-packing enabled and 8-byte slots.
    pub fn create_compressed(path: &Path, granule_size: usize) -> ExoResult<Self> {
        Self::create_with_codec_and_slot_width(path, granule_size, CODEC_FOR, 8)
    }

    /// Creates a new `.exo.addr` writer with explicit codec flags and 8-byte slots.
    pub fn create_with_codec(
        path: &Path,
        granule_size: usize,
        codec_flags: u16,
    ) -> ExoResult<Self> {
        Self::create_with_codec_and_slot_width(path, granule_size, codec_flags, 8)
    }

    /// Creates a new `.exo.addr` writer with explicit slot width (1, 2, 4, or 8 bytes) and RAW codec.
    pub fn create_with_slot_width(
        path: &Path,
        granule_size: usize,
        slot_width: u8,
    ) -> ExoResult<Self> {
        Self::create_with_codec_and_slot_width(path, granule_size, CODEC_RAW, slot_width)
    }

    /// Creates a new `.exo.addr` writer with explicit codec flags and slot width.
    pub fn create_with_codec_and_slot_width(
        path: &Path,
        granule_size: usize,
        codec_flags: u16,
        slot_width: u8,
    ) -> ExoResult<Self> {
        let mut file = File::create(path)?;
        // Write blank 64-byte header placeholder
        let placeholder = [0u8; HEADER_SIZE];
        file.write_all(&placeholder)?;

        let valid_width = match slot_width {
            1 | 2 | 4 | 8 => slot_width,
            _ => 8,
        };

        Ok(Self {
            writer: file,
            path: Some(path.to_path_buf()),
            granule_size,
            codec_flags,
            slot_width: valid_width,
            buffer: Vec::with_capacity(granule_size * (valid_width as usize)),
            validity: Vec::with_capacity((granule_size + 7) / 8),
            rows_in_granule: 0,
            marks: Vec::new(),
            mark_spool: Some(MarkSpool::create_near(path, "addr-marks")?),
            mark_count: 0,
            total_rows: 0,
            min_vaddr: u64::MAX,
            max_vaddr: 0,
            has_non_null: false,
            has_null: false,
        })
    }

    /// Returns the path to the file being written.
    pub fn path(&self) -> &Path {
        self.path.as_deref().expect("File writer always has path")
    }
}

impl ExoAddressWriter<std::io::Cursor<Vec<u8>>> {
    /// Creates a new in-memory `.exo.addr` writer that encodes directly into RAM.
    pub fn create_in_memory(granule_size: usize, slot_width: u8) -> ExoResult<Self> {
        let mut cursor = std::io::Cursor::new(Vec::new());
        let placeholder = [0u8; HEADER_SIZE];
        cursor.write_all(&placeholder)?;

        let valid_width = match slot_width {
            1 | 2 | 4 | 8 => slot_width,
            _ => 8,
        };

        Ok(Self {
            writer: cursor,
            path: None,
            granule_size,
            codec_flags: CODEC_RAW,
            slot_width: valid_width,
            buffer: Vec::with_capacity(granule_size * (valid_width as usize)),
            validity: Vec::with_capacity((granule_size + 7) / 8),
            rows_in_granule: 0,
            marks: Vec::new(),
            mark_spool: None,
            mark_count: 0,
            total_rows: 0,
            min_vaddr: u64::MAX,
            max_vaddr: 0,
            has_non_null: false,
            has_null: false,
        })
    }

    /// Finishes writing into memory and returns `(total_rows, buffer_bytes)`.
    pub fn finish_into_bytes(mut self) -> ExoResult<(u64, Vec<u8>)> {
        let rows = self.finish_internal()?;
        Ok((rows, self.writer.into_inner()))
    }
}

impl<W: Write + Seek> ExoAddressWriter<W> {
    /// Flushes the current in-memory buffer into a discrete physical granule on disk.
    fn flush_granule(&mut self) -> ExoResult<()> {
        if self.rows_in_granule == 0 {
            return Ok(());
        }

        let byte_offset = self.writer.stream_position()?;
        let min_vaddr = self.min_vaddr;
        let max_vaddr = self.max_vaddr;
        let bitmap_len = self.validity.len();
        let value_offset = aligned_value_offset(bitmap_len)?;
        let validity_len = u32::try_from(value_offset).map_err(|_| {
            ExoError::CodecError("Address validity bitmap is too large".to_string())
        })?;
        let uncompressed_len = self.buffer.len() as u32;
        self.writer.write_all(&self.validity)?;
        if value_offset > bitmap_len {
            self.writer
                .write_all(&[0u8; ADDRESS_VALUE_ALIGNMENT][..value_offset - bitmap_len])?;
        }

        let encoded_values_len = if self.codec_flags == CODEC_FOR && self.slot_width == 8 {
            let mut u64_buf = Vec::with_capacity(self.rows_in_granule);
            for chunk in self.buffer.chunks_exact(8) {
                u64_buf.push(u64::from_le_bytes(chunk.try_into().unwrap_or([0; 8])));
            }
            let (_, bit_width, packed_data) = ForCodec::encode_granule(&u64_buf);
            self.writer.write_all(&[bit_width])?;
            self.writer.write_all(&packed_data)?;
            (1 + packed_data.len()) as u32
        } else {
            self.writer.write_all(&self.buffer)?;
            uncompressed_len
        };

        let compressed_len = validity_len
            .checked_add(encoded_values_len)
            .ok_or_else(|| ExoError::CodecError("Address granule length overflow".to_string()))?;
        let mark = AddressGranuleMark {
            byte_offset,
            compressed_len,
            uncompressed_len,
            min_vaddr,
            max_vaddr,
            validity_len,
            flags: (if self.has_non_null {
                MARK_FLAG_HAS_NON_NULL
            } else {
                0
            }) | (if self.has_null { MARK_FLAG_HAS_NULL } else { 0 }),
        };
        self.append_mark(mark)?;

        self.buffer.clear();
        self.validity.clear();
        self.rows_in_granule = 0;
        self.min_vaddr = u64::MAX;
        self.max_vaddr = 0;
        self.has_non_null = false;
        self.has_null = false;
        Ok(())
    }

    fn append_mark(&mut self, mark: AddressGranuleMark) -> ExoResult<()> {
        if let Some(spool) = &mut self.mark_spool {
            spool.append(&mark.encode())?;
        } else {
            self.marks.push(mark);
        }
        self.mark_count = self
            .mark_count
            .checked_add(1)
            .ok_or_else(|| ExoError::CodecError("Address granule mark count exceeds u32".into()))?;
        Ok(())
    }

    fn finish_internal(&mut self) -> ExoResult<u64> {
        // Flush any lingering values in the final granule
        self.flush_granule()?;

        let marks_offset = self.writer.stream_position()?;

        // Write marks table
        if let Some(spool) = &mut self.mark_spool {
            spool.copy_to(&mut self.writer)?;
        } else {
            for mark in &self.marks {
                self.writer.write_all(&mark.encode())?;
            }
        }

        // Write trailer (marks count + AEOF magic)
        let marks_count = self.mark_count;
        self.writer.write_all(&marks_count.to_le_bytes())?;
        self.writer.write_all(ADDR_TRAILER_MAGIC)?;

        // Seek back to start and write finalized header
        self.writer.seek(SeekFrom::Start(0))?;

        let header = AddressHeader {
            magic: *ADDR_HEADER_MAGIC,
            version: ADDR_FORMAT_VERSION,
            codec_flags: self.codec_flags,
            total_rows: self.total_rows,
            granule_size: self.granule_size as u32,
            total_granules: self.mark_count,
            marks_offset,
            checksum: 0,
            slot_width: self.slot_width,
            _reserved: [0u8; 27],
        };

        self.writer.write_all(&header.encode())?;
        let _ = self.writer.flush();

        Ok(self.total_rows)
    }
}

impl<W: Write + Seek> AddressWriter for ExoAddressWriter<W> {
    fn append_vaddr(&mut self, vaddr: u64) -> ExoResult<()> {
        self.append_nullable_vaddr(vaddr, true)
    }

    fn append_nullable_vaddr(&mut self, vaddr: u64, valid: bool) -> ExoResult<()> {
        match self.slot_width {
            1 => self.buffer.push(vaddr as u8),
            2 => self.buffer.extend_from_slice(&(vaddr as u16).to_le_bytes()),
            4 => self.buffer.extend_from_slice(&(vaddr as u32).to_le_bytes()),
            _ => self.buffer.extend_from_slice(&vaddr.to_le_bytes()),
        }
        let bitmap_byte = self.rows_in_granule / 8;
        if bitmap_byte >= self.validity.len() {
            self.validity.push(0);
        }
        if valid {
            self.validity[bitmap_byte] |= 1 << (self.rows_in_granule % 8);
            self.min_vaddr = self.min_vaddr.min(vaddr);
            self.max_vaddr = self.max_vaddr.max(vaddr);
            self.has_non_null = true;
        } else {
            self.has_null = true;
        }
        self.rows_in_granule += 1;
        self.total_rows += 1;

        if self.rows_in_granule >= self.granule_size {
            self.flush_granule()?;
        }
        Ok(())
    }

    fn append_batch(&mut self, vaddrs: &[u64]) -> ExoResult<()> {
        for &vaddr in vaddrs {
            self.append_vaddr(vaddr)?;
        }
        Ok(())
    }

    fn append_nullable_batch(&mut self, vaddrs: &[u64], validity: &[bool]) -> ExoResult<()> {
        if vaddrs.len() != validity.len() {
            return Err(ExoError::CodecError(
                "Address values and validity lengths differ".to_string(),
            ));
        }
        for (&vaddr, &valid) in vaddrs.iter().zip(validity) {
            self.append_nullable_vaddr(vaddr, valid)?;
        }
        Ok(())
    }

    fn finish(mut self) -> ExoResult<u64> {
        self.finish_internal()
    }
}

/// Production-grade zero-copy reader for `.exo.addr` streams.
pub struct ExoAddressReader {
    _file: Option<File>,
    data: StorageBuffer,
    header: AddressHeader,
    marks: Vec<AddressGranuleMark>,
}

impl ExoAddressReader {
    /// Opens an `.exo.addr` file and loads its metadata.
    pub fn open(path: &Path) -> ExoResult<Self> {
        let file = OpenOptions::new().read(true).open(path)?;
        let mmap = unsafe { memmap2::MmapOptions::new().map(&file)? };
        Self::from_storage_buffer(Some(file), StorageBuffer::Mmap(mmap))
    }

    /// Constructs an `ExoAddressReader` directly from in-memory bytes (e.g. from VFS blocks).
    pub fn from_bytes(bytes: Vec<u8>) -> ExoResult<Self> {
        Self::from_storage_buffer(None, StorageBuffer::Bytes(bytes))
    }

    fn from_storage_buffer(file: Option<File>, data: StorageBuffer) -> ExoResult<Self> {
        let file_len = data.len();

        if file_len < HEADER_SIZE + TRAILER_SIZE {
            return Err(ExoError::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "Buffer too small to be a valid .exo.addr stream",
            )));
        }

        // Decode and validate header
        let mut header_buf = [0u8; HEADER_SIZE];
        header_buf.copy_from_slice(&data[0..HEADER_SIZE]);
        let header = AddressHeader::decode(&header_buf)?;

        // Validate trailer magic at tail
        let trailer_offset = file_len - TRAILER_SIZE;
        let mut trailer_magic = [0u8; 4];
        trailer_magic.copy_from_slice(&data[trailer_offset + 4..file_len]);
        if &trailer_magic != ADDR_TRAILER_MAGIC {
            return Err(ExoError::CorruptedMagic {
                expected: "AEOF",
                found: trailer_magic,
            });
        }

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
            let mut mark_buf = [0u8; MARK_SIZE];
            mark_buf.copy_from_slice(chunk);
            marks.push(AddressGranuleMark::decode(&mark_buf));
        }

        Ok(Self {
            _file: file,
            data,
            header,
            marks,
        })
    }

    /// Returns the codec flags configured for this address column.
    #[inline(always)]
    pub fn codec_flags(&self) -> u16 {
        self.header.codec_flags
    }

    /// Returns the slot width (1, 2, 4, or 8 bytes) configured for this address column.
    #[inline(always)]
    pub fn slot_width(&self) -> u8 {
        self.header.slot_width
    }

    /// Returns a direct slice of the raw granule bytes from the buffer.
    pub fn scan_raw_granule_bytes(&self, granule_idx: usize) -> ExoResult<&[u8]> {
        if granule_idx >= self.marks.len() {
            return Err(ExoError::InvalidGranuleIndex {
                index: granule_idx,
                total_granules: self.marks.len(),
            });
        }
        if self.header.codec_flags != CODEC_RAW {
            return Err(ExoError::CodecError(
                "Raw granule bytes are unavailable for compressed address columns".to_string(),
            ));
        }
        let mark = &self.marks[granule_idx];
        let start = mark.byte_offset as usize + mark.validity_len as usize;
        let byte_len = mark.uncompressed_len as usize;
        let end = start + byte_len;
        if end > self.data.len() {
            return Err(ExoError::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "Granule offset exceeds buffer boundary",
            )));
        }
        Ok(&self.data[start..end])
    }
    /// Decodes one fetched granule without requiring the complete address column.
    pub fn decode_granule_bytes(
        header: &AddressHeader,
        mark: &AddressGranuleMark,
        bytes: &[u8],
        out_buffer: &mut [u64],
    ) -> ExoResult<usize> {
        let slot_w = header.slot_width as usize;
        let num_values = (mark.uncompressed_len as usize) / slot_w;
        if out_buffer.len() < num_values {
            return Err(ExoError::BufferTooSmall {
                required: num_values,
                provided: out_buffer.len(),
            });
        }
        let total_len = mark.compressed_len as usize;
        if bytes.len() < total_len || mark.validity_len as usize > total_len {
            return Err(ExoError::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "Fetched address granule is shorter than its mark",
            )));
        }
        let values = &bytes[mark.validity_len as usize..total_len];
        if header.codec_flags == CODEC_FOR && header.slot_width == 8 {
            if values.is_empty() {
                return Err(ExoError::CodecError(
                    "Compressed granule has no codec header".to_string(),
                ));
            }
            return ForCodec::decode_granule(
                &values[1..],
                mark.min_vaddr,
                values[0],
                num_values,
                out_buffer,
            );
        }
        let expected_len = num_values * slot_w;
        if values.len() < expected_len {
            return Err(ExoError::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "Raw address granule is shorter than its mark",
            )));
        }
        match header.slot_width {
            1 => {
                for (idx, value) in values[..num_values].iter().enumerate() {
                    out_buffer[idx] = *value as u64;
                }
            }
            2 => {
                for (idx, value) in values.chunks_exact(2).take(num_values).enumerate() {
                    out_buffer[idx] = u16::from_le_bytes(value.try_into().unwrap_or([0; 2])) as u64;
                }
            }
            4 => {
                for (idx, value) in values.chunks_exact(4).take(num_values).enumerate() {
                    out_buffer[idx] = u32::from_le_bytes(value.try_into().unwrap_or([0; 4])) as u64;
                }
            }
            8 => {
                for (idx, value) in values.chunks_exact(8).take(num_values).enumerate() {
                    out_buffer[idx] = u64::from_le_bytes(value.try_into().unwrap_or([0; 8]));
                }
            }
            width => {
                return Err(ExoError::CodecError(format!(
                    "Unsupported address slot width: {width}"
                )));
            }
        }
        Ok(num_values)
    }

    /// Copies the fetched granule validity bitmap into a caller-owned buffer.
    /// Returns metadata for one address granule.
    pub fn granule_mark(&self, granule_idx: usize) -> ExoResult<&AddressGranuleMark> {
        self.marks
            .get(granule_idx)
            .ok_or(ExoError::InvalidGranuleIndex {
                index: granule_idx,
                total_granules: self.marks.len(),
            })
    }

    pub fn granule_validity_bytes<'a>(
        mark: &AddressGranuleMark,
        bytes: &'a [u8],
    ) -> ExoResult<&'a [u8]> {
        let validity_len = mark.validity_len as usize;
        if bytes.len() < validity_len {
            return Err(ExoError::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "Fetched address validity bitmap is truncated",
            )));
        }
        Ok(&bytes[..validity_len])
    }
}

impl AddressReader for ExoAddressReader {
    #[inline(always)]
    fn total_rows(&self) -> u64 {
        self.header.total_rows
    }

    #[inline(always)]
    fn total_granules(&self) -> usize {
        self.marks.len()
    }

    #[inline(always)]
    fn granule_size(&self) -> usize {
        self.header.granule_size as usize
    }

    fn read_granule(&self, granule_idx: usize, out_buffer: &mut [u64]) -> ExoResult<usize> {
        if granule_idx >= self.marks.len() {
            return Err(ExoError::InvalidGranuleIndex {
                index: granule_idx,
                total_granules: self.marks.len(),
            });
        }

        let mark = &self.marks[granule_idx];
        let slot_w = self.header.slot_width as usize;
        let num_values = (mark.uncompressed_len as usize) / slot_w;

        if out_buffer.len() < num_values {
            return Err(ExoError::BufferTooSmall {
                required: num_values,
                provided: out_buffer.len(),
            });
        }

        let values_start = (mark.byte_offset as usize) + (mark.validity_len as usize);
        if self.header.codec_flags == CODEC_FOR && self.header.slot_width == 8 {
            let start = values_start;
            let end = (mark.byte_offset as usize) + (mark.compressed_len as usize);
            if end > self.data.len() || end <= start {
                return Err(ExoError::Io(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "FoR granule offset exceeds buffer boundary",
                )));
            }
            let granule_bytes = &self.data[start..end];
            let bit_width = granule_bytes[0];
            let packed_data = &granule_bytes[1..];
            ForCodec::decode_granule(
                packed_data,
                mark.min_vaddr,
                bit_width,
                num_values,
                out_buffer,
            )
        } else {
            let start = values_start;
            let byte_len = mark.uncompressed_len as usize;
            let end = start + byte_len;
            if end > self.data.len() {
                return Err(ExoError::Io(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "Granule offset exceeds buffer boundary",
                )));
            }
            let raw_bytes = &self.data[start..end];
            match self.header.slot_width {
                1 => {
                    for (i, &b) in raw_bytes[..num_values].iter().enumerate() {
                        out_buffer[i] = b as u64;
                    }
                }
                2 => {
                    for (i, chunk) in raw_bytes.chunks_exact(2).take(num_values).enumerate() {
                        out_buffer[i] =
                            u16::from_le_bytes(chunk.try_into().unwrap_or([0; 2])) as u64;
                    }
                }
                4 => {
                    for (i, chunk) in raw_bytes.chunks_exact(4).take(num_values).enumerate() {
                        out_buffer[i] =
                            u32::from_le_bytes(chunk.try_into().unwrap_or([0; 4])) as u64;
                    }
                }
                _ => {
                    for (i, chunk) in raw_bytes.chunks_exact(8).take(num_values).enumerate() {
                        out_buffer[i] = u64::from_le_bytes(chunk.try_into().unwrap_or([0; 8]));
                    }
                }
            }
            Ok(num_values)
        }
    }

    /// Zero-copy borrowed slice directly into memory for RAW uncompressed granules with 8-byte slots!
    fn scan_granule_slice(&self, granule_idx: usize) -> ExoResult<&[u64]> {
        if self.header.slot_width != 8 {
            return Err(ExoError::CodecError(format!(
                "Cannot obtain &[u64] slice from column with slot_width = {}; use read_granule or read_vaddr instead",
                self.header.slot_width
            )));
        }

        if self.header.codec_flags == CODEC_FOR {
            return Err(ExoError::CodecError(
                "Cannot obtain zero-copy &[u64] slice from FoR bit-packed granule; use read_granule() instead".to_string(),
            ));
        }

        if granule_idx >= self.marks.len() {
            return Err(ExoError::InvalidGranuleIndex {
                index: granule_idx,
                total_granules: self.marks.len(),
            });
        }

        let mark = &self.marks[granule_idx];
        let start = mark.byte_offset as usize + mark.validity_len as usize;
        let byte_len = mark.uncompressed_len as usize;
        let end = start + byte_len;

        if end > self.data.len() {
            return Err(ExoError::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "Granule offset exceeds buffer boundary",
            )));
        }

        let raw_bytes = &self.data[start..end];
        let num_u64s = byte_len / 8;

        // Verify 8-byte alignment for safe zero-copy slice casting
        let ptr = raw_bytes.as_ptr();
        if (ptr as usize) % std::mem::align_of::<u64>() != 0 {
            return Err(ExoError::CodecError(
                "Buffer pointer is unaligned for u64 array".to_string(),
            ));
        }

        // Safety:
        // 1. Pointer is aligned to 8 bytes.
        // 2. Memory is valid for the lifetime of `self.data`.
        // 3. Size is exactly `num_u64s * 8`.
        let u64_slice = unsafe { std::slice::from_raw_parts(ptr as *const u64, num_u64s) };
        Ok(u64_slice)
    }

    #[inline(always)]
    fn min_scalar(&self, granule_idx: usize) -> Option<u64> {
        self.marks
            .get(granule_idx)
            .and_then(|m| (m.flags & MARK_FLAG_HAS_NON_NULL != 0).then_some(m.min_vaddr))
    }

    #[inline(always)]
    fn max_scalar(&self, granule_idx: usize) -> Option<u64> {
        self.marks
            .get(granule_idx)
            .and_then(|m| (m.flags & MARK_FLAG_HAS_NON_NULL != 0).then_some(m.max_vaddr))
    }

    fn is_valid(&self, row_idx: u64) -> ExoResult<bool> {
        if row_idx >= self.total_rows() {
            return Err(ExoError::InvalidSlotIndex {
                slot: row_idx as u32,
                total_rows: self.total_rows(),
            });
        }
        let granule_idx = (row_idx as usize) / self.granule_size();
        let offset = (row_idx as usize) % self.granule_size();
        let mark = &self.marks[granule_idx];
        let bitmap = mark.byte_offset as usize;
        let end = bitmap + mark.validity_len as usize;
        if end > self.data.len() || offset / 8 >= mark.validity_len as usize {
            return Err(ExoError::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "Validity bitmap exceeds buffer boundary",
            )));
        }
        Ok(self.data[bitmap + offset / 8] & (1 << (offset % 8)) != 0)
    }

    fn read_vaddr(&self, row_idx: u64) -> ExoResult<u64> {
        if row_idx >= self.total_rows() {
            return Err(ExoError::InvalidSlotIndex {
                slot: row_idx as u32,
                total_rows: self.total_rows(),
            });
        }
        let gsize = self.granule_size();
        let g_idx = (row_idx as usize) / gsize;
        let offset = (row_idx as usize) % gsize;
        let slot_w = self.header.slot_width as usize;

        if self.codec_flags() == CODEC_RAW {
            let mark = &self.marks[g_idx];
            let start = mark.byte_offset as usize + mark.validity_len as usize + offset * slot_w;
            let end = start + slot_w;
            if end > self.data.len() {
                return Err(ExoError::Io(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "Slot offset exceeds buffer boundary",
                )));
            }
            let bytes = &self.data[start..end];
            let val = match self.header.slot_width {
                1 => bytes[0] as u64,
                2 => u16::from_le_bytes(bytes.try_into().unwrap_or([0; 2])) as u64,
                4 => u32::from_le_bytes(bytes.try_into().unwrap_or([0; 4])) as u64,
                _ => u64::from_le_bytes(bytes.try_into().unwrap_or([0; 8])),
            };
            Ok(val)
        } else {
            let mut buf = vec![0u64; gsize];
            let read = self.read_granule(g_idx, &mut buf)?;
            if offset < read {
                Ok(buf[offset])
            } else {
                Err(ExoError::InvalidSlotIndex {
                    slot: row_idx as u32,
                    total_rows: self.total_rows(),
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_address_writer_reader_roundtrip_zero_copy() -> ExoResult<()> {
        let dir = tempdir()?;
        let file_path = dir.path().join("test_col.exo.addr");

        const GRANULE_SIZE: usize = 1024;
        const TOTAL_VALUES: usize = 2500;

        let test_vaddrs: Vec<u64> = (0..TOTAL_VALUES as u64)
            .map(|i| (i * 1337) ^ 0xA5A5A5A5_5A5A5A5A)
            .collect();

        // 1. Write values using ExoAddressWriter
        {
            let mut writer = ExoAddressWriter::create(&file_path, GRANULE_SIZE)?;
            writer.append_batch(&test_vaddrs)?;
            let written = writer.finish()?;
            assert_eq!(written, TOTAL_VALUES as u64);
        }

        // 2. Open via ExoAddressReader
        let reader = ExoAddressReader::open(&file_path)?;
        assert_eq!(reader.total_rows(), TOTAL_VALUES as u64);
        assert_eq!(reader.granule_size(), GRANULE_SIZE);
        // 2500 rows / 1024 = 3 granules (1024, 1024, 452)
        assert_eq!(reader.total_granules(), 3);

        // 3. Verify zero-copy slice reads per granule
        let mut read_count = 0;
        for g in 0..reader.total_granules() {
            let slice = reader.scan_granule_slice(g)?;
            let expected_start = g * GRANULE_SIZE;
            let expected_end = std::cmp::min(expected_start + GRANULE_SIZE, TOTAL_VALUES);
            let expected_slice = &test_vaddrs[expected_start..expected_end];

            assert_eq!(slice, expected_slice, "Mismatch in granule {}", g);
            read_count += slice.len();

            // Verify min_scalar matches actual min in this granule
            let actual_min = *expected_slice.iter().min().unwrap_or(&0);
            assert_eq!(reader.min_scalar(g), Some(actual_min));
        }

        assert_eq!(read_count, TOTAL_VALUES);

        // Verify min and max scalars per granule
        for g in 0..reader.total_granules() {
            let slice = reader.scan_granule_slice(g)?;
            let actual_min = *slice.iter().min().unwrap_or(&0);
            let actual_max = *slice.iter().max().unwrap_or(&0);
            assert_eq!(reader.min_scalar(g), Some(actual_min));
            assert_eq!(reader.max_scalar(g), Some(actual_max));
        }

        // Verify read_vaddr point reads
        assert_eq!(reader.read_vaddr(0)?, test_vaddrs[0]);
        assert_eq!(reader.read_vaddr(1023)?, test_vaddrs[1023]);
        assert_eq!(reader.read_vaddr(1024)?, test_vaddrs[1024]);
        assert_eq!(reader.read_vaddr(2499)?, test_vaddrs[2499]);
        assert!(reader.read_vaddr(2500).is_err());

        Ok(())
    }

    #[test]
    fn test_nullable_compact_address_roundtrip() -> ExoResult<()> {
        let dir = tempdir()?;
        let file_path = dir.path().join("nullable_u8.exo.addr");
        let values = [0u64, 17, 0, 255, 0, 42];
        let validity = [true, false, true, true, false, true];

        {
            let mut writer = ExoAddressWriter::create_with_slot_width(&file_path, 4, 1)?;
            writer.append_nullable_batch(&values, &validity)?;
            assert_eq!(writer.finish()?, values.len() as u64);
        }

        let reader = ExoAddressReader::open(&file_path)?;
        assert_eq!(reader.slot_width(), 1);
        for (idx, (&value, &valid)) in values.iter().zip(&validity).enumerate() {
            assert_eq!(reader.is_valid(idx as u64)?, valid);
            assert_eq!(reader.read_vaddr(idx as u64)?, value as u8 as u64);
        }
        assert_eq!(reader.min_scalar(0), Some(0));
        assert_eq!(reader.max_scalar(0), Some(255));
        assert_eq!(reader.min_scalar(1), Some(42));
        assert_eq!(reader.max_scalar(1), Some(42));
        Ok(())
    }

    #[test]
    fn test_address_for_compressed_roundtrip_and_range_pruning() -> ExoResult<()> {
        let dir = tempdir()?;
        let raw_path = dir.path().join("raw.exo.addr");
        let for_path = dir.path().join("for.exo.addr");

        const GRANULE_SIZE: usize = 512;
        const TOTAL_VALUES: usize = 2000;

        // Monotonic values with small delta: FoR should compress dramatically
        let test_vaddrs: Vec<u64> = (0..TOTAL_VALUES as u64)
            .map(|i| 1_000_000_000 + (i * 3))
            .collect();

        // 1. Write uncompressed RAW
        {
            let mut writer = ExoAddressWriter::create(&raw_path, GRANULE_SIZE)?;
            writer.append_batch(&test_vaddrs)?;
            writer.finish()?;
        }

        // 2. Write FoR compressed
        {
            let mut writer = ExoAddressWriter::create_compressed(&for_path, GRANULE_SIZE)?;
            writer.append_batch(&test_vaddrs)?;
            writer.finish()?;
        }

        let raw_size = std::fs::metadata(&raw_path)?.len();
        let for_size = std::fs::metadata(&for_path)?.len();

        // FoR compression should be substantially smaller than raw 64-bit arrays
        assert!(
            for_size < raw_size,
            "FoR size ({}) should be smaller than raw size ({})",
            for_size,
            raw_size
        );

        // 3. Open FoR reader and verify
        let reader = ExoAddressReader::open(&for_path)?;
        assert_eq!(reader.codec_flags(), CODEC_FOR);
        assert_eq!(reader.total_rows(), TOTAL_VALUES as u64);
        assert_eq!(reader.total_granules(), 4); // 2000 / 512 = 4 granules

        // FoR must disallow direct raw &[u64] slice access
        match reader.scan_granule_slice(0) {
            Err(ExoError::CodecError(_)) => {} // Expected!
            res => panic!(
                "Expected CodecError for scan_granule_slice on FoR, got: {:?}",
                res
            ),
        }

        // Decode each granule via read_granule
        let mut buf = vec![0u64; GRANULE_SIZE];
        let mut total_decoded = 0;

        for g in 0..reader.total_granules() {
            let count = reader.read_granule(g, &mut buf)?;
            let expected_start = g * GRANULE_SIZE;
            let expected_end = std::cmp::min(expected_start + GRANULE_SIZE, TOTAL_VALUES);
            let expected_slice = &test_vaddrs[expected_start..expected_end];

            assert_eq!(&buf[..count], expected_slice);
            total_decoded += count;

            // Range pruning verification: min and max must match perfectly
            let expected_min = *expected_slice.iter().min().unwrap_or(&0);
            let expected_max = *expected_slice.iter().max().unwrap_or(&0);
            assert_eq!(reader.min_scalar(g), Some(expected_min));
            assert_eq!(reader.max_scalar(g), Some(expected_max));

            // Verify range skipping logic:
            // Values strictly less than min or strictly greater than max can be skipped!
            let target_below = expected_min - 1;
            let target_above = expected_max + 1;
            assert!(target_below < reader.min_scalar(g).unwrap_or(0));
            assert!(target_above > reader.max_scalar(g).unwrap_or(0));
        }

        assert_eq!(total_decoded, TOTAL_VALUES);
        Ok(())
    }

    #[test]
    fn test_address_reader_out_of_bounds_errors() -> ExoResult<()> {
        let dir = tempdir()?;
        let file_path = dir.path().join("empty_col.exo.addr");

        {
            let writer = ExoAddressWriter::create(&file_path, 1024)?;
            writer.finish()?;
        }

        let reader = ExoAddressReader::open(&file_path)?;
        assert_eq!(reader.total_rows(), 0);
        assert_eq!(reader.total_granules(), 0);

        // Attempting to read granule 0 must return InvalidGranuleIndex
        match reader.scan_granule_slice(0) {
            Err(ExoError::InvalidGranuleIndex {
                index,
                total_granules,
            }) => {
                assert_eq!(index, 0);
                assert_eq!(total_granules, 0);
            }
            res => panic!("Expected InvalidGranuleIndex error, got: {:?}", res),
        }

        Ok(())
    }

    #[test]
    fn test_address_in_memory_roundtrip() -> ExoResult<()> {
        const GRANULE_SIZE: usize = 256;
        let test_vaddrs: Vec<u64> = (0..500).map(|i| i * 42).collect();

        let mut writer = ExoAddressWriter::create_in_memory(GRANULE_SIZE, 8)?;
        writer.append_batch(&test_vaddrs)?;
        let (rows, bytes) = writer.finish_into_bytes()?;
        assert_eq!(rows, 500);

        let reader = ExoAddressReader::from_bytes(bytes)?;
        assert_eq!(reader.total_rows(), 500);
        assert_eq!(reader.total_granules(), 2);

        for (i, &expected) in test_vaddrs.iter().enumerate() {
            assert_eq!(reader.read_vaddr(i as u64)?, expected);
        }

        Ok(())
    }
}
