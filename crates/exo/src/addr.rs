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
use crate::traits::{AddressReader, AddressWriter};

pub const ADDR_HEADER_MAGIC: &[u8; 4] = b"EXOA";
pub const ADDR_TRAILER_MAGIC: &[u8; 4] = b"AEOF";
pub const ADDR_FORMAT_VERSION: u16 = 1;
pub const DEFAULT_GRANULE_SIZE: usize = 8192;
pub const HEADER_SIZE: usize = 64;
pub const MARK_SIZE: usize = 32;
pub const TRAILER_SIZE: usize = 8;

pub const CODEC_RAW: u16 = 0;
pub const CODEC_FOR: u16 = 1;

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
    /// Minimum vAddr in this granule (used for range skipping).
    pub min_vaddr: u64,
    /// Maximum vAddr in this granule (used for range skipping).
    pub max_vaddr: u64,
}

impl AddressGranuleMark {
    pub fn encode(&self) -> [u8; MARK_SIZE] {
        let mut buf = [0u8; MARK_SIZE];
        buf[0..8].copy_from_slice(&self.byte_offset.to_le_bytes());
        buf[8..12].copy_from_slice(&self.compressed_len.to_le_bytes());
        buf[12..16].copy_from_slice(&self.uncompressed_len.to_le_bytes());
        buf[16..24].copy_from_slice(&self.min_vaddr.to_le_bytes());
        buf[24..32].copy_from_slice(&self.max_vaddr.to_le_bytes());
        buf
    }

    pub fn decode(bytes: &[u8; MARK_SIZE]) -> Self {
        let byte_offset = u64::from_le_bytes(bytes[0..8].try_into().unwrap_or([0u8; 8]));
        let compressed_len = u32::from_le_bytes(bytes[8..12].try_into().unwrap_or([0u8; 4]));
        let uncompressed_len = u32::from_le_bytes(bytes[12..16].try_into().unwrap_or([0u8; 4]));
        let min_vaddr = u64::from_le_bytes(bytes[16..24].try_into().unwrap_or([0u8; 8]));
        let max_vaddr = u64::from_le_bytes(bytes[24..32].try_into().unwrap_or([0u8; 8]));
        Self {
            byte_offset,
            compressed_len,
            uncompressed_len,
            min_vaddr,
            max_vaddr,
        }
    }
}

/// Writer for generating `.exo.addr` files.
pub struct ExoAddressWriter {
    file: File,
    path: PathBuf,
    granule_size: usize,
    codec_flags: u16,
    buffer: Vec<u64>,
    marks: Vec<AddressGranuleMark>,
    total_rows: u64,
}

impl ExoAddressWriter {
    /// Creates a new `.exo.addr` writer at the specified path with default RAW codec.
    pub fn create(path: &Path, granule_size: usize) -> ExoResult<Self> {
        Self::create_with_codec(path, granule_size, CODEC_RAW)
    }

    /// Creates a new `.exo.addr` writer with Frame-of-Reference (FoR) bit-packing enabled.
    pub fn create_compressed(path: &Path, granule_size: usize) -> ExoResult<Self> {
        Self::create_with_codec(path, granule_size, CODEC_FOR)
    }

    /// Creates a new `.exo.addr` writer with explicit codec flags.
    pub fn create_with_codec(path: &Path, granule_size: usize, codec_flags: u16) -> ExoResult<Self> {
        let mut file = File::create(path)?;
        // Write blank 64-byte header placeholder
        let placeholder = [0u8; HEADER_SIZE];
        file.write_all(&placeholder)?;

        Ok(Self {
            file,
            path: path.to_path_buf(),
            granule_size,
            codec_flags,
            buffer: Vec::with_capacity(granule_size),
            marks: Vec::new(),
            total_rows: 0,
        })
    }

    /// Returns the path to the file being written.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Flushes the current in-memory buffer into a discrete physical granule on disk.
    fn flush_granule(&mut self) -> ExoResult<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }

        let byte_offset = self.file.stream_position()?;
        let min_vaddr = self.buffer.iter().copied().min().unwrap_or(0);
        let max_vaddr = self.buffer.iter().copied().max().unwrap_or(0);
        let uncompressed_len = (self.buffer.len() * 8) as u32;

        let compressed_len = if self.codec_flags == CODEC_FOR {
            let (_, bit_width, packed_data) = ForCodec::encode_granule(&self.buffer);
            self.file.write_all(&[bit_width])?;
            self.file.write_all(&packed_data)?;
            (1 + packed_data.len()) as u32
        } else {
            // Convert slice of u64 to byte slice
            let byte_slice = unsafe {
                std::slice::from_raw_parts(
                    self.buffer.as_ptr() as *const u8,
                    self.buffer.len() * 8,
                )
            };
            self.file.write_all(byte_slice)?;
            uncompressed_len
        };

        self.marks.push(AddressGranuleMark {
            byte_offset,
            compressed_len,
            uncompressed_len,
            min_vaddr,
            max_vaddr,
        });

        self.buffer.clear();
        Ok(())
    }
}

impl AddressWriter for ExoAddressWriter {
    fn append_vaddr(&mut self, vaddr: u64) -> ExoResult<()> {
        self.buffer.push(vaddr);
        self.total_rows += 1;

        if self.buffer.len() >= self.granule_size {
            self.flush_granule()?;
        }
        Ok(())
    }

    fn append_batch(&mut self, vaddrs: &[u64]) -> ExoResult<()> {
        let mut remaining = vaddrs;
        while !remaining.is_empty() {
            let capacity_left = self.granule_size - self.buffer.len();
            let take = std::cmp::min(capacity_left, remaining.len());
            self.buffer.extend_from_slice(&remaining[..take]);
            self.total_rows += take as u64;
            remaining = &remaining[take..];

            if self.buffer.len() >= self.granule_size {
                self.flush_granule()?;
            }
        }
        Ok(())
    }

    fn finish(mut self) -> ExoResult<u64> {
        // Flush any lingering values in the final granule
        self.flush_granule()?;

        let marks_offset = self.file.stream_position()?;

        // Write marks table
        for mark in &self.marks {
            self.file.write_all(&mark.encode())?;
        }

        // Write trailer (marks count + AEOF magic)
        let marks_count = self.marks.len() as u32;
        self.file.write_all(&marks_count.to_le_bytes())?;
        self.file.write_all(ADDR_TRAILER_MAGIC)?;

        // Seek back to start and write finalized header
        self.file.seek(SeekFrom::Start(0))?;

        let header = AddressHeader {
            magic: *ADDR_HEADER_MAGIC,
            version: ADDR_FORMAT_VERSION,
            codec_flags: self.codec_flags,
            total_rows: self.total_rows,
            granule_size: self.granule_size as u32,
            total_granules: self.marks.len() as u32,
            marks_offset,
            checksum: 0,
        };

        self.file.write_all(&header.encode())?;
        self.file.flush()?;

        Ok(self.total_rows)
    }
}

/// Zero-copy reader for `.exo.addr` files backed by memory mapping.
pub struct ExoAddressReader {
    _file: File,
    mmap: memmap2::Mmap,
    header: AddressHeader,
    marks: Vec<AddressGranuleMark>,
}

impl ExoAddressReader {
    /// Opens an `.exo.addr` file and loads its metadata.
    pub fn open(path: &Path) -> ExoResult<Self> {
        let file = OpenOptions::new().read(true).open(path)?;
        let metadata = file.metadata()?;
        let file_len = metadata.len() as usize;

        if file_len < HEADER_SIZE + TRAILER_SIZE {
            return Err(ExoError::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                format!("File {:?} too small to be a valid .exo.addr file", path),
            )));
        }

        // Create zero-copy memory map
        let mmap = unsafe { memmap2::MmapOptions::new().map(&file)? };

        // Decode and validate header
        let mut header_buf = [0u8; HEADER_SIZE];
        header_buf.copy_from_slice(&mmap[0..HEADER_SIZE]);
        let header = AddressHeader::decode(&header_buf)?;

        // Validate trailer magic at tail
        let trailer_offset = file_len - TRAILER_SIZE;
        let mut trailer_magic = [0u8; 4];
        trailer_magic.copy_from_slice(&mmap[trailer_offset + 4..file_len]);
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
                "Marks table offset exceeds file bounds",
            )));
        }

        let mut marks = Vec::with_capacity(total_granules);
        for chunk in mmap[marks_start..marks_end].chunks_exact(MARK_SIZE) {
            let mut mark_buf = [0u8; MARK_SIZE];
            mark_buf.copy_from_slice(chunk);
            marks.push(AddressGranuleMark::decode(&mark_buf));
        }

        Ok(Self {
            _file: file,
            mmap,
            header,
            marks,
        })
    }

    /// Returns the codec flags configured for this address column.
    #[inline(always)]
    pub fn codec_flags(&self) -> u16 {
        self.header.codec_flags
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
        let num_values = (mark.uncompressed_len / 8) as usize;

        if out_buffer.len() < num_values {
            return Err(ExoError::BufferTooSmall {
                required: num_values,
                provided: out_buffer.len(),
            });
        }

        if self.header.codec_flags == CODEC_FOR {
            let start = mark.byte_offset as usize;
            let end = start + (mark.compressed_len as usize);
            if end > self.mmap.len() || end <= start {
                return Err(ExoError::Io(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "FoR granule offset exceeds memory map boundary",
                )));
            }
            let granule_bytes = &self.mmap[start..end];
            let bit_width = granule_bytes[0];
            let packed_data = &granule_bytes[1..];
            ForCodec::decode_granule(packed_data, mark.min_vaddr, bit_width, num_values, out_buffer)
        } else {
            let slice = self.scan_granule_slice(granule_idx)?;
            out_buffer[..slice.len()].copy_from_slice(slice);
            Ok(slice.len())
        }
    }

    /// Zero-copy borrowed slice directly into mmap memory for RAW uncompressed granules!
    fn scan_granule_slice(&self, granule_idx: usize) -> ExoResult<&[u64]> {
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
        let start = mark.byte_offset as usize;
        let byte_len = mark.uncompressed_len as usize;
        let end = start + byte_len;

        if end > self.mmap.len() {
            return Err(ExoError::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "Granule offset exceeds memory map boundary",
            )));
        }

        let raw_bytes = &self.mmap[start..end];
        let num_u64s = byte_len / 8;

        // Verify 8-byte alignment for safe zero-copy slice casting
        let ptr = raw_bytes.as_ptr();
        if (ptr as usize) % std::mem::align_of::<u64>() != 0 {
            return Err(ExoError::CodecError(
                "Mmap pointer is unaligned for u64 array".to_string(),
            ));
        }

        // Safety:
        // 1. Pointer is aligned to 8 bytes.
        // 2. Memory is valid for the lifetime of `self.mmap`.
        // 3. Size is exactly `num_u64s * 8`.
        let u64_slice = unsafe { std::slice::from_raw_parts(ptr as *const u64, num_u64s) };
        Ok(u64_slice)
    }

    #[inline(always)]
    fn min_scalar(&self, granule_idx: usize) -> Option<u64> {
        self.marks.get(granule_idx).map(|m| m.min_vaddr)
    }

    #[inline(always)]
    fn max_scalar(&self, granule_idx: usize) -> Option<u64> {
        self.marks.get(granule_idx).map(|m| m.max_vaddr)
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
        if self.codec_flags() == CODEC_RAW {
            let slice = self.scan_granule_slice(g_idx)?;
            Ok(slice[offset])
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
            res => panic!("Expected CodecError for scan_granule_slice on FoR, got: {:?}", res),
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
            Err(ExoError::InvalidGranuleIndex { index, total_granules }) => {
                assert_eq!(index, 0);
                assert_eq!(total_granules, 0);
            }
            res => panic!("Expected InvalidGranuleIndex error, got: {:?}", res),
        }

        Ok(())
    }
}
