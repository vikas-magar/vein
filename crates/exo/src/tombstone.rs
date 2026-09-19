//! Address-Plane Tombstone Mask (`tombstones.exo.tomb`) physical implementation.
//!
//! Stores deleted and superseded slot IDs for an immutable Part using
//! an IEEE Roaring Bitmap standard stream.
//!
//! Enables zero-I/O lightweight deletes: deletes and out-of-place updates
//! never touch, read, or rewrite the Payload Vault (`.exo.vault`).
//! Strict zero-unwrap policy is enforced.

use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::Path;

use roaring::RoaringBitmap;

use crate::error::{ExoError, ExoResult};
use crate::traits::TombstoneStore;

pub const TOMB_HEADER_MAGIC: &[u8; 4] = b"TOMB";
pub const TOMB_FORMAT_VERSION: u16 = 1;
pub const TOMB_HEADER_SIZE: usize = 32;

/// Fixed 32-byte header for `.exo.tomb` files.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TombstoneHeader {
    pub magic: [u8; 4],
    pub version: u16,
    pub flags: u16,
    pub deleted_count: u64,
    pub payload_bytes_len: u64,
    pub checksum: u32,
}

impl TombstoneHeader {
    pub fn encode(&self) -> [u8; TOMB_HEADER_SIZE] {
        let mut buf = [0u8; TOMB_HEADER_SIZE];
        buf[0..4].copy_from_slice(&self.magic);
        buf[4..6].copy_from_slice(&self.version.to_le_bytes());
        buf[6..8].copy_from_slice(&self.flags.to_le_bytes());
        buf[8..16].copy_from_slice(&self.deleted_count.to_le_bytes());
        buf[16..24].copy_from_slice(&self.payload_bytes_len.to_le_bytes());
        buf[24..28].copy_from_slice(&self.checksum.to_le_bytes());
        buf
    }

    pub fn decode(bytes: &[u8; TOMB_HEADER_SIZE]) -> ExoResult<Self> {
        let mut magic = [0u8; 4];
        magic.copy_from_slice(&bytes[0..4]);
        if &magic != TOMB_HEADER_MAGIC {
            return Err(ExoError::CorruptedMagic {
                expected: "TOMB",
                found: magic,
            });
        }

        let version = u16::from_le_bytes(bytes[4..6].try_into().unwrap_or([0u8; 2]));
        if version != TOMB_FORMAT_VERSION {
            return Err(ExoError::UnsupportedVersion { version });
        }

        let flags = u16::from_le_bytes(bytes[6..8].try_into().unwrap_or([0u8; 2]));
        let deleted_count = u64::from_le_bytes(bytes[8..16].try_into().unwrap_or([0u8; 8]));
        let payload_bytes_len = u64::from_le_bytes(bytes[16..24].try_into().unwrap_or([0u8; 8]));
        let checksum = u32::from_le_bytes(bytes[24..28].try_into().unwrap_or([0u8; 4]));

        Ok(Self {
            magic,
            version,
            flags,
            deleted_count,
            payload_bytes_len,
            checksum,
        })
    }
}

/// Address-plane mask tracking deleted or superseded slot IDs for a Part.
#[derive(Default, Clone, Debug, PartialEq, Eq)]
pub struct TombstoneMask {
    bitmap: RoaringBitmap,
}

impl TombstoneMask {
    /// Creates a new empty tombstone mask.
    pub fn new() -> Self {
        Self {
            bitmap: RoaringBitmap::new(),
        }
    }

    /// Wraps an existing RoaringBitmap.
    pub fn from_bitmap(bitmap: RoaringBitmap) -> Self {
        Self { bitmap }
    }

    /// Marks a single slot as deleted.
    #[inline(always)]
    pub fn mark_deleted(&mut self, slot_id: u32) {
        self.bitmap.insert(slot_id);
    }

    /// Marks a contiguous range of slots as deleted.
    pub fn mark_range_deleted(&mut self, range: std::ops::Range<u32>) {
        self.bitmap.insert_range(range);
    }

    /// Returns true if the slot is marked deleted.
    #[inline(always)]
    pub fn is_deleted(&self, slot_id: u32) -> bool {
        self.bitmap.contains(slot_id)
    }

    /// Total number of deleted slots in this mask.
    #[inline(always)]
    pub fn deleted_count(&self) -> u64 {
        self.bitmap.len()
    }

    /// Returns true if no slots are deleted.
    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.bitmap.is_empty()
    }

    /// Serializes the tombstone mask to in-memory bytes with header and CRC32-C.
    pub fn to_bytes(&self) -> ExoResult<Vec<u8>> {
        let mut payload_bytes = Vec::new();
        self.bitmap
            .serialize_into(&mut payload_bytes)
            .map_err(|e| ExoError::TombstoneError(e.to_string()))?;

        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&payload_bytes);
        let checksum = hasher.finalize();

        let header = TombstoneHeader {
            magic: *TOMB_HEADER_MAGIC,
            version: TOMB_FORMAT_VERSION,
            flags: 0,
            deleted_count: self.bitmap.len(),
            payload_bytes_len: payload_bytes.len() as u64,
            checksum,
        };

        let mut out = Vec::with_capacity(TOMB_HEADER_SIZE + payload_bytes.len());
        out.extend_from_slice(&header.encode());
        out.extend_from_slice(&payload_bytes);
        Ok(out)
    }

    /// Deserializes a tombstone mask from in-memory bytes.
    pub fn from_bytes(bytes: &[u8]) -> ExoResult<Self> {
        if bytes.is_empty() {
            return Ok(Self::new());
        }
        if bytes.len() < TOMB_HEADER_SIZE {
            return Err(ExoError::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "Tombstone buffer too small",
            )));
        }

        let mut header_buf = [0u8; TOMB_HEADER_SIZE];
        header_buf.copy_from_slice(&bytes[..TOMB_HEADER_SIZE]);
        let header = TombstoneHeader::decode(&header_buf)?;

        let payload_len = header.payload_bytes_len as usize;
        if TOMB_HEADER_SIZE + payload_len > bytes.len() {
            return Err(ExoError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Tombstone payload length exceeds buffer boundary",
            )));
        }

        let payload_bytes = &bytes[TOMB_HEADER_SIZE..TOMB_HEADER_SIZE + payload_len];
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(payload_bytes);
        let actual_checksum = hasher.finalize();

        if actual_checksum != header.checksum {
            return Err(ExoError::ChecksumMismatch {
                expected: header.checksum,
                actual: actual_checksum,
                offset: TOMB_HEADER_SIZE as u64,
            });
        }

        let bitmap = RoaringBitmap::deserialize_from(payload_bytes)
            .map_err(|e| ExoError::TombstoneError(e.to_string()))?;

        Ok(Self { bitmap })
    }

    /// Serializes the tombstone mask to disk with a 32-byte header and CRC32-C.
    ///
    /// Implements atomic crash-safe durability: writes to a unique temporary file,
    /// calls `sync_all()` to flush dirty OS page cache to physical disk, and atomically
    /// renames the temporary file to the final destination path.
    ///
    /// Returns the total bytes written to the file.
    pub fn save(&self, path: &Path) -> ExoResult<usize> {
        let file_bytes = self.to_bytes()?;

        // Determine parent directory and unique temporary file path
        let parent = path.parent().unwrap_or_else(|| Path::new("."));
        let file_name = path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("tombstones.exo.tomb");

        static TMP_COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let count = TMP_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let tmp_path = parent.join(format!(".{}.tmp.{}.{}", file_name, std::process::id(), count));

        let write_res = (|| -> ExoResult<()> {
            let mut file = File::create(&tmp_path)?;
            file.write_all(&file_bytes)?;
            file.sync_all()?;
            Ok(())
        })();

        if let Err(e) = write_res {
            let _ = std::fs::remove_file(&tmp_path);
            return Err(e);
        }

        if let Err(e) = std::fs::rename(&tmp_path, path) {
            let _ = std::fs::remove_file(&tmp_path);
            return Err(ExoError::Io(e));
        }

        // Fsync parent directory to ensure directory entry commit
        if let Ok(parent_dir) = File::open(parent) {
            let _ = parent_dir.sync_all();
        }

        Ok(file_bytes.len())
    }

    /// Loads a tombstone mask from disk, verifying header magic and CRC32-C checksum.
    ///
    /// If the file does not exist, returns a default empty mask.
    pub fn load(path: &Path) -> ExoResult<Self> {
        if !path.exists() {
            return Ok(Self::new());
        }

        let mut file = OpenOptions::new().read(true).open(path)?;
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)?;
        Self::from_bytes(&bytes)
    }
}

impl TombstoneStore for TombstoneMask {
    #[inline(always)]
    fn is_deleted(&self, slot_id: u32) -> bool {
        self.bitmap.contains(slot_id)
    }

    #[inline(always)]
    fn mark_deleted(&mut self, slot_id: u32) {
        self.bitmap.insert(slot_id);
    }

    #[inline(always)]
    fn deleted_count(&self) -> u64 {
        self.bitmap.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Seek, SeekFrom};
    use tempfile::tempdir;

    #[test]
    fn test_tombstone_mask_roundtrip_and_membership() -> ExoResult<()> {
        let dir = tempdir()?;
        let tomb_path = dir.path().join("tombstones.exo.tomb");

        let mut mask = TombstoneMask::new();
        assert!(mask.is_empty());
        assert_eq!(mask.deleted_count(), 0);

        // Add sparse deletions
        mask.mark_deleted(5);
        mask.mark_deleted(42);
        mask.mark_deleted(999);

        // Add dense range deletions (5,000 slots)
        mask.mark_range_deleted(10_000..15_000);

        assert_eq!(mask.deleted_count(), 3 + 5000);
        assert!(mask.is_deleted(5));
        assert!(mask.is_deleted(42));
        assert!(mask.is_deleted(999));
        assert!(mask.is_deleted(10_000));
        assert!(mask.is_deleted(12_345));
        assert!(mask.is_deleted(14_999));
        assert!(!mask.is_deleted(15_000));
        assert!(!mask.is_deleted(0));
        assert!(!mask.is_deleted(43));

        // Save to disk
        let written = mask.save(&tomb_path)?;
        assert!(written > TOMB_HEADER_SIZE);

        // Reload from disk
        let loaded = TombstoneMask::load(&tomb_path)?;
        assert_eq!(loaded.deleted_count(), 3 + 5000);
        assert_eq!(mask, loaded);

        Ok(())
    }

    #[test]
    fn test_tombstone_non_existent_path_returns_empty() -> ExoResult<()> {
        let dir = tempdir()?;
        let non_existent = dir.path().join("does_not_exist.exo.tomb");

        let mask = TombstoneMask::load(&non_existent)?;
        assert!(mask.is_empty());
        assert_eq!(mask.deleted_count(), 0);
        assert!(!mask.is_deleted(42));
        Ok(())
    }

    #[test]
    fn test_tombstone_checksum_corruption_detection() -> ExoResult<()> {
        let dir = tempdir()?;
        let tomb_path = dir.path().join("corrupted.exo.tomb");

        let mut mask = TombstoneMask::new();
        mask.mark_deleted(100);
        mask.save(&tomb_path)?;

        // Corrupt 1 byte in the payload
        let mut file = OpenOptions::new().read(true).write(true).open(&tomb_path)?;
        file.seek(SeekFrom::Start((TOMB_HEADER_SIZE + 2) as u64))?;
        file.write_all(&[0xFF])?;
        file.flush()?;

        // Reload should fail with ChecksumMismatch
        match TombstoneMask::load(&tomb_path) {
            Err(ExoError::ChecksumMismatch { .. }) => {}
            res => panic!("Expected ChecksumMismatch error, got: {:?}", res),
        }

        Ok(())
    }
}
