//! Core architectural abstractions and traits for the .exo engine.
//!
//! Provides modular interfaces for storage writers, zero-copy readers,
//! and late materialization decoders.

use crate::coord::ToCanonicalAddress;
use crate::error::ExoResult;

/// Encodes a primitive value into its canonical address and exact vault representation.
///
/// Implementations must use a stable, little-endian exact encoding. Values below 64 bits
/// remain address-only; 64-bit values are persisted in both planes.
pub trait ExactValueCodec: ToCanonicalAddress {
    const WIDTH: u8;

    fn encode_exact_into(&self, output: &mut [u8]) -> ExoResult<()>;
}

macro_rules! impl_exo_value_encoding {
    ($ty:ty, $width:expr, $encode:expr) => {
        impl ExactValueCodec for $ty {
            const WIDTH: u8 = $width;

            #[inline]
            fn encode_exact_into(&self, output: &mut [u8]) -> ExoResult<()> {
                if output.len() != Self::WIDTH as usize {
                    return Err(crate::error::ExoError::CodecError(format!(
                        "invalid exact-value output width: expected {}, got {}",
                        Self::WIDTH,
                        output.len()
                    )));
                }
                output.copy_from_slice(&$encode(self));
                Ok(())
            }
        }
    };
}

impl_exo_value_encoding!(i8, 1, |value: &i8| [*value as u8]);
impl_exo_value_encoding!(u8, 1, |value: &u8| [*value]);
impl_exo_value_encoding!(i16, 2, |value: &i16| value.to_le_bytes());
impl_exo_value_encoding!(u16, 2, |value: &u16| value.to_le_bytes());
impl_exo_value_encoding!(i32, 4, |value: &i32| value.to_le_bytes());
impl_exo_value_encoding!(u32, 4, |value: &u32| value.to_le_bytes());
impl_exo_value_encoding!(f32, 4, |value: &f32| value.to_le_bytes());
impl_exo_value_encoding!(i64, 8, |value: &i64| value.to_le_bytes());
impl_exo_value_encoding!(u64, 8, |value: &u64| value.to_le_bytes());
impl_exo_value_encoding!(f64, 8, |value: &f64| value.to_le_bytes());
impl_exo_value_encoding!(i128, 16, |value: &i128| value.to_le_bytes());
impl_exo_value_encoding!(
    arrow::datatypes::i256,
    32,
    |value: &arrow::datatypes::i256| value.to_le_bytes()
);

impl ExactValueCodec for bool {
    const WIDTH: u8 = 1;

    #[inline]
    fn encode_exact_into(&self, output: &mut [u8]) -> ExoResult<()> {
        if output.len() != 1 {
            return Err(crate::error::ExoError::CodecError(format!(
                "invalid exact-value output width: expected 1, got {}",
                output.len()
            )));
        }
        output[0] = u8::from(*self);
        Ok(())
    }
}

/// Unified storage buffer that can be backed either by a zero-copy memory map
/// or an in-memory byte buffer (e.g. decoded from VFS blocks).
#[derive(Debug)]
pub enum StorageBuffer {
    Mmap(memmap2::Mmap),
    Bytes(Vec<u8>),
}

impl std::ops::Deref for StorageBuffer {
    type Target = [u8];
    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        match self {
            StorageBuffer::Mmap(m) => &m[..],
            StorageBuffer::Bytes(b) => &b[..],
        }
    }
}

/// Stream-oriented writer for the Address Plane (`.exo.addr`).
pub trait AddressWriter {
    /// Appends a single 64-bit canonical address.
    fn append_vaddr(&mut self, vaddr: u64) -> ExoResult<()>;

    /// Appends one canonical value with an explicit validity bit.
    fn append_nullable_vaddr(&mut self, vaddr: u64, valid: bool) -> ExoResult<()>;

    /// Appends a batch of canonical addresses without copying.
    fn append_batch(&mut self, vaddrs: &[u64]) -> ExoResult<()>;

    /// Appends canonical values together with one validity bit per row.
    fn append_nullable_batch(&mut self, vaddrs: &[u64], validity: &[bool]) -> ExoResult<()>;

    /// Flushes any pending granule buffers and writes tail marks table.
    /// Returns total rows written to the column.
    fn finish(self) -> ExoResult<u64>;
}

/// Zero-copy reader for the Address Plane (`.exo.addr`).
pub trait AddressReader: Send + Sync {
    /// Total rows in this part's column.
    fn total_rows(&self) -> u64;

    /// Total granules in this part's column.
    fn total_granules(&self) -> usize;

    /// Configured granule size (e.g. 8192).
    fn granule_size(&self) -> usize;

    /// Decodes a granule into a provided buffer, returning the number of decoded values.
    fn read_granule(&self, granule_idx: usize, out_buffer: &mut [u64]) -> ExoResult<usize>;

    /// Zero-copy slice access to raw vAddrs for mmap'd uncompressed granules.
    fn scan_granule_slice(&self, granule_idx: usize) -> ExoResult<&[u64]>;

    /// Returns the minimum scalar value for this granule (used for range skipping).
    fn min_scalar(&self, granule_idx: usize) -> Option<u64>;

    /// Returns the maximum scalar value for this granule (used for range skipping).
    fn max_scalar(&self, granule_idx: usize) -> Option<u64>;

    /// Reads a single canonical address at the specified row index.
    fn read_vaddr(&self, row_idx: u64) -> ExoResult<u64>;

    /// Returns whether the specified row contains a non-null value.
    fn is_valid(&self, row_idx: u64) -> ExoResult<bool>;
}

/// Chunk-oriented writer for the Payload Vault (`.exo.vault`).
pub trait VaultWriter {
    /// Appends a raw byte payload for a row.
    fn append_bytes(&mut self, payload: &[u8]) -> ExoResult<()>;

    /// Appends an exact fixed-width value payload.
    #[inline]
    fn append_fixed_bytes(&mut self, payload: &[u8]) -> ExoResult<()> {
        self.append_bytes(payload)
    }

    /// Appends an optional exact fixed-width value payload.
    #[inline]
    fn append_opt_fixed_bytes(&mut self, payload: Option<&[u8]>) -> ExoResult<()> {
        match payload {
            Some(payload) => self.append_fixed_bytes(payload),
            None => self.append_null(),
        }
    }

    /// Appends a string value for a row.
    fn append_str(&mut self, val: &str) -> ExoResult<()>;

    /// Appends a SQL NULL value for a row.
    fn append_null(&mut self) -> ExoResult<()>;

    /// Appends an optional raw byte payload (None is SQL NULL).
    fn append_opt_bytes(&mut self, payload: Option<&[u8]>) -> ExoResult<()>;

    /// Appends an optional string value (None is SQL NULL).
    fn append_opt_str(&mut self, val: Option<&str>) -> ExoResult<()>;

    /// Appends a slice of string values.
    fn append_batch_strings(&mut self, vals: &[String]) -> ExoResult<()>;

    /// Appends a slice of optional string values.
    fn append_batch_opt_strings(&mut self, vals: &[Option<String>]) -> ExoResult<()>;

    /// Flushes any pending granules and writes the tail marks table.
    /// Returns total rows written to the vault.
    fn finish(self) -> ExoResult<u64>;
}

/// Dereferencing reader for the Payload Vault (`.exo.vault`).
pub trait VaultReader: Send {
    /// Total rows in this part's vault.
    fn total_rows(&self) -> u64;

    /// Returns true if the slot in this part is SQL NULL.
    fn is_null(&mut self, slot_id: u32) -> ExoResult<bool>;

    /// O(1) slot dereferencing returning borrowed string slice.
    fn read_str_slot(&mut self, slot_id: u32) -> ExoResult<&str>;

    /// O(1) slot dereferencing returning borrowed raw byte slice.
    fn read_bytes_slot(&mut self, slot_id: u32) -> ExoResult<&[u8]>;

    /// O(1) slot dereferencing for an exact fixed-width value payload.
    #[inline]
    fn read_fixed_bytes_slot(&mut self, slot_id: u32) -> ExoResult<&[u8]> {
        self.read_bytes_slot(slot_id)
    }

    /// O(1) slot dereferencing returning optional borrowed string slice (None if NULL).
    fn read_opt_str_slot(&mut self, slot_id: u32) -> ExoResult<Option<&str>>;

    /// O(1) slot dereferencing returning optional borrowed raw byte slice (None if NULL).
    fn read_opt_bytes_slot(&mut self, slot_id: u32) -> ExoResult<Option<&[u8]>>;

    /// Vectorized materialization: extracts all requested slots directly into an Arrow StringBuilder.
    fn materialize_string_batch(
        &mut self,
        slots: &[u32],
        builder: &mut arrow::array::StringBuilder,
    ) -> ExoResult<()>;
}

/// Abstraction for checking and updating deleted slot coordinates.
pub trait TombstoneStore: Send + Sync {
    /// Returns true if the slot in this part has been tombstoned (deleted or superseded).
    fn is_deleted(&self, slot_id: u32) -> bool;

    /// Marks a slot as deleted.
    fn mark_deleted(&mut self, slot_id: u32);

    /// Total count of tombstoned slots in this part.
    fn deleted_count(&self) -> u64;
}
