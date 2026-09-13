//! Domain error types for the .exo storage engine.
//!
//! Enforces zero unwrap policy: all fallible operations return [`ExoResult`].

use thiserror::Error;

/// Result alias for all operations in `crates/exo`.
pub type ExoResult<T> = Result<T, ExoError>;

/// Master error enum for `.exo` format operations.
#[derive(Debug, Error)]
pub enum ExoError {
    /// Underlying I/O error.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// File magic bytes did not match the expected specification.
    #[error("Corrupted magic bytes: expected {expected:?}, found {found:?}")]
    CorruptedMagic {
        /// The magic byte string expected (e.g. "EXOA", "EXOV", "TOMB").
        expected: &'static str,
        /// The 4 bytes actually read from the file header.
        found: [u8; 4],
    },

    /// Format version not supported by this engine build.
    #[error("Unsupported .exo format version: {version}")]
    UnsupportedVersion {
        /// The version number read from the header.
        version: u16,
    },

    /// CRC32-Castagnoli checksum verification failed.
    #[error("CRC32-C checksum mismatch at offset {offset}: expected {expected:#010x}, calculated {actual:#010x}")]
    ChecksumMismatch {
        /// Expected checksum from block header/manifest.
        expected: u32,
        /// Calculated checksum from payload bytes.
        actual: u32,
        /// Byte offset in the file where corruption was detected.
        offset: u64,
    },

    /// Requested granule index is out of bounds.
    #[error("Invalid granule index {index}: file has {total_granules} granules")]
    InvalidGranuleIndex {
        /// The requested granule index.
        index: usize,
        /// Total granules present in the file.
        total_granules: usize,
    },

    /// Requested slot index is out of bounds for this part.
    #[error("Invalid slot index {slot}: part has {total_rows} rows")]
    InvalidSlotIndex {
        /// The requested slot index.
        slot: u32,
        /// Total rows present in the part.
        total_rows: u64,
    },

    /// Provided buffer is too small to receive decoded data.
    #[error("Buffer too small: required {required} bytes, but provided {provided} bytes")]
    BufferTooSmall {
        /// Required buffer capacity.
        required: usize,
        /// Provided buffer capacity.
        provided: usize,
    },

    /// Error during compression or decompression.
    #[error("Compression codec error: {0}")]
    CodecError(String),

    /// Arrow conversion or batch construction error.
    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    /// Tombstone bitmap serialization or deserialization failure.
    #[error("Tombstone mask error: {0}")]
    TombstoneError(String),
}
