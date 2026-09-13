//! Vein Exosome (`.exo`): Dual-Plane Columnar Storage Format Engine.
//!
//! Architected for exabyte-scale distributed analytics:
//! - Physical separation of Address Plane (`.exo.addr`) and Payload Vault (`.exo.vault`).
//! - 16-byte fixed distributed coordinates: `(vAddr: u64, part_id: u32, slot_id: u32)`.
//! - Zero-copy borrowed reads and SIMD-aligned structures.
//! - Strict zero-unwrap policy: all fallible operations return [`ExoResult`].

pub mod addr;
pub mod codec;
pub mod compactor;
pub mod coord;
pub mod error;
pub mod part;
pub mod tombstone;
pub mod traits;
pub mod vault;

pub use addr::{
    AddressGranuleMark, AddressHeader, ExoAddressReader, ExoAddressWriter, CODEC_FOR, CODEC_RAW,
};
pub use codec::ForCodec;
pub use compactor::{CompactionStats, PartCompactor};
pub use coord::{
    AddressCoordinate, NULL_VADDR, ToCanonicalAddress, canonical_from_f64, f64_from_canonical,
};
pub use error::{ExoError, ExoResult};
pub use part::{PartDescriptor, PartReader, PartWriter};
pub use tombstone::{TombstoneHeader, TombstoneMask};
pub use traits::{
    AddressReader, AddressWriter, TombstoneStore, VaultReader, VaultWriter,
};
pub use vault::{ExoVaultReader, ExoVaultWriter, VaultGranuleMark, VaultHeader};
