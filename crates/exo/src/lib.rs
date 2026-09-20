//! Vein Exosome (`.exo`): Dual-Plane Columnar Storage Format Engine.
//!
//! Architected for exabyte-scale distributed analytics:
//! - Physical separation of Address Plane (`.exo.addr`) and Payload Vault (`.exo.vault`).
//! - 16-byte fixed distributed coordinates: `(vAddr: u64, part_id: u32, slot_id: u32)`.
//! - Zero-copy borrowed reads and SIMD-aligned structures.
//! - Strict zero-unwrap policy: all fallible operations return [`ExoResult`].

pub mod addr;
pub mod address_batch;
pub mod codec;
pub mod compactor;
pub mod coord;
pub mod error;
pub mod part;
pub mod predicate;
pub mod storage_class;
pub mod tombstone;
pub mod traits;
pub mod vault;

pub use addr::{
    AddressGranuleMark, AddressHeader, ExoAddressReader, ExoAddressWriter, CODEC_FOR, CODEC_RAW,
};
pub use address_batch::AddressBatch;
pub use codec::ForCodec;
pub use compactor::{CompactionStats, PartCompactor};
pub use coord::{
    AddressCoordinate, GlobalRowCoord, JoinToken, NULL_VADDR, ToCanonicalAddress,
    canonical_from_f32_native, canonical_from_f64, canonical_from_i16_native,
    canonical_from_i32, canonical_from_i32_native, canonical_from_i64, canonical_from_i8_native,
    f32_from_canonical_native, f64_from_canonical, i16_from_canonical_native,
    i32_from_canonical, i32_from_canonical_native, i64_from_canonical, i8_from_canonical_native,
    u32_from_canonical, u64_from_canonical,
};
pub use error::{ExoError, ExoResult};
pub use part::{PartBufferDescriptor, PartDataSource, PartDescriptor, PartReader, PartWriter};
pub use predicate::{AddressFilter, AddressPredicate, AddressPredicateEvaluator};
pub use storage_class::{
    address_preserves_order, classify_storage, classify_type_name, data_type_uses_vault,
    requires_collision_verification, require_storage_class, ExoStorageClass,
};
pub use tombstone::{TombstoneHeader, TombstoneMask};
pub use traits::{
    AddressReader, AddressWriter, ExactValueCodec, StorageBuffer, TombstoneStore, VaultReader,
    VaultWriter,
};
pub use vault::{ExoVaultReader, ExoVaultWriter, VaultGranuleMark, VaultHeader};
