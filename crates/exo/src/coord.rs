//! Physical Address Coordinate and Canonical Address Engine.
//!
//! Every row value in Vein is addressed via a 16-byte distributed coordinate:
//! `(vAddr: u64, part_id: u32, slot_id: u32)`.

use std::mem::{align_of, size_of};

/// Fixed 16-byte coordinate representing an address in distributed space.
///
/// Perfectly aligned to 16 bytes for SIMD register loads and lock-free streaming.
#[repr(C, align(16))]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub struct AddressCoordinate {
    /// Canonical 64-bit value address (WHAT the value is).
    pub vaddr: u64,
    /// Identifier of the Part (WHICH immutable part).
    pub part_id: u32,
    /// Row offset within the part (0 .. N-1) (WHERE in the part).
    pub slot_id: u32,
}

const _: () = assert!(size_of::<AddressCoordinate>() == 16);
const _: () = assert!(align_of::<AddressCoordinate>() == 16);

impl AddressCoordinate {
    /// Creates a new 16-byte address coordinate.
    #[inline(always)]
    pub const fn new(vaddr: u64, part_id: u32, slot_id: u32) -> Self {
        Self {
            vaddr,
            part_id,
            slot_id,
        }
    }

    /// Zero-copy cast to 16-byte byte array for zero-overhead network streaming.
    #[inline(always)]
    pub fn as_bytes(&self) -> &[u8; 16] {
        // Safety: AddressCoordinate is repr(C, align(16)) and exactly 16 bytes with no padding
        unsafe { &*(self as *const Self as *const [u8; 16]) }
    }

    /// Zero-copy reconstruction from 16-byte wire chunk.
    #[inline(always)]
    pub fn from_bytes(bytes: &[u8; 16]) -> Self {
        let vaddr = u64::from_le_bytes(bytes[0..8].try_into().unwrap_or([0u8; 8]));
        let part_id = u32::from_le_bytes(bytes[8..12].try_into().unwrap_or([0u8; 4]));
        let slot_id = u32::from_le_bytes(bytes[12..16].try_into().unwrap_or([0u8; 4]));
        Self {
            vaddr,
            part_id,
            slot_id,
        }
    }
}

/// Trait for zero-copy canonical address derivation across arbitrary data types.
pub trait ToCanonicalAddress {
    /// Maps the value into a deterministic 64-bit canonical address.
    fn to_canonical(&self) -> u64;
}

/// Deterministic canonical hash seed ("VEINEXOS").
pub const CANONICAL_SEED: u64 = 0x5645494E_45584F53;

impl ToCanonicalAddress for str {
    #[inline(always)]
    fn to_canonical(&self) -> u64 {
        twox_hash::XxHash64::oneshot(CANONICAL_SEED, self.as_bytes())
    }
}

impl ToCanonicalAddress for [u8] {
    #[inline(always)]
    fn to_canonical(&self) -> u64 {
        twox_hash::XxHash64::oneshot(CANONICAL_SEED, self)
    }
}

impl ToCanonicalAddress for i64 {
    #[inline(always)]
    fn to_canonical(&self) -> u64 {
        *self as u64
    }
}

impl ToCanonicalAddress for u64 {
    #[inline(always)]
    fn to_canonical(&self) -> u64 {
        *self
    }
}

impl ToCanonicalAddress for i32 {
    #[inline(always)]
    fn to_canonical(&self) -> u64 {
        *self as u32 as u64
    }
}

impl ToCanonicalAddress for u32 {
    #[inline(always)]
    fn to_canonical(&self) -> u64 {
        *self as u64
    }
}

impl ToCanonicalAddress for bool {
    #[inline(always)]
    fn to_canonical(&self) -> u64 {
        if *self { 1 } else { 0 }
    }
}

impl ToCanonicalAddress for f64 {
    /// Order-preserving IEEE-754 bit-cast.
    ///
    /// Preserves natural ordering: `a < b <=> to_canonical(a) < to_canonical(b)`.
    #[inline(always)]
    fn to_canonical(&self) -> u64 {
        canonical_from_f64(*self)
    }
}

impl ToCanonicalAddress for f32 {
    /// Order-preserving f32 mapping widened into u64.
    #[inline(always)]
    fn to_canonical(&self) -> u64 {
        canonical_from_f64(*self as f64)
    }
}

/// Canonical 64-bit value address representing a SQL NULL.
pub const NULL_VADDR: u64 = u64::MAX;

impl<T: ToCanonicalAddress> ToCanonicalAddress for Option<T> {
    #[inline(always)]
    fn to_canonical(&self) -> u64 {
        match self {
            Some(v) => {
                let addr = v.to_canonical();
                if addr == NULL_VADDR {
                    NULL_VADDR - 1
                } else {
                    addr
                }
            }
            None => NULL_VADDR,
        }
    }
}

/// Order-preserving conversion from f64 to u64.
///
/// Inverts sign-magnitude encoding for negative numbers so that negative numbers
/// sort strictly before positive numbers as unsigned integers.
#[inline(always)]
pub fn canonical_from_f64(val: f64) -> u64 {
    let bits = val.to_bits();
    if (bits & (1u64 << 63)) != 0 {
        !bits
    } else {
        bits ^ (1u64 << 63)
    }
}

/// Inverse mapping: reconstructs original f64 from its canonical address.
#[inline(always)]
pub fn f64_from_canonical(vaddr: u64) -> f64 {
    let bits = if (vaddr & (1u64 << 63)) == 0 {
        !vaddr
    } else {
        vaddr ^ (1u64 << 63)
    };
    f64::from_bits(bits)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_coordinate_size_and_alignment() {
        assert_eq!(size_of::<AddressCoordinate>(), 16);
        assert_eq!(align_of::<AddressCoordinate>(), 16);

        let coord = AddressCoordinate::new(0xDEADBEEF_CAFE1234, 42, 1005);
        let bytes = coord.as_bytes();
        let reconstructed = AddressCoordinate::from_bytes(bytes);
        assert_eq!(coord, reconstructed);
    }

    #[test]
    fn test_string_canonical_determinism() {
        let s1 = "CUSTOMER_ENTERPRISE_ALPHA";
        let s2 = "CUSTOMER_ENTERPRISE_ALPHA";
        let s3 = "CUSTOMER_ENTERPRISE_BETA";

        assert_eq!(s1.to_canonical(), s2.to_canonical());
        assert_ne!(s1.to_canonical(), s3.to_canonical());
    }

    #[test]
    fn test_f64_order_preservation() {
        let values = [
            f64::NEG_INFINITY,
            -1_000_000.5,
            -42.0,
            -0.0001,
            0.0,
            0.00001,
            1.0,
            99.99,
            1_000_000.5,
            f64::INFINITY,
        ];

        for window in values.windows(2) {
            let a = window[0];
            let b = window[1];

            let ca = a.to_canonical();
            let cb = b.to_canonical();

            assert!(
                ca < cb,
                "Order preservation failed: {} ({:#x}) was not < {} ({:#x})",
                a,
                ca,
                b,
                cb
            );

            // Verify round-trip accuracy
            let ra = f64_from_canonical(ca);
            let rb = f64_from_canonical(cb);
            assert_eq!(a.to_bits(), ra.to_bits(), "Round-trip failed for {}", a);
            assert_eq!(b.to_bits(), rb.to_bits(), "Round-trip failed for {}", b);
        }
    }
}
