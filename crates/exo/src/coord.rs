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

impl ToCanonicalAddress for i128 {
    #[inline(always)]
    fn to_canonical(&self) -> u64 {
        twox_hash::XxHash64::oneshot(CANONICAL_SEED, &self.to_le_bytes())
    }
}

impl ToCanonicalAddress for arrow::datatypes::i256 {
    #[inline(always)]
    fn to_canonical(&self) -> u64 {
        twox_hash::XxHash64::oneshot(CANONICAL_SEED, &self.to_le_bytes())
    }
}

impl ToCanonicalAddress for i64 {
    #[inline(always)]
    fn to_canonical(&self) -> u64 {
        canonical_from_i64(*self)
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
        canonical_from_i32(*self)
    }
}

impl ToCanonicalAddress for u32 {
    #[inline(always)]
    fn to_canonical(&self) -> u64 {
        *self as u64
    }
}

impl ToCanonicalAddress for i16 {
    #[inline(always)]
    fn to_canonical(&self) -> u64 {
        canonical_from_i64(*self as i64)
    }
}

impl ToCanonicalAddress for u16 {
    #[inline(always)]
    fn to_canonical(&self) -> u64 {
        *self as u64
    }
}

impl ToCanonicalAddress for i8 {
    #[inline(always)]
    fn to_canonical(&self) -> u64 {
        canonical_from_i64(*self as i64)
    }
}

impl ToCanonicalAddress for u8 {
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

/// Order-preserving monotonic conversion from signed i64 to u64.
/// Flips the sign bit so negative numbers map below positive numbers in unsigned space.
#[inline(always)]
pub fn canonical_from_i64(val: i64) -> u64 {
    (val as u64) ^ (1u64 << 63)
}

/// Inverse mapping: reconstructs original i64 from canonical address.
#[inline(always)]
pub fn i64_from_canonical(vaddr: u64) -> i64 {
    (vaddr ^ (1u64 << 63)) as i64
}

/// Order-preserving conversion from signed i32 to u64.
#[inline(always)]
pub fn canonical_from_i32(val: i32) -> u64 {
    canonical_from_i64(val as i64)
}

/// Inverse mapping: reconstructs original i32 from canonical address.
#[inline(always)]
pub fn i32_from_canonical(vaddr: u64) -> i32 {
    i64_from_canonical(vaddr) as i32
}

/// Direct mapping for unsigned 64-bit integers.
#[inline(always)]
pub fn u64_from_canonical(vaddr: u64) -> u64 {
    vaddr
}

/// Direct mapping for unsigned 32-bit integers.
#[inline(always)]
pub fn u32_from_canonical(vaddr: u64) -> u32 {
    vaddr as u32
}

/// Native 32-bit order-preserving conversion from signed i32 to u32.
#[inline(always)]
pub fn canonical_from_i32_native(val: i32) -> u32 {
    (val as u32) ^ (1u32 << 31)
}

/// Inverse mapping: reconstructs original i32 from 32-bit canonical representation.
#[inline(always)]
pub fn i32_from_canonical_native(val: u32) -> i32 {
    (val ^ (1u32 << 31)) as i32
}

/// Native 32-bit order-preserving conversion from f32 to u32.
#[inline(always)]
pub fn canonical_from_f32_native(val: f32) -> u32 {
    let bits = val.to_bits();
    if (bits & (1u32 << 31)) != 0 {
        !bits
    } else {
        bits ^ (1u32 << 31)
    }
}

/// Inverse mapping: reconstructs original f32 from 32-bit canonical representation.
#[inline(always)]
pub fn f32_from_canonical_native(val: u32) -> f32 {
    let bits = if (val & (1u32 << 31)) == 0 {
        !val
    } else {
        val ^ (1u32 << 31)
    };
    f32::from_bits(bits)
}

/// Native 16-bit order-preserving conversion from signed i16 to u16.
#[inline(always)]
pub fn canonical_from_i16_native(val: i16) -> u16 {
    (val as u16) ^ (1u16 << 15)
}

/// Inverse mapping: reconstructs original i16 from 16-bit canonical representation.
#[inline(always)]
pub fn i16_from_canonical_native(val: u16) -> i16 {
    (val ^ (1u16 << 15)) as i16
}

/// Native 8-bit order-preserving conversion from signed i8 to u8.
#[inline(always)]
pub fn canonical_from_i8_native(val: i8) -> u8 {
    (val as u8) ^ (1u8 << 7)
}

/// Inverse mapping: reconstructs original i8 from 8-bit canonical representation.
#[inline(always)]
pub fn i8_from_canonical_native(val: u8) -> i8 {
    (val ^ (1u8 << 7)) as i8
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

/// 8-byte Compound Join Token for zero-data-shuffle joins with collision elimination.
///
/// Layout:
/// - `prefix`: First 4 bytes of UTF-8 string or big-endian primitive (enables 1 CPU cycle rejection).
/// - `gf_fingerprint`: 32-bit polynomial fingerprint / hash over GF(2^32).
#[repr(C, packed)]
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, Default)]
pub struct JoinToken {
    /// 4-byte string prefix (or primitive prefix).
    pub prefix: [u8; 4],
    /// 32-bit algebraic fingerprint.
    pub gf_fingerprint: u32,
}

const _: () = assert!(size_of::<JoinToken>() == 8);

impl JoinToken {
    /// Creates a new JoinToken from prefix and fingerprint.
    #[inline(always)]
    pub const fn new(prefix: [u8; 4], gf_fingerprint: u32) -> Self {
        Self {
            prefix,
            gf_fingerprint,
        }
    }

    /// Derives a JoinToken from arbitrary byte slice.
    #[inline(always)]
    pub fn from_bytes(bytes: &[u8]) -> Self {
        let mut prefix = [0u8; 4];
        let len = bytes.len().min(4);
        prefix[..len].copy_from_slice(&bytes[..len]);
        let gf_fingerprint = twox_hash::XxHash32::oneshot(CANONICAL_SEED as u32, bytes);
        Self {
            prefix,
            gf_fingerprint,
        }
    }

    /// Derives a JoinToken from string slice.
    #[inline(always)]
    pub fn from_str(s: &str) -> Self {
        Self::from_bytes(s.as_bytes())
    }

    /// Converts token to a 64-bit scalar for SIMD register storage and comparison.
    #[inline(always)]
    pub fn as_u64(&self) -> u64 {
        u64::from_ne_bytes(unsafe { std::mem::transmute(*self) })
    }

    /// Reconstructs token from 64-bit scalar.
    #[inline(always)]
    pub fn from_u64(val: u64) -> Self {
        unsafe { std::mem::transmute(val.to_ne_bytes()) }
    }
}

/// Canonical 64-bit Global Row Coordinate (GRC) for distributed query coordination.
///
/// Bit Layout:
/// - `[63..56]`: Node ID    (8 bits:  0..255 nodes)
/// - `[55..32]`: Part ID    (24 bits: 0..16,777,215 parts per node)
/// - `[31..16]`: Granule ID (16 bits: 0..65,535 granules per part)
/// - `[15..0]`:  Slot ID    (16 bits: 0..65,535 rows per granule)
#[repr(transparent)]
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Default, Debug)]
pub struct GlobalRowCoord(pub u64);

const _: () = assert!(size_of::<GlobalRowCoord>() == 8);

impl GlobalRowCoord {
    /// Constructs a 64-bit Global Row Coordinate from hierarchical components.
    #[inline(always)]
    pub const fn new(node_id: u8, part_id: u32, granule_id: u16, slot_id: u16) -> Self {
        let raw = ((node_id as u64) << 56)
            | (((part_id & 0x00FF_FFFF) as u64) << 32)
            | ((granule_id as u64) << 16)
            | (slot_id as u64);
        Self(raw)
    }

    /// Extracts the 8-bit cluster node identifier.
    #[inline(always)]
    pub const fn node_id(&self) -> u8 {
        ((self.0 >> 56) & 0xFF) as u8
    }

    /// Extracts the 24-bit part identifier.
    #[inline(always)]
    pub const fn part_id(&self) -> u32 {
        ((self.0 >> 32) & 0x00FF_FFFF) as u32
    }

    /// Extracts the 16-bit granule identifier.
    #[inline(always)]
    pub const fn granule_id(&self) -> u16 {
        ((self.0 >> 16) & 0xFFFF) as u16
    }

    /// Extracts the 16-bit slot offset within the granule.
    #[inline(always)]
    pub const fn slot_id(&self) -> u16 {
        (self.0 & 0xFFFF) as u16
    }

    /// Raw 64-bit value.
    #[inline(always)]
    pub const fn as_u64(&self) -> u64 {
        self.0
    }

    /// Reconstructs from raw 64-bit value.
    #[inline(always)]
    pub const fn from_u64(raw: u64) -> Self {
        Self(raw)
    }
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

    #[test]
    fn test_i64_order_preservation() {
        let values = [
            i64::MIN,
            -1_000_000_000,
            -42,
            -1,
            0,
            1,
            42,
            1_000_000_000,
            i64::MAX,
        ];

        for window in values.windows(2) {
            let a = window[0];
            let b = window[1];

            let ca = a.to_canonical();
            let cb = b.to_canonical();

            assert!(
                ca < cb,
                "i64 order preservation failed: {} ({:#x}) was not < {} ({:#x})",
                a,
                ca,
                b,
                cb
            );

            let ra = i64_from_canonical(ca);
            let rb = i64_from_canonical(cb);
            assert_eq!(a, ra, "i64 round-trip failed for {}", a);
            assert_eq!(b, rb, "i64 round-trip failed for {}", b);
        }
    }

    #[test]
    fn test_join_token_and_rejection() {
        let token1 = JoinToken::from_str("CUSTOMER_ALPHA");
        let token2 = JoinToken::from_str("CUSTOMER_ALPHA");
        let token3 = JoinToken::from_str("ORDERS_ALPHA");

        assert_eq!(token1, token2);
        assert_eq!(token1.as_u64(), token2.as_u64());
        assert_ne!(token1, token3);
        assert_ne!(token1.as_u64(), token3.as_u64());

        // Fast prefix mismatch test
        assert_ne!(token1.prefix, token3.prefix);
        assert_eq!(JoinToken::from_u64(token1.as_u64()), token1);
    }

    #[test]
    fn test_global_row_coord_packing() {
        let node_id = 42u8;
        let part_id = 12345u32;
        let granule_id = 789u16;
        let slot_id = 1023u16;

        let grc = GlobalRowCoord::new(node_id, part_id, granule_id, slot_id);
        assert_eq!(grc.node_id(), node_id);
        assert_eq!(grc.part_id(), part_id);
        assert_eq!(grc.granule_id(), granule_id);
        assert_eq!(grc.slot_id(), slot_id);

        let grc2 = GlobalRowCoord::from_u64(grc.as_u64());
        assert_eq!(grc, grc2);
    }

    #[test]
    fn test_native_sub_64bit_order_preservation() {
        // i32
        let i32_vals = [i32::MIN, -100_000, -1, 0, 1, 100_000, i32::MAX];
        for window in i32_vals.windows(2) {
            let a = window[0];
            let b = window[1];
            let ca = canonical_from_i32_native(a);
            let cb = canonical_from_i32_native(b);
            assert!(ca < cb, "i32 order failed: {} was not < {}", a, b);
            assert_eq!(a, i32_from_canonical_native(ca));
        }

        // f32
        let f32_vals = [f32::NEG_INFINITY, -100.5, -0.01, 0.0, 0.01, 100.5, f32::INFINITY];
        for window in f32_vals.windows(2) {
            let a = window[0];
            let b = window[1];
            let ca = canonical_from_f32_native(a);
            let cb = canonical_from_f32_native(b);
            assert!(ca < cb, "f32 order failed: {} was not < {}", a, b);
            assert_eq!(a.to_bits(), f32_from_canonical_native(ca).to_bits());
        }

        // i16
        let i16_vals = [i16::MIN, -100, 0, 100, i16::MAX];
        for window in i16_vals.windows(2) {
            let a = window[0];
            let b = window[1];
            let ca = canonical_from_i16_native(a);
            let cb = canonical_from_i16_native(b);
            assert!(ca < cb);
            assert_eq!(a, i16_from_canonical_native(ca));
        }

        // i8
        let i8_vals = [i8::MIN, -10, 0, 10, i8::MAX];
        for window in i8_vals.windows(2) {
            let a = window[0];
            let b = window[1];
            let ca = canonical_from_i8_native(a);
            let cb = canonical_from_i8_native(b);
            assert!(ca < cb);
            assert_eq!(a, i8_from_canonical_native(ca));
        }
    }
}

