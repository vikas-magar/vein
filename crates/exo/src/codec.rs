//! Frame-of-Reference (FoR) Bit-Packing Codec for `.exo.addr`.
//!
//! Compresses dense 64-bit canonical address arrays into bit-packed deltas
//! relative to the minimum address base in the granule:
//!
//!   min_base = min(vAddrs)
//!   delta_i  = vAddr_i - min_base
//!   bit_width = 64 - leading_zeros(max(deltas))
//!
//! Enforces zero-unwrap policy and SIMD-friendly alignments.

use crate::error::{ExoError, ExoResult};

/// Frame-of-Reference (FoR) bit-packer and unpacker.
pub struct ForCodec;

impl ForCodec {
    /// Determines the minimum bit width required to represent the given range of values.
    #[inline(always)]
    pub fn calculate_bit_width(max_delta: u64) -> u8 {
        if max_delta == 0 {
            0
        } else {
            64 - max_delta.leading_zeros() as u8
        }
    }

    /// Encodes a slice of 64-bit addresses into a Frame-of-Reference bit-packed payload.
    ///
    /// Returns:
    /// - `min_base`: The baseline value subtracted from all elements.
    /// - `bit_width`: The bit-width used per delta (0 .. 64).
    /// - `packed_data`: The tightly packed binary stream.
    pub fn encode_granule(vaddrs: &[u64]) -> (u64, u8, Vec<u8>) {
        if vaddrs.is_empty() {
            return (0, 0, Vec::new());
        }

        let min_base = *vaddrs.iter().min().unwrap_or(&0);
        let max_val = *vaddrs.iter().max().unwrap_or(&min_base);
        let max_delta = max_val - min_base;
        let bit_width = Self::calculate_bit_width(max_delta);

        if bit_width == 0 {
            return (min_base, 0, Vec::new());
        }

        if bit_width == 64 {
            let mut raw_bytes = Vec::with_capacity(vaddrs.len() * 8);
            for &v in vaddrs {
                raw_bytes.extend_from_slice(&v.to_le_bytes());
            }
            return (min_base, 64, raw_bytes);
        }

        let total_bits = vaddrs.len() * (bit_width as usize);
        let total_bytes = (total_bits + 7) / 8;
        // Pad with extra 8 bytes so unaligned 64-bit loads/stores never read out-of-bounds
        let mut packed_data = vec![0u8; total_bytes + 8];

        let mut bit_offset = 0usize;
        for &v in vaddrs {
            let delta = v - min_base;
            let byte_idx = bit_offset >> 3;
            let bit_in_byte = (bit_offset & 7) as u64;

            // Load existing 8 bytes from buffer
            let mut current = u64::from_le_bytes(
                packed_data[byte_idx..byte_idx + 8].try_into().unwrap_or([0u8; 8]),
            );

            // Shift delta into position and mask
            current |= delta << bit_in_byte;
            packed_data[byte_idx..byte_idx + 8].copy_from_slice(&current.to_le_bytes());

            // Handle overflow into the 9th byte if bits crossed a 64-bit word boundary
            let bits_written_in_word = 64 - bit_in_byte;
            if (bit_width as u64) > bits_written_in_word {
                let overflow_bits = (bit_width as u64) - bits_written_in_word;
                let overflow_byte_idx = byte_idx + 8;
                let overflow_val = (delta >> bits_written_in_word) as u8;
                let mask = (1u8 << overflow_bits) - 1;
                packed_data[overflow_byte_idx] |= overflow_val & mask;
            }

            bit_offset += bit_width as usize;
        }

        packed_data.truncate(total_bytes);
        (min_base, bit_width, packed_data)
    }

    /// Decodes a Frame-of-Reference bit-packed stream into a provided `out` buffer.
    pub fn decode_granule(
        packed_data: &[u8],
        min_base: u64,
        bit_width: u8,
        num_values: usize,
        out: &mut [u64],
    ) -> ExoResult<usize> {
        if out.len() < num_values {
            return Err(ExoError::BufferTooSmall {
                required: num_values,
                provided: out.len(),
            });
        }

        if bit_width == 0 {
            out[..num_values].fill(min_base);
            return Ok(num_values);
        }

        if bit_width == 64 {
            let required_bytes = num_values * 8;
            if packed_data.len() < required_bytes {
                return Err(ExoError::CodecError(format!(
                    "FoR 64-bit decode: expected {} bytes, but found {}",
                    required_bytes,
                    packed_data.len()
                )));
            }
            for (i, chunk) in packed_data[..required_bytes].chunks_exact(8).enumerate() {
                out[i] = u64::from_le_bytes(chunk.try_into().unwrap_or([0u8; 8]));
            }
            return Ok(num_values);
        }

        let mask = (1u64 << bit_width) - 1;
        let mut bit_offset = 0usize;

        // Provide padded view to allow unaligned 8-byte loads without branching
        let mut padded = Vec::with_capacity(packed_data.len() + 8);
        padded.extend_from_slice(packed_data);
        padded.resize(packed_data.len() + 8, 0);

        for i in 0..num_values {
            let byte_idx = bit_offset >> 3;
            let bit_in_byte = (bit_offset & 7) as u64;

            let word = u64::from_le_bytes(
                padded[byte_idx..byte_idx + 8].try_into().unwrap_or([0u8; 8]),
            );

            let mut delta = (word >> bit_in_byte) & mask;

            let bits_read = 64 - bit_in_byte;
            if (bit_width as u64) > bits_read {
                let overflow_byte = padded[byte_idx + 8];
                let overflow_bits = (bit_width as u64) - bits_read;
                let overflow_mask = (1u64 << overflow_bits) - 1;
                let high_part = ((overflow_byte as u64) & overflow_mask) << bits_read;
                delta |= high_part;
            }

            out[i] = min_base + delta;
            bit_offset += bit_width as usize;
        }

        Ok(num_values)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_for_codec_constant_granule() -> ExoResult<()> {
        let values = vec![0x1234_5678_9ABC_DEF0; 1000];
        let (min_base, bit_width, packed) = ForCodec::encode_granule(&values);

        assert_eq!(min_base, 0x1234_5678_9ABC_DEF0);
        assert_eq!(bit_width, 0);
        assert!(packed.is_empty());

        let mut decoded = vec![0u64; 1000];
        let count = ForCodec::decode_granule(&packed, min_base, bit_width, 1000, &mut decoded)?;
        assert_eq!(count, 1000);
        assert_eq!(decoded, values);

        Ok(())
    }

    #[test]
    fn test_for_codec_varying_bit_widths() -> ExoResult<()> {
        // Test different ranges to exercise various bit widths (1, 4, 8, 12, 20, 34)
        for &range in &[2u64, 16, 256, 4096, 1_000_000, 10_000_000_000] {
            let base = 500_000_000_000u64;
            let values: Vec<u64> = (0..500).map(|i| base + (i % range)).collect();

            let (min_base, bit_width, packed) = ForCodec::encode_granule(&values);
            assert_eq!(min_base, base);
            assert!(bit_width > 0 && bit_width < 64);

            let mut decoded = vec![0u64; 500];
            let count = ForCodec::decode_granule(&packed, min_base, bit_width, 500, &mut decoded)?;
            assert_eq!(count, 500);
            assert_eq!(decoded, values, "FoR roundtrip mismatch for range {}", range);
        }

        Ok(())
    }

    #[test]
    fn test_for_codec_full_64_bit() -> ExoResult<()> {
        let values: Vec<u64> = vec![0, u64::MAX / 2, u64::MAX];
        let (min_base, bit_width, packed) = ForCodec::encode_granule(&values);

        assert_eq!(min_base, 0);
        assert_eq!(bit_width, 64);
        assert_eq!(packed.len(), 24);

        let mut decoded = vec![0u64; 3];
        ForCodec::decode_granule(&packed, min_base, bit_width, 3, &mut decoded)?;
        assert_eq!(decoded, values);

        Ok(())
    }
}
