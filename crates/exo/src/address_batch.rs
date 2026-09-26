//! Batch-oriented canonical address storage.
//!
//! An AddressBatch keeps canonical addresses aligned with a compact validity
//! bitmap. It is independent of Arrow and storage readers so it can be shared
//! by scans, joins, and compaction.

use crate::error::{ExoError, ExoResult};
use crate::traits::AddressReader;
use std::sync::Arc;

/// A row-oriented batch of canonical addresses.
///
/// The address at position i is valid when bit i in the validity bitmap is set.
/// Null addresses remain stored in the address vector to preserve positional
/// alignment, but their value is not observable through validity-aware APIs.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct AddressBatch {
    addresses: Arc<Vec<u64>>,
    validity: Arc<Vec<u8>>,
}

impl AddressBatch {
    /// Creates an empty batch with capacity for capacity rows.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            addresses: Arc::new(Vec::with_capacity(capacity)),
            validity: Arc::new(Vec::with_capacity(validity_bytes(capacity))),
        }
    }

    /// Creates a batch where every address is valid.
    pub fn from_addresses(addresses: Vec<u64>) -> Self {
        let mut batch = Self::with_capacity(addresses.len());
        batch.addresses = Arc::new(addresses);
        let rows = batch.len();
        let validity = Arc::make_mut(&mut batch.validity);
        validity.resize(validity_bytes(rows), 0xff);
        clear_unused_bits(validity.as_mut_slice(), rows);
        batch
    }

    /// Creates a batch from addresses and one validity bit per row.
    pub fn from_nullable(addresses: Vec<u64>, validity: &[bool]) -> ExoResult<Self> {
        if addresses.len() != validity.len() {
            return Err(ExoError::CodecError(format!(
                "address batch mismatch: {} addresses != {} validity bits",
                addresses.len(),
                validity.len()
            )));
        }

        let mut batch = Self::with_capacity(addresses.len());
        batch.addresses = Arc::new(addresses);
        batch.validity = Arc::new(encode_validity(validity));
        Ok(batch)
    }

    /// Reads selected rows from an address reader into an aligned batch.
    pub fn from_reader<R: AddressReader + ?Sized>(reader: &R, slots: &[u32]) -> ExoResult<Self> {
        let mut batch = Self::with_capacity(slots.len());
        for &slot in slots {
            let valid = reader.is_valid(slot as u64)?;
            let address = if valid {
                reader.read_vaddr(slot as u64)?
            } else {
                0
            };
            batch.push_nullable(address, valid);
        }
        Ok(batch)
    }

    /// Number of rows in the batch.
    #[inline]
    pub fn len(&self) -> usize {
        self.addresses.len()
    }

    /// Whether the batch contains no rows.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.addresses.is_empty()
    }

    /// Appends one valid canonical address.
    pub fn push(&mut self, address: u64) {
        self.push_nullable(address, true);
    }

    /// Appends one canonical address with an explicit validity bit.
    pub fn push_nullable(&mut self, address: u64, valid: bool) {
        let row = self.addresses.len();
        Arc::make_mut(&mut self.addresses).push(address);
        let bitmap = Arc::make_mut(&mut self.validity);
        if row % 8 == 0 {
            bitmap.push(0);
        }
        if valid {
            bitmap[row / 8] |= 1 << (row % 8);
        }
    }

    /// Appends a slice of valid canonical addresses.
    pub fn extend(&mut self, addresses: &[u64]) {
        for &address in addresses {
            self.push(address);
        }
    }

    /// Returns the canonical address storage without copying.
    #[inline]
    pub fn addresses(&self) -> &[u64] {
        self.addresses.as_slice()
    }

    /// Returns the packed validity bitmap without copying.
    #[inline]
    pub fn validity_bitmap(&self) -> &[u8] {
        self.validity.as_slice()
    }

    /// Returns whether the row contains a non-null value.
    #[inline]
    pub fn is_valid(&self, row: usize) -> bool {
        row < self.len() && (self.validity[row / 8] & (1 << (row % 8))) != 0
    }

    /// Returns the address and validity state for one row.
    #[inline]
    pub fn get(&self, row: usize) -> Option<(u64, bool)> {
        self.addresses
            .get(row)
            .copied()
            .map(|address| (address, self.is_valid(row)))
    }

    /// Iterates over (canonical address, is valid) pairs without allocation.
    pub fn iter(&self) -> impl Iterator<Item = (u64, bool)> + '_ {
        self.addresses
            .iter()
            .copied()
            .enumerate()
            .map(|(row, address)| (address, self.is_valid(row)))
    }

    /// Returns a boolean validity vector for APIs that require row-wise bits.
    pub fn validity_bools(&self) -> Vec<bool> {
        (0..self.len()).map(|row| self.is_valid(row)).collect()
    }

    /// Consumes the batch and returns its address vector and packed validity bitmap.
    pub fn into_parts(self) -> (Vec<u64>, Vec<u8>) {
        (
            Arc::try_unwrap(self.addresses).unwrap_or_else(|addresses| (*addresses).clone()),
            Arc::try_unwrap(self.validity).unwrap_or_else(|validity| (*validity).clone()),
        )
    }
}

fn validity_bytes(rows: usize) -> usize {
    rows.saturating_add(7) / 8
}

fn encode_validity(validity: &[bool]) -> Vec<u8> {
    let mut bitmap = vec![0; validity_bytes(validity.len())];
    for (row, &valid) in validity.iter().enumerate() {
        if valid {
            bitmap[row / 8] |= 1 << (row % 8);
        }
    }
    bitmap
}

fn clear_unused_bits(bitmap: &mut [u8], rows: usize) {
    let remainder = rows % 8;
    if remainder != 0 {
        if let Some(last) = bitmap.last_mut() {
            *last &= (1 << remainder) - 1;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::AddressBatch;
    use std::sync::Arc;

    #[test]
    fn tracks_nullable_rows_in_a_packed_bitmap() {
        let batch = AddressBatch::from_nullable(vec![10, 20, 30, 40], &[true, false, true, false])
            .expect("matching lengths");

        assert_eq!(batch.len(), 4);
        assert_eq!(batch.addresses(), &[10, 20, 30, 40]);
        assert_eq!(batch.validity_bitmap(), &[0b0101]);
        assert_eq!(
            batch.iter().collect::<Vec<_>>(),
            vec![(10, true), (20, false), (30, true), (40, false)]
        );
    }

    #[test]
    fn push_preserves_alignment_across_bitmap_bytes() {
        let mut batch = AddressBatch::with_capacity(9);
        for address in 0..9 {
            batch.push_nullable(address, address != 3 && address != 8);
        }

        assert_eq!(batch.validity_bitmap(), &[0b11110111, 0]);
        assert!(batch.is_valid(7));
        assert!(!batch.is_valid(8));
    }

    #[test]
    fn clone_shares_sidecar_buffers_until_mutated() {
        let mut original = AddressBatch::from_addresses(vec![7, 11, 13]);
        let clone = original.clone();
        assert!(Arc::ptr_eq(&original.addresses, &clone.addresses));
        assert!(Arc::ptr_eq(&original.validity, &clone.validity));

        original.push(17);
        assert_eq!(clone.addresses(), &[7, 11, 13]);
        assert_eq!(original.addresses(), &[7, 11, 13, 17]);
        assert!(!Arc::ptr_eq(&original.addresses, &clone.addresses));
    }

    #[test]
    fn rejects_mismatched_validity() {
        assert!(AddressBatch::from_nullable(vec![1, 2], &[true]).is_err());
    }
}
