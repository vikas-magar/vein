//! Virtual register file implementation for the Vector VM.
//!
//! Provides [`RegisterFile`] which manages slot-based [`ArrayRef`] storage
//! for instruction inputs, intermediates, and outputs during vectorized query execution.

use std::sync::Arc;
use arrow::array::{ArrayRef, NullArray};
use common::protocol::RegId;
use common::Result;
use error::VeinError;
use crate::pipeline::DataChunk;

/// A virtual register file that holds intermediate and column vectors for a morsel/chunk.
///
/// Slots are pre-allocated according to the program's register count to eliminate
/// heap allocations on the hot path during steady-state instruction execution.
#[derive(Debug, Clone)]
pub struct RegisterFile {
    /// Array of register slots holding [`ArrayRef`]s.
    slots: Vec<ArrayRef>,
}

impl RegisterFile {
    /// Creates a new register file pre-allocated with `register_count` slots.
    ///
    /// Each slot is initially populated with a zero-length [`NullArray`] to guarantee
    /// valid references without repeated allocation.
    pub fn new(register_count: u8) -> Self {
        let count = register_count as usize;
        let dummy: ArrayRef = Arc::new(NullArray::new(0));
        let slots = vec![dummy; count];
        Self { slots }
    }

    /// Total number of registers in this register file.
    #[inline]
    pub fn len(&self) -> usize {
        self.slots.len()
    }

    /// Whether the register file has zero registers allocated.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.slots.is_empty()
    }

    /// Mounts the input columns from a [`DataChunk`] into the lowest registers (0..N-1).
    ///
    /// Returns an error if the chunk contains more vectors than available registers.
    pub fn mount_chunk(&mut self, chunk: &DataChunk) -> Result<()> {
        if chunk.vectors.len() > self.slots.len() {
            return Err(VeinError::internal(format!(
                "DataChunk has {} columns but register file only has {} slots",
                chunk.vectors.len(),
                self.slots.len()
            )));
        }

        for (i, col) in chunk.vectors.iter().enumerate() {
            self.slots[i] = col.clone();
        }
        Ok(())
    }

    /// Retrieves an immutable reference to the array stored at register `reg`.
    #[inline]
    pub fn get(&self, reg: RegId) -> Result<&ArrayRef> {
        let idx = reg as usize;
        self.slots.get(idx).ok_or_else(|| {
            VeinError::internal(format!(
                "Register out of bounds: R{reg} accessed but register file capacity is {}",
                self.slots.len()
            ))
        })
    }

    /// Stores an array into register `reg`.
    #[inline]
    pub fn set(&mut self, reg: RegId, array: ArrayRef) -> Result<()> {
        let idx = reg as usize;
        if idx >= self.slots.len() {
            return Err(VeinError::internal(format!(
                "Register out of bounds: R{reg} written but register file capacity is {}",
                self.slots.len()
            )));
        }
        self.slots[idx] = array;
        Ok(())
    }

    /// Extracts the registers specified by `out_regs` as a vector of column arrays.
    pub fn project(&self, out_regs: &[RegId]) -> Result<Vec<ArrayRef>> {
        let mut out = Vec::with_capacity(out_regs.len());
        for &reg in out_regs {
            out.push(self.get(reg)?.clone());
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;

    #[test]
    fn test_register_file_bounds_and_lifecycle() {
        let mut rf = RegisterFile::new(4);
        assert_eq!(rf.len(), 4);
        assert!(!rf.is_empty());

        // Initially slots are dummy null arrays
        assert_eq!(rf.get(0).unwrap().len(), 0);

        // Out of bounds read/write
        assert!(rf.get(4).is_err());
        assert!(rf.set(4, Arc::new(Int64Array::from(vec![1, 2]))).is_err());

        // Valid write and read
        let array: ArrayRef = Arc::new(Int64Array::from(vec![10, 20, 30]));
        rf.set(1, array.clone()).unwrap();
        assert_eq!(rf.get(1).unwrap().len(), 3);

        // Project
        let projected = rf.project(&[1, 1]).unwrap();
        assert_eq!(projected.len(), 2);
        assert_eq!(projected[0].len(), 3);
        assert_eq!(projected[1].len(), 3);
    }

    #[test]
    fn test_register_file_mount_chunk() {
        let mut rf = RegisterFile::new(3);
        let col0: ArrayRef = Arc::new(Int64Array::from(vec![1, 2]));
        let col1: ArrayRef = Arc::new(Int64Array::from(vec![3, 4]));
        let chunk = DataChunk::from_vectors(vec![col0, col1]);

        rf.mount_chunk(&chunk).unwrap();
        assert_eq!(rf.get(0).unwrap().len(), 2);
        assert_eq!(rf.get(1).unwrap().len(), 2);

        // Mounting chunk with more columns than slots returns an error
        let col2: ArrayRef = Arc::new(Int64Array::from(vec![5, 6]));
        let col3: ArrayRef = Arc::new(Int64Array::from(vec![7, 8]));
        let large_chunk = DataChunk::from_vectors(vec![
            rf.get(0).unwrap().clone(),
            rf.get(1).unwrap().clone(),
            col2,
            col3,
        ]);
        assert!(rf.mount_chunk(&large_chunk).is_err());
    }
}
