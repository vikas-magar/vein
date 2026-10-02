//! Virtual register file implementation for the Vector VM.
//!
//! Provides [`RegisterFile`] which manages slot-based [`ArrayRef`] storage
//! for instruction inputs, intermediates, and outputs during vectorized query execution.

use std::sync::Arc;
use arrow::array::{Array, ArrayRef, Datum, NullArray, UInt32Array};
use arrow::compute::take;
use arrow::datatypes::DataType;
use common::protocol::RegId;
use common::Result;
use error::VeinError;
use crate::pipeline::DataChunk;

/// A value held in a virtual register.
///
/// [`RegValue::Scalar`] carries a length-1 array plus the flag Arrow's
/// element-wise kernels read from [`Datum::get`], so a literal is broadcast
/// instead of being expanded to one element per row. Anything that genuinely
/// needs a column — a UDF argument, a cast, a projection — goes through
/// [`RegisterFile::to_array`], which is the only place a scalar materializes.
#[derive(Debug, Clone)]
pub enum RegValue {
    /// A full-length column.
    Array(ArrayRef),
    /// A single value that element-wise kernels broadcast across the chunk.
    Scalar(ArrayRef),
}

impl RegValue {
    /// Whether this register is a broadcast scalar rather than a full column.
    pub fn is_scalar(&self) -> bool {
        matches!(self, RegValue::Scalar(_))
    }

    /// The stored array. For a scalar this is the length-1 value, not a broadcast column.
    pub fn array(&self) -> &ArrayRef {
        match self {
            RegValue::Array(array) | RegValue::Scalar(array) => array,
        }
    }

    /// Physical length of the stored array (always 1 for a scalar).
    pub fn physical_len(&self) -> usize {
        self.array().len()
    }

    /// Arrow data type of the stored value.
    pub fn data_type(&self) -> &DataType {
        self.array().data_type()
    }
}

impl Datum for RegValue {
    fn get(&self) -> (&dyn Array, bool) {
        match self {
            RegValue::Array(array) => (array.as_ref(), false),
            RegValue::Scalar(array) => (array.as_ref(), true),
        }
    }
}

/// A virtual register file that holds intermediate and column vectors for a morsel/chunk.
///
/// Slots are pre-allocated according to the program's register count to eliminate
/// heap allocations on the hot path during steady-state instruction execution.
#[derive(Debug, Clone)]
pub struct RegisterFile {
    /// The register slots.
    slots: Vec<RegValue>,
    /// Row count of the mounted chunk; broadcast scalars expand to this length.
    chunk_len: usize,
}

impl RegisterFile {
    /// Creates a new register file pre-allocated with `register_count` slots.
    ///
    /// Each slot is initially populated with a zero-length [`NullArray`] to guarantee
    /// valid references without repeated allocation.
    pub fn new(register_count: RegId) -> Self {
        let count = register_count as usize;
        let dummy: ArrayRef = Arc::new(NullArray::new(0));
        let slots = vec![RegValue::Array(dummy); count];
        Self { slots, chunk_len: 0 }
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

    /// Row count of the currently mounted chunk.
    #[inline]
    pub fn chunk_len(&self) -> usize {
        self.chunk_len
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

        self.chunk_len = chunk.physical_len();
        for (i, col) in chunk.vectors.iter().enumerate() {
            self.slots[i] = RegValue::Array(col.clone());
        }
        Ok(())
    }

    /// Retrieves the value stored at register `reg`.
    #[inline]
    pub fn get(&self, reg: RegId) -> Result<&RegValue> {
        let idx = reg as usize;
        self.slots.get(idx).ok_or_else(|| {
            VeinError::internal(format!(
                "Register out of bounds: R{reg} accessed but register file capacity is {}",
                self.slots.len()
            ))
        })
    }

    /// Stores a full-length array into register `reg`.
    #[inline]
    pub fn set(&mut self, reg: RegId, array: ArrayRef) -> Result<()> {
        self.set_value(reg, RegValue::Array(array))
    }

    /// Stores a length-1 value into register `reg` as a broadcast scalar.
    pub fn set_scalar(&mut self, reg: RegId, array: ArrayRef) -> Result<()> {
        if array.len() != 1 {
            return Err(VeinError::internal(format!(
                "broadcast scalar for R{reg} must have length 1, got {}",
                array.len()
            )));
        }
        self.set_value(reg, RegValue::Scalar(array))
    }

    /// Stores an arbitrary register value into register `reg`.
    #[inline]
    pub fn set_value(&mut self, reg: RegId, value: RegValue) -> Result<()> {
        let idx = reg as usize;
        if idx >= self.slots.len() {
            return Err(VeinError::internal(format!(
                "Register out of bounds: R{reg} written but register file capacity is {}",
                self.slots.len()
            )));
        }
        self.slots[idx] = value;
        Ok(())
    }

    /// Returns register `reg` as a full-length column, broadcasting a scalar to the chunk length.
    pub fn to_array(&self, reg: RegId) -> Result<ArrayRef> {
        match self.get(reg)? {
            RegValue::Array(array) => Ok(array.clone()),
            RegValue::Scalar(array) => broadcast(array, self.chunk_len),
        }
    }

    /// Extracts the registers specified by `out_regs` as a vector of column arrays.
    pub fn project(&self, out_regs: &[RegId]) -> Result<Vec<ArrayRef>> {
        out_regs.iter().map(|&reg| self.to_array(reg)).collect()
    }
}

/// Expands a length-1 array to `len` rows by repeating its single value.
fn broadcast(array: &ArrayRef, len: usize) -> Result<ArrayRef> {
    if array.len() == len {
        return Ok(array.clone());
    }
    if array.len() != 1 {
        return Err(VeinError::internal(format!(
            "cannot broadcast an array of length {} to {len} rows",
            array.len()
        )));
    }
    let indices = UInt32Array::from(vec![0u32; len]);
    take(array.as_ref(), &indices, None)
        .map_err(|error| VeinError::internal(format!("scalar broadcast failed: {error}")))
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
        assert_eq!(rf.get(0).unwrap().physical_len(), 0);

        // Out of bounds read/write
        assert!(rf.get(4).is_err());
        assert!(rf.set(4, Arc::new(Int64Array::from(vec![1, 2]))).is_err());

        // Valid write and read
        let array: ArrayRef = Arc::new(Int64Array::from(vec![10, 20, 30]));
        rf.set(1, array.clone()).unwrap();
        assert_eq!(rf.get(1).unwrap().physical_len(), 3);

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
        assert_eq!(rf.get(0).unwrap().physical_len(), 2);
        assert_eq!(rf.get(1).unwrap().physical_len(), 2);

        // Mounting chunk with more columns than slots returns an error
        let col2: ArrayRef = Arc::new(Int64Array::from(vec![5, 6]));
        let col3: ArrayRef = Arc::new(Int64Array::from(vec![7, 8]));
        let large_chunk = DataChunk::from_vectors(vec![
            rf.get(0).unwrap().array().clone(),
            rf.get(1).unwrap().array().clone(),
            col2,
            col3,
        ]);
        assert!(rf.mount_chunk(&large_chunk).is_err());
    }

    #[test]
    fn test_scalar_register_broadcasts_to_chunk_length() {
        let mut rf = RegisterFile::new(2);
        let column: ArrayRef = Arc::new(Int64Array::from(vec![10, 20, 30]));
        let chunk = DataChunk::from_vectors(vec![column]);
        rf.mount_chunk(&chunk).unwrap();

        rf.set_scalar(1, Arc::new(Int64Array::from(vec![7]))).unwrap();
        assert!(rf.get(1).unwrap().is_scalar());
        // The stored scalar stays a single element; only `to_array` expands it.
        assert_eq!(rf.get(1).unwrap().physical_len(), 1);

        let broadcast = rf.to_array(1).unwrap();
        assert_eq!(broadcast.len(), 3);
        let values = broadcast.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(values.values(), &[7, 7, 7]);

        // A scalar must be a single value.
        assert!(rf.set_scalar(1, Arc::new(Int64Array::from(vec![1, 2]))).is_err());
    }
}
