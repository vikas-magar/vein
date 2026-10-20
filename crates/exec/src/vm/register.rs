//! Virtual register file implementation for the Vector VM.

use std::sync::Arc;

use arrow::array::{ArrayRef, NullArray};
use common::protocol::RegId;

mod access;
mod mount;
mod segmented;
mod value;

pub use segmented::SegmentedArray;
pub use value::{RegValue, RegisterDatum};

/// Slot-based storage for one morsel's input columns and intermediate values.
#[derive(Debug, Clone)]
pub struct RegisterFile {
    slots: Vec<RegValue>,
    empty: ArrayRef,
    chunk_len: usize,
}

impl RegisterFile {
    /// Creates a register file with a fixed number of initially empty slots.
    pub fn new(register_count: RegId) -> Self {
        let count = register_count as usize;
        let empty: ArrayRef = Arc::new(NullArray::new(0));
        let slots = vec![RegValue::Array(Arc::clone(&empty)); count];
        Self { slots, empty, chunk_len: 0 }
    }

    /// Total number of registers in this file.
    #[inline]
    pub fn len(&self) -> usize {
        self.slots.len()
    }

    /// Whether this file has no register slots.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.slots.is_empty()
    }

    /// Logical row count of the mounted input chunk.
    #[inline]
    pub fn chunk_len(&self) -> usize {
        self.chunk_len
    }
}

#[cfg(test)]
#[path = "register/tests.rs"]
mod tests;
