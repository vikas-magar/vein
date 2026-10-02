//! Virtual registers holding the engine's own values.
//!
//! `02-execution-and-vir-lld.md` §4 defines the register file as `Box<
//! [RegisterSlot]>` over `Scalar | Vector | Selection | Mask`. The requirements
//! it states shape this type: reads and writes are a slot index, a dead register
//! is reusable, and ownership is query-local, so a write *moves* a value in and
//! a read borrows it — nothing is cloned per instruction.
//!
//! This is the register file for the engine representation. The Arrow-backed
//! `vm::register::RegisterFile` remains until the VM cuts over, at which point
//! it is deleted; keeping both is a tracked migration state (Rule 34), not two
//! permanent designs.

use std::sync::Arc;

use common::Result;
use error::VeinError;
use types::LogicalLiteral;
use vector::{DataChunk, MaskRef, Selection, VectorRef, trim_words, words_for};

/// One virtual register.
#[derive(Debug, Clone, Default)]
pub enum RegisterSlot {
    /// Nothing written yet. The state every slot starts in, and the state a
    /// released slot returns to.
    #[default]
    Empty,
    /// A value shared by every row.
    Scalar {
        /// The value.
        value: LogicalLiteral,
    },
    /// A column.
    Vector {
        /// The column.
        vector: VectorRef,
    },
    /// A row selection.
    Selection {
        /// The selection.
        selection: Selection,
    },
    /// A bit per row, one bit per row, LSB-first.
    Mask {
        /// The bits, with their validity.
        mask: BitMask,
    },
}

/// A nullable boolean per row: which rows are true, and which rows are known.
///
/// One bitmap is not enough. A single set of bits cannot distinguish "false"
/// from "unknown", and SQL three-valued logic depends on the difference:
/// `NOT (x > 20)` must not keep a row where `x` is null, because `NOT NULL` is
/// null (`05-llm-rules.md` Rule 6). `values` holds the true bits and `valid`
/// holds the rows the value is known for.
#[derive(Debug, Clone)]
pub struct BitMask {
    /// A set bit means the row's condition is true. Meaningful only where
    /// `valid` is set.
    pub values: MaskRef,
    /// A set bit means the row's condition is known rather than null.
    pub valid: MaskRef,
}

impl BitMask {
    /// The rows a filter keeps: true *and* known.
    ///
    /// Only rows that are definitely true survive, which is the same rule
    /// `apply_filter_mask` follows on the Arrow path.
    pub fn keep(&self, row_count: u32) -> MaskRef {
        let mut kept = vec![0u64; words_for(row_count)];
        for (index, word) in kept.iter_mut().enumerate() {
            let values = self.values.get(index).copied().unwrap_or(0);
            let valid = self.valid.get(index).copied().unwrap_or(0);
            *word = values & valid;
        }
        trim_words(&mut kept, row_count);
        kept.into()
    }
}

/// The virtual registers of one program.
#[derive(Debug, Clone)]
pub struct RegisterFile {
    slots: Box<[RegisterSlot]>,
}

impl RegisterFile {
    /// A file of `len` empty registers.
    pub fn new(len: usize) -> Self {
        Self {
            slots: vec![RegisterSlot::Empty; len].into_boxed_slice(),
        }
    }

    /// Number of registers.
    pub fn len(&self) -> usize {
        self.slots.len()
    }

    /// Whether the file has no registers.
    pub fn is_empty(&self) -> bool {
        self.slots.is_empty()
    }

    /// Borrows a register.
    pub fn get(&self, register: u8) -> Option<&RegisterSlot> {
        self.slots.get(register as usize)
    }

    /// Writes a register, moving the value in.
    pub fn set(&mut self, register: u8, slot: RegisterSlot) -> Result<()> {
        if register as usize >= self.slots.len() {
            return Err(VeinError::internal(format!(
                "register R{register} is outside a file of {}",
                self.slots.len()
            )));
        }
        self.slots[register as usize] = slot;
        Ok(())
    }

    /// Releases a register, returning it to `Empty`.
    ///
    /// A released slot is immediately reusable, which is what lets the physical
    /// slot count stay below the virtual register count.
    pub fn release(&mut self, register: u8) -> Result<()> {
        self.set(register, RegisterSlot::Empty)
    }

    /// Releases every register.
    pub fn release_all(&mut self) {
        self.slots.fill(RegisterSlot::Empty);
    }

    /// Loads a chunk's columns into the lowest registers.
    ///
    /// Column `i` lands in register `i`, so a program's column references are
    /// register numbers. Fails when the chunk is wider than the file rather than
    /// truncating it, which would read the wrong column.
    pub fn mount(&mut self, chunk: &DataChunk) -> Result<()> {
        if chunk.num_columns() > self.slots.len() {
            return Err(VeinError::internal(format!(
                "chunk has {} columns but the register file has {}",
                chunk.num_columns(),
                self.slots.len()
            )));
        }
        for (register, column) in chunk.columns().iter().enumerate() {
            self.slots[register] = RegisterSlot::Vector {
                vector: Arc::clone(column),
            };
        }
        Ok(())
    }

    /// Borrows a register as a scalar.
    pub fn scalar(&self, register: u8) -> Option<&LogicalLiteral> {
        match self.get(register)? {
            RegisterSlot::Scalar { value } => Some(value),
            _ => None,
        }
    }

    /// Borrows a register as a column.
    pub fn vector(&self, register: u8) -> Option<&VectorRef> {
        match self.get(register)? {
            RegisterSlot::Vector { vector } => Some(vector),
            _ => None,
        }
    }

    /// Borrows a register as a mask.
    pub fn mask(&self, register: u8) -> Option<&BitMask> {
        match self.get(register)? {
            RegisterSlot::Mask { mask } => Some(mask),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slots_start_empty_and_report_their_kind() {
        let file = RegisterFile::new(3);
        assert_eq!(file.len(), 3);
        assert!(!file.is_empty());
        for register in 0..3 {
            assert!(matches!(file.get(register), Some(RegisterSlot::Empty)));
        }
        assert!(file.get(3).is_none());
    }

    #[test]
    fn writing_moves_the_value_in_and_reads_borrow_it() {
        let mut file = RegisterFile::new(2);
        let column: VectorRef = Arc::new(vector::Vector::from_i64(&[1, 2, 3]));
        let slot = RegisterSlot::Vector {
            vector: Arc::clone(&column),
        };
        file.set(0, slot).expect("in range");

        // The register holds the same allocation, not a copy.
        assert!(Arc::ptr_eq(file.vector(0).expect("vector"), &column));
        assert!(file.scalar(0).is_none(), "a column is not a scalar");
    }

    #[test]
    fn releasing_a_register_makes_it_reusable() {
        let mut file = RegisterFile::new(1);
        let first = RegisterSlot::Scalar {
            value: LogicalLiteral::Int64(7),
        };
        file.set(0, first).expect("in range");
        file.release(0).expect("in range");
        assert!(matches!(file.get(0), Some(RegisterSlot::Empty)));
        assert!(file.scalar(0).is_none());

        let second = RegisterSlot::Scalar {
            value: LogicalLiteral::Int64(8),
        };
        file.set(0, second).expect("in range");
        assert_eq!(file.scalar(0), Some(&LogicalLiteral::Int64(8)));

        file.release_all();
        assert!(file.scalar(0).is_none());
    }

    #[test]
    fn out_of_range_access_is_refused_not_wrapped() {
        let mut file = RegisterFile::new(2);
        assert!(file.set(2, RegisterSlot::Empty).is_err());
        assert!(file.release(2).is_err());
    }

    #[test]
    fn mounting_places_columns_in_register_order() {
        let columns: Vec<VectorRef> = vec![
            Arc::new(vector::Vector::from_i64(&[1, 2])),
            Arc::new(vector::Vector::from_i64(&[3, 4])),
        ];
        let chunk = DataChunk::new(2, columns.clone(), Selection::all()).expect("consistent");

        let mut file = RegisterFile::new(2);
        file.mount(&chunk).expect("columns fit");
        assert!(Arc::ptr_eq(file.vector(0).expect("column 0"), &columns[0]));
        assert!(Arc::ptr_eq(file.vector(1).expect("column 1"), &columns[1]));

        // A chunk wider than the file is refused rather than truncated.
        let mut narrow = RegisterFile::new(1);
        assert!(narrow.mount(&chunk).is_err());
    }
}
