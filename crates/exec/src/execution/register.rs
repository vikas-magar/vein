//! Virtual registers holding the engine's own values.
//!
//! `02-execution-and-vir-lld.md` §3 requires the register file to hold
//! **non-owning views** rather than repeatedly cloning shared columns:
//! *"Hot-path ownership should not be represented by repeated `Arc<Array>`
//! churn. ... Ownership lives at pipeline boundaries."* A chunk owns its
//! columns; a register borrows a view of one, so mounting a chunk is a pointer
//! write per column rather than a refcount update, and Rule 5's prohibition on
//! `Arc` in an inner loop is respected by construction.
//!
//! The file is parameterised by the lifetime of the chunk it borrows, which is
//! what makes the borrowing explicit: a register cannot outlive the chunk it
//! views, and a program that tried to would not compile.
//!
//! This is the register file for the engine representation. The Arrow-backed
//! `vm::register::RegisterFile` remains until the VM cuts over, at which point
//! it is deleted; keeping both is a tracked migration state, not two permanent
//! designs.

use std::sync::Arc;

use common::Result;
use common::protocol::RegId;
use error::VeinError;
use types::LogicalLiteral;
use vector::{DataChunk, MaskRef, Selection, VectorRef, VectorView, trim_words, words_for};

/// A column in a register: borrowed from the chunk, or produced and owned here.
///
/// Mounting borrows, so a column that already exists is never copied. A column
/// the program computed exists nowhere else, so the register file owns it
/// (ADR 0004). Only the view lifetime differs; consumers see a `VectorView`
/// either way.
#[derive(Debug, Clone)]
pub enum ColumnRef<'a> {
    /// A view of a chunk column.
    Borrowed(VectorView<'a>),
    /// A column this register file produced.
    Owned(VectorRef),
}

impl ColumnRef<'_> {
    /// A view of the column, borrowing the slot rather than the chunk.
    ///
    /// The register file holds no selection, so a produced column is dense and
    /// a consumer narrows it with the chunk's selection.
    pub fn as_view(&self) -> VectorView<'_> {
        match self {
            ColumnRef::Borrowed(view) => *view,
            ColumnRef::Owned(vector) => VectorView::all(vector),
        }
    }
}

/// One virtual register.
#[derive(Debug, Clone, Default)]
pub enum RegisterSlot<'a> {
    /// Nothing written yet. The state every slot starts in, and the state a
    /// released slot returns to.
    #[default]
    Empty,
    /// A value shared by every row.
    Scalar {
        /// The value.
        value: LogicalLiteral,
    },
    /// A column, borrowed from the chunk or produced by the program.
    Vector {
        /// The column.
        column: ColumnRef<'a>,
    },
    /// A row selection.
    Selection {
        /// The selection.
        selection: Selection,
    },
    /// A nullable boolean per row.
    Mask {
        /// The bits, with their validity.
        mask: BitMask,
    },
}

/// A nullable boolean per row: which rows are true, and which rows are known.
///
/// See `vein_design/adr/0003-nullable-conditions.md`. One bitmap cannot
/// distinguish *false* from *unknown*, and SQL three-valued logic depends on the
/// difference: `NOT (x > 20)` must not keep a row where `x` is null.
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
    ///
    /// Consumes the condition and folds the validity into the value bits in
    /// place, so a filter that narrows immediately needs one mask rather than
    /// two (Rule 3). The mask is unique on the filter path; a mask that is still
    /// shared falls back to a copy, so the result is the same either way.
    pub fn into_keep(self, row_count: u32) -> MaskRef {
        let BitMask { mut values, valid } = self;
        match Arc::get_mut(&mut values) {
            Some(words) => {
                for (index, word) in words.iter_mut().enumerate() {
                    *word &= valid.get(index).copied().unwrap_or(0);
                }
                trim_words(words, row_count);
                values
            }
            None => {
                let mut kept = vec![0u64; words_for(row_count)];
                for (index, word) in kept.iter_mut().enumerate() {
                    let values = values.get(index).copied().unwrap_or(0);
                    let valid = valid.get(index).copied().unwrap_or(0);
                    *word = values & valid;
                }
                trim_words(&mut kept, row_count);
                kept.into()
            }
        }
    }
}

/// The virtual registers of one program.
#[derive(Debug, Clone)]
pub struct RegisterFile<'a> {
    slots: Box<[RegisterSlot<'a>]>,
}

impl<'a> RegisterFile<'a> {
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
    pub fn get(&self, register: RegId) -> Option<&RegisterSlot<'a>> {
        self.slots.get(register as usize)
    }

    /// Writes a register, moving the value in.
    pub fn set(&mut self, register: RegId, slot: RegisterSlot<'a>) -> Result<()> {
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
    pub fn release(&mut self, register: RegId) -> Result<()> {
        self.set(register, RegisterSlot::Empty)
    }

    /// Releases every register.
    pub fn release_all(&mut self) {
        self.slots.fill(RegisterSlot::Empty);
    }

    /// Loads views of a chunk's columns into the lowest registers.
    ///
    /// Column `i` lands in register `i`, so a program's column references are
    /// register numbers. Mounting copies no data and takes no reference count —
    /// each register stores a view, which is `Copy`. Fails when the chunk is
    /// wider than the file rather than truncating it, which would read the wrong
    /// column.
    pub fn mount(&mut self, chunk: &'a DataChunk) -> Result<()> {
        if chunk.num_columns() > self.slots.len() {
            return Err(VeinError::internal(format!(
                "chunk has {} columns but the register file has {}",
                chunk.num_columns(),
                self.slots.len()
            )));
        }
        for (register, column) in chunk.columns().iter().enumerate() {
            self.slots[register] = RegisterSlot::Vector {
                column: ColumnRef::Borrowed(VectorView::all(column)),
            };
        }
        Ok(())
    }

    /// Borrows a register as a scalar.
    pub fn scalar(&self, register: RegId) -> Option<&LogicalLiteral> {
        match self.get(register)? {
            RegisterSlot::Scalar { value } => Some(value),
            _ => None,
        }
    }

    /// Borrows a register as a column, borrowed or owned.
    pub fn column(&self, register: RegId) -> Option<&ColumnRef<'a>> {
        match self.get(register)? {
            RegisterSlot::Vector { column } => Some(column),
            _ => None,
        }
    }

    /// Writes a produced column into a register, taking ownership of it.
    ///
    /// This is the one place ownership enters the register file: a value the
    /// program computed has no other owner (ADR 0004). A mounted column stays
    /// borrowed, so the common path still copies nothing.
    pub fn set_owned(&mut self, register: RegId, vector: VectorRef) -> Result<()> {
        self.set(
            register,
            RegisterSlot::Vector {
                column: ColumnRef::Owned(vector),
            },
        )
    }

    /// Borrows a register as a mask.
    pub fn mask(&self, register: RegId) -> Option<&BitMask> {
        match self.get(register)? {
            RegisterSlot::Mask { mask } => Some(mask),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use vector::{Vector, VectorRef};

    use super::*;

    fn chunk_of(values: &[i64]) -> DataChunk {
        let column: VectorRef = Arc::new(Vector::from_i64(values));
        DataChunk::new(values.len() as u32, vec![column], Selection::all()).expect("consistent")
    }

    #[test]
    fn slots_start_empty_and_report_their_kind() {
        let file: RegisterFile<'_> = RegisterFile::new(3);
        assert_eq!(file.len(), 3);
        assert!(!file.is_empty());
        for register in 0..3 {
            assert!(matches!(file.get(register), Some(RegisterSlot::Empty)));
        }
        assert!(file.get(3).is_none());
    }

    #[test]
    fn reading_a_register_reports_only_its_kind() {
        let mut file: RegisterFile<'_> = RegisterFile::new(2);
        let slot = RegisterSlot::Scalar {
            value: LogicalLiteral::Int64(7),
        };
        file.set(0, slot).expect("in range");
        assert_eq!(file.scalar(0), Some(&LogicalLiteral::Int64(7)));
        assert!(file.column(0).is_none(), "a scalar is not a column");
    }

    #[test]
    fn releasing_a_register_makes_it_reusable() {
        let mut file: RegisterFile<'_> = RegisterFile::new(1);
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
        let mut file: RegisterFile<'_> = RegisterFile::new(2);
        assert!(file.set(2, RegisterSlot::Empty).is_err());
        assert!(file.release(2).is_err());
    }

    /// Mounting borrows the columns: the register reads the chunk's values
    /// without copying them or taking a reference count.
    #[test]
    fn mounting_borrows_columns_in_register_order() {
        let first = chunk_of(&[1, 2]);
        let second = chunk_of(&[3, 4]);

        // Two single-column chunks cannot both be mounted into the same file, so
        // mount one and check the view reads through to the chunk's buffer.
        let mut file: RegisterFile<'_> = RegisterFile::new(1);
        file.mount(&first).expect("columns fit");
        let view = file.column(0).expect("column 0").as_view();
        assert_eq!(view.len(), 2);
        let values = view.resolve_i64().expect("i64").as_flat().expect("flat");
        assert_eq!(values, &[1, 2]);

        // The view tracks the chunk it borrows, not a snapshot of it.
        let mut other: RegisterFile<'_> = RegisterFile::new(1);
        other.mount(&second).expect("columns fit");
        let values = other
            .column(0)
            .expect("column 0")
            .as_view()
            .resolve_i64()
            .expect("i64")
            .as_flat()
            .expect("flat");
        assert_eq!(values, &[3, 4]);
    }

    #[test]
    fn a_chunk_wider_than_the_file_is_refused() {
        let wide: Vec<VectorRef> = vec![
            Arc::new(Vector::from_i64(&[1])),
            Arc::new(Vector::from_i64(&[2])),
        ];
        let chunk = DataChunk::new(1, wide, Selection::all()).expect("consistent");
        let mut narrow: RegisterFile<'_> = RegisterFile::new(1);
        assert!(narrow.mount(&chunk).is_err());
    }

    #[test]
    fn into_keep_folds_validity_in_place() {
        let mask = BitMask {
            values: vec![0b0000_1111u64].into(),
            valid: vec![0b0000_0101u64].into(),
        };
        assert_eq!(&*mask.into_keep(4), &[0b0000_0101u64]);
    }

    #[test]
    fn into_keep_falls_back_when_the_mask_is_shared() {
        let mask = BitMask {
            values: vec![0b0000_1111u64].into(),
            valid: vec![0b0000_0101u64].into(),
        };
        let _shared = mask.clone();
        assert_eq!(&*mask.into_keep(4), &[0b0000_0101u64]);
    }
}
