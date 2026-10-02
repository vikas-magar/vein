//! Borrowed views over a vector and a selection.
//!
//! `01-final-architecture.md` §3 defines the view as the execution interface and
//! `02-execution-and-vir-lld.md` §6 pairs it with a [`Selection`]. A view owns
//! nothing: it borrows the vector's buffers and the selection, so passing data
//! between operators costs a pointer rather than a clone (Rule 3).
//!
//! Two kinds of access exist deliberately:
//!
//! - [`VectorView::rows`] yields the physical rows the selection activates, in
//!   order. A kernel iterates that once and indexes a slice inside the loop, so
//!   the inner loop contains no encoding match and no dispatch — the invariant
//!   Rule 4 states ("no dynamic dispatch inside the innermost loop").
//! - [`VectorView::values_i64`] and [`VectorView::values_utf8`] expose the flat
//!   buffers as slices when the encoding allows it, and return `None` otherwise.
//!   A kernel that needs an encoding it cannot consume calls
//!   [`Vector::to_flat`](crate::Vector::to_flat) explicitly, so the
//!   materialisation is visible at the call site (Rule 25).

use types::{LogicalLiteral, VeinDataType};

use crate::selection::{Selection, SelectionIter};
use crate::vector::{Encoding, Vector};

/// `All` as a value with `'static` lifetime, so [`VectorView::all`] has
/// something to borrow instead of a temporary.
static ALL_SELECTION: Selection = Selection::All;

/// A borrowed view of a vector narrowed by a selection.
///
/// Positions are *logical*: `0..len()` enumerates the selected rows in order,
/// and [`VectorView::rows`] maps each to its physical row in the vector.
#[derive(Debug, Clone, Copy)]
pub struct VectorView<'a> {
    vector: &'a Vector,
    selection: &'a Selection,
    /// Row count of the underlying vector, which bounds `All` and `Mask`.
    row_count: u32,
}

impl<'a> VectorView<'a> {
    /// A view of `vector` narrowed by `selection`.
    ///
    /// `row_count` is the chunk's row count; it bounds `All` and `Mask`
    /// selections and is ignored for `Indices`, which carries its own bound.
    pub fn new(vector: &'a Vector, selection: &'a Selection, row_count: u32) -> Self {
        Self {
            vector,
            selection,
            row_count,
        }
    }

    /// A view of every row of `vector`, with no selection.
    pub fn all(vector: &'a Vector) -> Self {
        Self {
            vector,
            selection: &ALL_SELECTION,
            row_count: vector.len(),
        }
    }

    /// The number of selected rows.
    pub fn len(&self) -> u32 {
        self.selection.active_count(self.row_count)
    }

    /// Whether no row is selected.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The logical type of the viewed values.
    pub fn data_type(&self) -> &'a VeinDataType {
        self.vector.data_type()
    }

    /// The underlying vector.
    pub fn vector(&self) -> &'a Vector {
        self.vector
    }

    /// The selection narrowing this view.
    pub fn selection(&self) -> &'a Selection {
        self.selection
    }

    /// The selected physical rows, in ascending order.
    pub fn rows(&self) -> SelectionIter<'a> {
        self.selection.iter(self.row_count)
    }

    /// Whether physical `row` is non-null in the underlying vector.
    pub fn is_valid(&self, row: u32) -> bool {
        self.vector.is_valid(row)
    }

    /// The `i64` values, resolved by encoding.
    ///
    /// A kernel matches once on the encoding and then runs its inner loop, so
    /// the match never lands in the inner loop (Rule 4) and an encoded vector
    /// is read in its own encoding rather than flattened (Rule 5). `None` means
    /// the vector is not `Int64` or the bridge does not cover its shape.
    pub fn resolve_i64(&self) -> Option<I64Values<'a>> {
        if *self.vector.data_type() != VeinDataType::Int64 {
            return None;
        }
        match self.vector.encoding() {
            Encoding::Flat { values, .. } => Some(I64Values::Flat(values.as_aligned::<i64>()?)),
            Encoding::Constant(LogicalLiteral::Int64(value)) => Some(I64Values::Constant(*value)),
            Encoding::Dictionary { indices, values } => Some(I64Values::Dictionary {
                indices: indices.as_aligned::<u32>()?,
                values: flat_i64(values)?,
            }),
            _ => None,
        }
    }

    /// The `Utf8` values, resolved by encoding.
    pub fn resolve_utf8(&self) -> Option<Utf8Values<'a>> {
        if *self.vector.data_type() != VeinDataType::Utf8 {
            return None;
        }
        match self.vector.encoding() {
            Encoding::Flat {
                values,
                offsets: Some(offsets),
            } => Some(Utf8Values::Flat {
                bytes: values.as_slice(),
                offsets: offsets.as_aligned::<u32>()?,
            }),
            Encoding::Constant(LogicalLiteral::String(value)) => {
                Some(Utf8Values::Constant(value.as_str()))
            }
            Encoding::Dictionary { indices, values } => {
                let (bytes, offsets) = flat_utf8(values)?;
                Some(Utf8Values::Dictionary {
                    indices: indices.as_aligned::<u32>()?,
                    bytes,
                    offsets,
                })
            }
            _ => None,
        }
    }
}

/// The flat `i64` values of `vector`, if it is flat and unaligned to nothing.
fn flat_i64(vector: &Vector) -> Option<&[i64]> {
    match vector.encoding() {
        Encoding::Flat { values, .. } => values.as_aligned::<i64>(),
        _ => None,
    }
}

/// The flat `Utf8` bytes and offsets of `vector`.
fn flat_utf8(vector: &Vector) -> Option<(&[u8], &[u32])> {
    match vector.encoding() {
        Encoding::Flat {
            values,
            offsets: Some(offsets),
        } => Some((values.as_slice(), offsets.as_aligned::<u32>()?)),
        _ => None,
    }
}

/// `Int64` values resolved by encoding.
///
/// A dictionary's values must themselves be flat: nesting an encoding inside a
/// dictionary is the factorized case, which `01-final-architecture.md` §3 lists
/// as later work, so it is refused here rather than silently flattened.
#[derive(Debug, Clone, Copy)]
pub enum I64Values<'a> {
    /// One value per physical row.
    Flat(&'a [i64]),
    /// One value for every row.
    Constant(i64),
    /// Indices into distinct values.
    Dictionary {
        /// One index per physical row.
        indices: &'a [u32],
        /// The distinct values.
        values: &'a [i64],
    },
}

impl<'a> I64Values<'a> {
    /// The value at physical `row`.
    ///
    /// `row` is assumed to be within the view's row count: `Flat` and
    /// `Dictionary` report `None` past their own length, `Constant` cannot
    /// detect it because it has no length of its own.
    pub fn at(&self, row: u32) -> Option<i64> {
        match self {
            I64Values::Flat(values) => values.get(row as usize).copied(),
            I64Values::Constant(value) => Some(*value),
            I64Values::Dictionary { indices, values } => {
                let index = indices.get(row as usize)?;
                values.get(*index as usize).copied()
            }
        }
    }

    /// The values as a slice, when the encoding is flat.
    ///
    /// A kernel that cannot consume an encoding calls this and falls back
    /// explicitly, so the decision is visible at the call site (Rule 25).
    pub fn as_flat(&self) -> Option<&'a [i64]> {
        match self {
            I64Values::Flat(values) => Some(values),
            _ => None,
        }
    }
}

/// `Utf8` values resolved by encoding.
#[derive(Debug, Clone, Copy)]
pub enum Utf8Values<'a> {
    /// Bytes plus `len + 1` end offsets.
    Flat {
        /// The concatenated value bytes.
        bytes: &'a [u8],
        /// `len + 1` end offsets into `bytes`.
        offsets: &'a [u32],
    },
    /// One value for every row.
    Constant(&'a str),
    /// Indices into a flat set of distinct values.
    Dictionary {
        /// One index per physical row.
        indices: &'a [u32],
        /// The concatenated distinct bytes.
        bytes: &'a [u8],
        /// `len + 1` end offsets into `bytes`.
        offsets: &'a [u32],
    },
}

impl<'a> Utf8Values<'a> {
    /// The value at physical `row`.
    pub fn at(&self, row: u32) -> Option<&'a str> {
        match self {
            Utf8Values::Constant(value) => Some(value),
            Utf8Values::Flat { bytes, offsets } => value_at(bytes, offsets, row),
            Utf8Values::Dictionary {
                indices,
                bytes,
                offsets,
            } => {
                let index = *indices.get(row as usize)?;
                value_at(bytes, offsets, index)
            }
        }
    }

    /// The bytes and offsets, when the encoding is flat.
    pub fn as_flat(&self) -> Option<(&'a [u8], &'a [u32])> {
        match self {
            Utf8Values::Flat { bytes, offsets } => Some((bytes, offsets)),
            _ => None,
        }
    }
}

/// The value at `row` in a bytes-plus-offsets pair.
fn value_at<'a>(bytes: &'a [u8], offsets: &'a [u32], row: u32) -> Option<&'a str> {
    let start = *offsets.get(row as usize)? as usize;
    let end = *offsets.get(row as usize + 1)? as usize;
    std::str::from_utf8(bytes.get(start..end)?).ok()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::validity::Validity;

    fn i64_vector() -> Vector {
        Vector::from_i64(&[10, 20, 30, 40])
    }

    #[test]
    fn all_view_covers_every_row() {
        let vector = i64_vector();
        let view = VectorView::all(&vector);
        assert_eq!(view.len(), 4);
        assert!(!view.is_empty());
        assert_eq!(view.data_type(), &VeinDataType::Int64);
        assert_eq!(view.rows().collect::<Vec<_>>(), vec![0, 1, 2, 3]);
        let values = view.resolve_i64().expect("i64");
        assert_eq!(values.as_flat(), Some(&[10, 20, 30, 40][..]));
    }

    /// `09-testing-and-correctness.md` §3: every operator must work with all
    /// five selection states.
    #[test]
    fn every_selection_state_narrows_the_same_way() {
        let vector = i64_vector();
        let expected = vec![0u32, 2, 3];

        let selections = [
            Selection::from_indices(vec![0, 2, 3]),
            Selection::from_mask(vec![0b0000_1101]),
        ];
        for selection in &selections {
            let view = VectorView::new(&vector, selection, 4);
            assert_eq!(view.len(), 3);
            assert_eq!(view.rows().collect::<Vec<_>>(), expected);
            assert_eq!(view.selection(), selection);
        }

        // Dense and All agree.
        let dense = Selection::from_mask(vec![0b1111]);
        let dense_view = VectorView::new(&vector, &dense, 4);
        assert_eq!(
            dense_view.rows().collect::<Vec<_>>(),
            VectorView::all(&vector).rows().collect::<Vec<_>>()
        );

        // None selects nothing.
        let none = Selection::none();
        let none_view = VectorView::new(&vector, &none, 4);
        assert!(none_view.is_empty());
        assert_eq!(none_view.rows().count(), 0);
    }

    /// The intended kernel shape: iterate the selection once, index a slice
    /// inside the loop, with no encoding match in the inner loop.
    #[test]
    fn kernel_shaped_loop_sums_only_selected_rows() {
        let vector = i64_vector();
        let selection = Selection::from_indices(vec![0, 2, 3]);
        let view = VectorView::new(&vector, &selection, 4);

        // Resolve once, outside the loop: the inner loop has no encoding match.
        let values = view.resolve_i64().expect("i64");
        let mut sum = 0i64;
        for row in view.rows() {
            sum += values.at(row).expect("row in range");
        }
        assert_eq!(sum, 10 + 30 + 40);
    }

    #[test]
    fn validity_is_read_per_physical_row() {
        // Rows 1 and 2 valid.
        let vector = i64_vector().with_validity(Validity::from_bitmap(vec![0b0110]));
        let view = VectorView::all(&vector);
        assert!(!view.is_valid(0));
        assert!(view.is_valid(1));
        assert!(view.is_valid(2));
        assert!(!view.is_valid(3));
    }

    #[test]
    fn utf8_fast_path_exposes_bytes_and_offsets() {
        let vector = Vector::from_utf8(&["alpha", "b"]);
        let view = VectorView::all(&vector);
        let (bytes, offsets) = view.resolve_utf8().expect("utf8").as_flat().expect("flat");
        assert_eq!(bytes, b"alphab");
        assert_eq!(offsets, &[0, 5, 6]);
        // A variable-width vector cannot resolve as `Int64`.
        assert!(view.resolve_i64().is_none());
    }

    #[test]
    fn encoded_vectors_resolve_without_being_flattened() {
        let constant = Vector::constant(
            VeinDataType::Int64,
            types::LogicalLiteral::Int64(7),
            3,
        );
        let view = VectorView::all(&constant);
        let resolved = view.resolve_i64().expect("i64");
        assert!(matches!(resolved, I64Values::Constant(7)));
        // A constant has no buffer, so there is no slice — but a kernel reads it
        // through `at` without materialising anything.
        assert!(resolved.as_flat().is_none());
        assert_eq!(resolved.at(2), Some(7));

        let dictionary = Arc::new(Vector::from_utf8(&["x", "y"]));
        let encoded = Vector::dictionary(dictionary, &[1, 1, 0]);
        let view = VectorView::all(&encoded);
        let resolved = view.resolve_utf8().expect("utf8");
        assert!(matches!(resolved, Utf8Values::Dictionary { .. }));
        assert_eq!(resolved.at(0), Some("y"));
        assert_eq!(resolved.at(2), Some("x"));
        // A dictionary is not flat in either type.
        assert!(resolved.as_flat().is_none());
        assert!(view.resolve_i64().is_none());
    }

    #[test]
    fn empty_vector_and_empty_selection() {
        let empty = Vector::from_i64(&[]);
        let view = VectorView::all(&empty);
        assert!(view.is_empty());
        assert_eq!(view.rows().count(), 0);
        assert_eq!(view.resolve_i64().expect("i64").as_flat(), Some(&[][..]));
    }
}
