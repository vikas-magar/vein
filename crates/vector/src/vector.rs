//! Typed value vectors.
//!
//! A vector is `len` values of one type plus the [`Validity`] of those rows.
//! ADR 0001 fixes the layout: fixed-width values are packed inline in a
//! [`BufferRef`] in Arrow-compatible columnar order, and variable-width values
//! carry an offsets buffer, so the Arrow boundary stays a view rather than a
//! conversion (`02-execution-and-vir-lld.md` §15, Rule 22).
//!
//! `Constant` stores no buffer at all — the value is broadcast — which is what
//! makes a literal cheap to pass through a pipeline (`01` §3).

use std::sync::Arc;

use types::{LogicalLiteral, VeinDataType};

use crate::BufferRef;
use crate::validity::Validity;

/// How a vector's values are stored.
///
/// Deliberately not `PartialEq`: the encodings hold shared buffers, so derived
/// equality would be an O(values) byte comparison behind a `==`. Value
/// comparison belongs to the differential tests through the Arrow boundary
/// (`09-testing-and-correctness.md` §4), not to buffer identity.
#[derive(Debug, Clone)]
pub enum Encoding {
    /// One value per row, packed in columnar order.
    Flat {
        /// Fixed-width values, or the concatenated bytes of variable-width ones.
        values: BufferRef,
        /// For variable-width types, `len + 1` end offsets into `values`.
        /// `None` for fixed-width types.
        offsets: Option<BufferRef>,
    },
    /// A single value broadcast to every row; holds no buffer.
    Constant(LogicalLiteral),
    /// Values are looked up through a per-row index into a shared dictionary.
    ///
    /// `05-llm-rules.md` Rule 25 requires every encoding to define the
    /// following, so it is recorded here rather than in prose elsewhere:
    ///
    /// - **semantics**: row `i` holds the value at `indices[i]` in `values`;
    /// - **storage**: one `u32` index per row, plus the shared value vector —
    ///   the value bytes are stored once no matter how many rows reference them;
    /// - **validity**: nullness is carried by this vector's own validity. The
    ///   dictionary holds distinct non-null values, so a null row must not be
    ///   expressed as a null dictionary entry;
    /// - **kernel support**: kernels either read through `get_i64`/`get_str`, or
    ///   take [`Vector::index_at`] and branch on the index directly, which is
    ///   the point of keeping the encoding instead of flattening it (Rule 5);
    /// - **materialisation**: [`Vector::to_flat`];
    /// - **fallback**: `to_flat`, used when a kernel cannot consume the index
    ///   form. Flattening is a per-kernel decision, never a default.
    Dictionary {
        /// One `u32` index per row, into `values`.
        indices: BufferRef,
        /// The distinct values, shared by every vector using this dictionary.
        values: Arc<Vector>,
    },
}

/// `len` values of one type, with row validity.
#[derive(Debug, Clone)]
pub struct Vector {
    /// The logical type of every value.
    data_type: VeinDataType,
    /// Row count, independent of the encoding.
    len: u32,
    /// Which rows are non-null.
    validity: Validity,
    /// How the values are stored.
    encoding: Encoding,
}

/// A shared vector, as a chunk holds its columns.
pub type VectorRef = Arc<Vector>;

impl Vector {
    /// A vector of `i64` values, all valid.
    pub fn from_i64(values: &[i64]) -> Self {
        // The words hold the values directly, so the buffer is aligned for `i64`
        // and `as_aligned::<i64>()` yields a view rather than a copy (ADR 0002).
        let words: Vec<u64> = values.iter().map(|value| *value as u64).collect();
        Self {
            data_type: VeinDataType::Int64,
            len: values.len() as u32,
            validity: Validity::all_valid(),
            encoding: Encoding::Flat {
                values: BufferRef::from_words(words, values.len() * 8),
                offsets: None,
            },
        }
    }

    /// A vector whose every row holds `value`.
    pub fn constant(data_type: VeinDataType, value: LogicalLiteral, len: u32) -> Self {
        Self {
            data_type,
            len,
            validity: Validity::all_valid(),
            encoding: Encoding::Constant(value),
        }
    }

    /// A vector of `Utf8` values, all valid.
    ///
    /// Values are concatenated into one buffer with `len + 1` end offsets,
    /// which is the variable-width half of the layout in ADR 0001 — the same
    /// shape Arrow uses, so the boundary stays a view rather than a conversion.
    pub fn from_utf8(values: &[&str]) -> Self {
        let mut bytes = Vec::new();
        let mut offsets = Vec::with_capacity((values.len() + 1) * 4);
        offsets.extend_from_slice(&0u32.to_ne_bytes());
        for value in values {
            bytes.extend_from_slice(value.as_bytes());
            offsets.extend_from_slice(&(bytes.len() as u32).to_ne_bytes());
        }
        Self {
            data_type: VeinDataType::Utf8,
            len: values.len() as u32,
            validity: Validity::all_valid(),
            encoding: Encoding::Flat {
                values: BufferRef::from_vec(bytes),
                offsets: Some(BufferRef::from_vec(offsets)),
            },
        }
    }

    /// Replaces the row validity.
    pub fn with_validity(mut self, validity: Validity) -> Self {
        self.validity = validity;
        self
    }

    /// A vector whose rows index into a shared dictionary of distinct values.
    ///
    /// The dictionary vector holds the values; this vector holds one `u32` index
    /// per row. Panics in debug builds if an index is past the end of
    /// `values`, which would be a producer bug rather than a data condition.
    pub fn dictionary(values: Arc<Vector>, indices: &[u32]) -> Self {
        debug_assert!(
            indices.iter().all(|index| *index < values.len()),
            "dictionary index out of range"
        );
        let mut bytes = Vec::with_capacity(indices.len() * 4);
        for index in indices {
            bytes.extend_from_slice(&index.to_ne_bytes());
        }
        Self {
            data_type: values.data_type().clone(),
            len: indices.len() as u32,
            validity: Validity::all_valid(),
            encoding: Encoding::Dictionary {
                indices: BufferRef::from_vec(bytes),
                values,
            },
        }
    }

    /// The dictionary index at `row`, when this vector is dictionary-encoded.
    ///
    /// This is the accessor a kernel uses to stay in the encoded domain instead
    /// of materialising (Rule 5).
    pub fn index_at(&self, row: u32) -> Option<u32> {
        match &self.encoding {
            Encoding::Dictionary { indices, .. } if row < self.len => {
                indices.get_u32(row as usize)
            }
            _ => None,
        }
    }

    /// The dictionary a dictionary-encoded vector indexes into.
    pub fn dictionary_values(&self) -> Option<&Arc<Vector>> {
        match &self.encoding {
            Encoding::Dictionary { values, .. } => Some(values),
            _ => None,
        }
    }

    /// Builds a flat vector over an existing value buffer.
    ///
    /// Crate-internal: the buffer must already be laid out for `len` values of
    /// `data_type` in the ADR 0001 layout, which is the invariant the bridges
    /// and kernels uphold. Public construction goes through the typed
    /// constructors above so that invariant cannot be broken from outside.
    pub(crate) fn from_parts(
        data_type: VeinDataType,
        len: u32,
        values: BufferRef,
        offsets: Option<BufferRef>,
        validity: Validity,
    ) -> Self {
        Self {
            data_type,
            len,
            validity,
            encoding: Encoding::Flat { values, offsets },
        }
    }

    /// Row count.
    pub fn len(&self) -> u32 {
        self.len
    }

    /// Whether the vector has no rows.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// The logical type of every value.
    pub fn data_type(&self) -> &VeinDataType {
        &self.data_type
    }

    /// Which rows are non-null.
    pub fn validity(&self) -> &Validity {
        &self.validity
    }

    /// How the values are stored.
    pub fn encoding(&self) -> &Encoding {
        &self.encoding
    }

    /// Whether `row` is non-null. Rows past the end are not valid.
    pub fn is_valid(&self, row: u32) -> bool {
        row < self.len && self.validity.is_valid(row)
    }

    /// Whether `row` is null, including any row past the end.
    pub fn is_null(&self, row: u32) -> bool {
        !self.is_valid(row)
    }

    /// The `i64` value at `row`, or `None` if the row is null, past the end, or
    /// the vector holds another type.
    pub fn get_i64(&self, row: u32) -> Option<i64> {
        if !self.is_valid(row) || self.data_type != VeinDataType::Int64 {
            return None;
        }
        match &self.encoding {
            Encoding::Flat { values, .. } => values.get_i64(row as usize),
            Encoding::Constant(LogicalLiteral::Int64(value)) => Some(*value),
            Encoding::Dictionary { indices, values } => {
                let index = indices.get_u32(row as usize)?;
                values.get_i64(index)
            }
            Encoding::Constant(_) => None,
        }
    }

    /// The `Utf8` value at `row`, or `None` if the row is null, past the end, or
    /// the vector holds another type.
    ///
    /// Returns a borrow into the value buffer; nothing is copied.
    pub fn get_str(&self, row: u32) -> Option<&str> {
        if !self.is_valid(row) || self.data_type != VeinDataType::Utf8 {
            return None;
        }
        match &self.encoding {
            Encoding::Flat {
                values,
                offsets: Some(offsets),
            } => {
                let start = offsets.get_u32(row as usize)? as usize;
                let end = offsets.get_u32(row as usize + 1)? as usize;
                if end < start {
                    return None;
                }
                // Index the parent buffer directly: a slice handle would be a
                // temporary, so the returned `&str` could not borrow from
                // `self` and the call would have to allocate.
                std::str::from_utf8(values.as_slice().get(start..end)?).ok()
            }
            Encoding::Constant(LogicalLiteral::String(value)) => Some(value.as_str()),
            Encoding::Dictionary { indices, values } => {
                let index = indices.get_u32(row as usize)?;
                values.get_str(index)
            }
            _ => None,
        }
    }

    /// Materialises this vector into a flat one.
    ///
    /// This is the fallback Rule 25 requires: a kernel that cannot consume an
    /// encoding calls it, and the cost is explicit at that call site. A vector
    /// that is already flat is returned as-is rather than rebuilt.
    ///
    /// Returns `None` for a type the materialiser does not yet cover rather than
    /// inventing a conversion.
    pub fn to_flat(&self) -> Option<Vector> {
        // Flat is already flat, and a constant stores nothing at all — both are
        // returned as-is, since rebuilding either would allocate for no gain.
        if matches!(
            self.encoding,
            Encoding::Flat { .. } | Encoding::Constant(_)
        ) {
            return Some(self.clone());
        }
        match self.data_type {
            VeinDataType::Int64 => {
                let words: Vec<u64> = (0..self.len)
                    .map(|row| self.get_i64(row).unwrap_or_default() as u64)
                    .collect();
                Some(Vector::from_parts(
                    VeinDataType::Int64,
                    self.len,
                    BufferRef::from_words(words, self.len as usize * 8),
                    None,
                    self.validity.clone(),
                ))
            }
            VeinDataType::Utf8 => {
                let values: Vec<&str> = (0..self.len)
                    .map(|row| self.get_str(row).unwrap_or_default())
                    .collect();
                Some(Vector::from_utf8(&values).with_validity(self.validity.clone()))
            }
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flat_i64_round_trips_every_row() {
        let vector = Vector::from_i64(&[1, -2, i64::MAX]);
        assert_eq!(vector.len(), 3);
        assert!(!vector.is_empty());
        assert_eq!(vector.data_type(), &VeinDataType::Int64);
        assert_eq!(vector.get_i64(0), Some(1));
        assert_eq!(vector.get_i64(1), Some(-2));
        assert_eq!(vector.get_i64(2), Some(i64::MAX));
        assert_eq!(vector.get_i64(3), None);
    }

    #[test]
    fn constant_broadcasts_to_every_row() {
        let vector = Vector::constant(VeinDataType::Int64, LogicalLiteral::Int64(7), 4);
        assert_eq!(vector.len(), 4);
        for row in 0..4 {
            assert_eq!(vector.get_i64(row), Some(7));
        }
        assert_eq!(vector.get_i64(4), None);
        assert!(matches!(vector.encoding(), Encoding::Constant(_)));
    }

    #[test]
    fn null_rows_read_as_none() {
        // Rows 0 and 2 valid; row 1 null.
        let validity = Validity::from_bitmap(vec![0b101]);
        let vector = Vector::from_i64(&[10, 20, 30]).with_validity(validity);
        assert_eq!(vector.get_i64(0), Some(10));
        assert!(vector.is_null(1));
        assert_eq!(vector.get_i64(1), None);
        assert_eq!(vector.get_i64(2), Some(30));
    }

    #[test]
    fn empty_vector_reads_nothing() {
        let vector = Vector::from_i64(&[]);
        assert!(vector.is_empty());
        assert_eq!(vector.len(), 0);
        assert_eq!(vector.get_i64(0), None);
    }

    #[test]
    fn wrong_type_reads_as_none() {
        let vector = Vector::constant(
            VeinDataType::Utf8,
            LogicalLiteral::String("a".to_string()),
            2,
        );
        assert_eq!(vector.get_i64(0), None);
        assert!(vector.is_valid(0));
    }

    #[test]
    fn utf8_round_trips_through_the_offsets_buffer() {
        let vector = Vector::from_utf8(&["alpha", "", "beta"]);
        assert_eq!(vector.len(), 3);
        assert_eq!(vector.data_type(), &VeinDataType::Utf8);
        assert_eq!(vector.get_str(0), Some("alpha"));
        assert_eq!(vector.get_str(1), Some(""));
        assert_eq!(vector.get_str(2), Some("beta"));
        assert_eq!(vector.get_str(3), None);
        // A fixed-width read of a variable-width vector is not a coercion.
        assert_eq!(vector.get_i64(0), None);
    }

    #[test]
    fn utf8_handles_multibyte_and_empty_vectors() {
        let vector = Vector::from_utf8(&["naïve", "日本語"]);
        assert_eq!(vector.get_str(0), Some("naïve"));
        assert_eq!(vector.get_str(1), Some("日本語"));

        let empty = Vector::from_utf8(&[]);
        assert!(empty.is_empty());
        assert_eq!(empty.get_str(0), None);
    }

    #[test]
    fn utf8_null_rows_read_as_none() {
        // Row 0 valid, row 1 null.
        let validity = Validity::from_bitmap(vec![0b01]);
        let vector = Vector::from_utf8(&["x", "y"]).with_validity(validity);
        assert_eq!(vector.get_str(0), Some("x"));
        assert_eq!(vector.get_str(1), None);
    }

    #[test]
    fn constant_string_reads_as_a_borrow() {
        let vector = Vector::constant(VeinDataType::Utf8, LogicalLiteral::String("k".into()), 3);
        assert_eq!(vector.get_str(0), Some("k"));
        assert_eq!(vector.get_str(2), Some("k"));
        assert_eq!(vector.get_str(3), None);
    }

    /// ADR 0002: a fixed-width vector's values are directly viewable, so the
    /// Arrow boundary can share memory instead of copying.
    #[test]
    fn flat_i64_values_are_directly_viewable() {
        let vector = Vector::from_i64(&[1, -2, i64::MAX]);
        let Encoding::Flat { values, .. } = vector.encoding() else {
            panic!("expected a flat encoding");
        };
        let view = values.as_aligned::<i64>().expect("aligned for i64");
        assert_eq!(view, &[1, -2, i64::MAX]);
        // The byte accessors must agree with the aligned view.
        assert_eq!(vector.get_i64(1), Some(-2));
    }

    #[test]
    fn dictionary_resolves_values_through_indices() {
        let dictionary = Arc::new(Vector::from_utf8(&["red", "green", "blue"]));
        let vector = Vector::dictionary(Arc::clone(&dictionary), &[2, 0, 2, 1]);
        assert_eq!(vector.len(), 4);
        assert_eq!(vector.data_type(), &VeinDataType::Utf8);
        assert_eq!(vector.get_str(0), Some("blue"));
        assert_eq!(vector.get_str(1), Some("red"));
        assert_eq!(vector.get_str(2), Some("blue"));
        assert_eq!(vector.get_str(3), Some("green"));
        assert_eq!(vector.get_str(4), None);
    }

    #[test]
    fn dictionary_exposes_indices_for_encoded_kernels() {
        let dictionary = Arc::new(Vector::from_i64(&[100, 200]));
        let vector = Vector::dictionary(Arc::clone(&dictionary), &[1, 0, 1]);
        assert_eq!(vector.index_at(0), Some(1));
        assert_eq!(vector.index_at(1), Some(0));
        assert_eq!(vector.index_at(3), None);
        assert_eq!(vector.get_i64(0), Some(200));

        // The dictionary itself is reachable, so a kernel can read the distinct
        // values once instead of per row.
        let values = vector.dictionary_values().expect("dictionary-encoded");
        assert_eq!(values.len(), 2);

        // A flat vector has no dictionary.
        assert!(Vector::from_i64(&[1]).dictionary_values().is_none());
        assert!(Vector::from_i64(&[1]).index_at(0).is_none());
    }

    #[test]
    fn dictionary_row_validity_is_independent_of_the_dictionary() {
        let dictionary = Arc::new(Vector::from_i64(&[7, 8]));
        let vector =
            Vector::dictionary(dictionary, &[0, 1]).with_validity(Validity::from_bitmap(vec![0b01]));
        assert_eq!(vector.get_i64(0), Some(7));
        assert!(vector.is_null(1));
        assert_eq!(vector.get_i64(1), None);
    }

    #[test]
    fn to_flat_matches_the_encoded_reads() {
        let dictionary = Arc::new(Vector::from_utf8(&["a", "bb", "ccc"]));
        let vector = Vector::dictionary(dictionary, &[2, 0, 0, 1, 2]);
        let flat = vector.to_flat().expect("materialisable");
        assert_eq!(flat.len(), vector.len());
        for row in 0..vector.len() {
            assert_eq!(flat.get_str(row), vector.get_str(row));
        }
        assert!(matches!(flat.encoding(), Encoding::Flat { .. }));

        let ints = Vector::dictionary(Arc::new(Vector::from_i64(&[5, 6])), &[1, 1, 0]);
        let flat = ints.to_flat().expect("materialisable");
        assert_eq!(flat.get_i64(0), Some(6));
        assert_eq!(flat.get_i64(2), Some(5));
    }

    #[test]
    fn to_flat_is_a_no_op_for_already_flat_and_constant_vectors() {
        let flat = Vector::from_i64(&[1, 2]);
        let materialised = flat.to_flat().expect("materialisable");
        assert!(matches!(materialised.encoding(), Encoding::Flat { .. }));

        let constant = Vector::constant(VeinDataType::Int64, LogicalLiteral::Int64(3), 2);
        let materialised = constant.to_flat().expect("materialisable");
        assert!(
            matches!(materialised.encoding(), Encoding::Constant(_)),
            "a constant must not be expanded into a buffer"
        );
    }
}
