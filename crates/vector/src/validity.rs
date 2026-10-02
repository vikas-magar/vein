//! Row validity: which rows are non-null.
//!
//! `vein_design/02-execution-and-vir-lld.md` section 7 requires validity as a
//! bitmap rather than `Option<T>` per row, and requires execution to specialize
//! for the three states below, so that null checks do not become an unavoidable
//! per-element branch.
//!
//! `AllValid` is the common case and holds nothing, mirroring
//! [`Selection::All`](crate::Selection::All): the absence of nulls must not cost
//! an allocation.
//!
//! A set bit means the row is valid, matching Arrow's validity convention at the
//! interoperability boundary (`05-llm-rules.md` Rule 22).

use crate::{MaskRef, bit_is_set, popcount_within};

/// Which rows of a chunk are non-null.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Validity {
    /// No nulls. Holds nothing.
    AllValid,
    /// Every row is null.
    AllNull,
    /// A bitmap: a set bit means the row is valid. Bits at or beyond the row
    /// count are ignored.
    Bitmap(MaskRef),
}

impl Validity {
    /// No row is null.
    pub fn all_valid() -> Self {
        Validity::AllValid
    }

    /// Every row is null.
    pub fn all_null() -> Self {
        Validity::AllNull
    }

    /// Validity from a dense bitmap.
    pub fn from_bitmap(words: Vec<u64>) -> Self {
        Validity::Bitmap(words.into())
    }

    /// Whether `row` is non-null.
    ///
    /// A row beyond the words present is treated as null, so a short bitmap
    /// fails closed rather than reading out of bounds.
    #[inline]
    pub fn is_valid(&self, row: u32) -> bool {
        match self {
            Validity::AllValid => true,
            Validity::AllNull => false,
            Validity::Bitmap(words) => bit_is_set(words, row),
        }
    }

    /// Whether `row` is null.
    pub fn is_null(&self, row: u32) -> bool {
        !self.is_valid(row)
    }

    /// Whether any row can be null.
    ///
    /// This is the specialization hint execution uses to choose between a
    /// no-null kernel and a null-aware one. A bitmap always reports `true`
    /// rather than scanning for an all-ones pattern: the hint must stay cheap.
    pub fn has_nulls(&self) -> bool {
        !matches!(self, Validity::AllValid)
    }

    /// Number of non-null rows.
    pub fn valid_count(&self, row_count: u32) -> u32 {
        match self {
            Validity::AllValid => row_count,
            Validity::AllNull => 0,
            Validity::Bitmap(words) => popcount_within(words, row_count),
        }
    }

    /// Number of null rows.
    pub fn null_count(&self, row_count: u32) -> u32 {
        row_count.saturating_sub(self.valid_count(row_count))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn all_valid_holds_nothing_and_reports_no_nulls() {
        let validity = Validity::all_valid();
        assert_eq!(validity, Validity::AllValid);
        assert!(!validity.has_nulls());
        assert_eq!(validity.valid_count(5), 5);
        assert_eq!(validity.null_count(5), 0);
        assert!(validity.is_valid(4));
        assert!(!validity.is_null(4));
    }

    #[test]
    fn all_null_reports_every_row_null() {
        let validity = Validity::all_null();
        assert!(validity.has_nulls());
        assert_eq!(validity.valid_count(5), 0);
        assert_eq!(validity.null_count(5), 5);
        assert!(validity.is_null(0));
    }

    /// Rows 0, 1 and 3 of 5 are valid.
    #[test]
    fn bitmap_reports_mixed_nulls() {
        let validity = Validity::from_bitmap(vec![0b0000_1011]);
        assert!(validity.has_nulls());
        assert_eq!(validity.valid_count(5), 3);
        assert_eq!(validity.null_count(5), 2);
        let rows: Vec<bool> = (0..5).map(|row| validity.is_valid(row)).collect();
        assert_eq!(rows, vec![true, true, false, true, false]);
    }

    #[test]
    fn bitmap_ignores_bits_beyond_the_row_count() {
        let validity = Validity::from_bitmap(vec![0b0000_1011]);
        assert_eq!(validity.valid_count(8), 3);
        assert_eq!(validity.valid_count(3), 2);
        assert_eq!(validity.valid_count(1), 1);
        assert_eq!(validity.valid_count(0), 0);
        assert_eq!(validity.null_count(3), 1);
    }

    #[test]
    fn empty_and_single_element() {
        assert_eq!(Validity::all_valid().valid_count(0), 0);
        assert_eq!(Validity::all_null().null_count(0), 0);
        assert_eq!(Validity::from_bitmap(vec![]).valid_count(4), 0);
        assert_eq!(Validity::from_bitmap(vec![1]).valid_count(1), 1);
        assert_eq!(Validity::from_bitmap(vec![0]).null_count(1), 1);
    }

    /// A short bitmap fails closed rather than reading past its words.
    #[test]
    fn rows_beyond_the_bitmap_are_null() {
        let validity = Validity::from_bitmap(vec![u64::MAX]);
        assert!(validity.is_valid(63));
        assert!(validity.is_null(64));
    }

    #[test]
    fn word_boundary() {
        // 70 rows: a full first word, and rows 64 and 65 valid in the second.
        let validity = Validity::from_bitmap(vec![u64::MAX, 0b11]);
        assert_eq!(validity.valid_count(70), 66);
        assert_eq!(validity.valid_count(66), 66);
        // Rows 0..=64 are all valid, so there is no null at 65 rows.
        assert_eq!(validity.null_count(65), 0);
        // Row 66 is set to null by the partial second word.
        assert_eq!(validity.valid_count(67), 66);
        assert_eq!(validity.null_count(67), 1);
    }
}
