//! Which rows of a chunk are active.
//!
//! `Selection` is the Phase 1 representation from
//! `vein_design/02-execution-and-vir-lld.md` §6 and `01-final-architecture.md`
//! §4. The common case is free: `All` must not allocate a selection vector (an
//! explicit invariant of `01` §12), and conversion between representations is
//! deferred until a consumer benefits from it.
//!
//! `MaskRef`/`IndexRef` are the design's own names for the ref-counted buffer
//! handles. They are `Arc<[..]>` placeholders: Phase 3's buffer manager replaces
//! the handle with `BufferRef` without changing this type's semantics.

use std::sync::Arc;

use crate::popcount_within;

/// A bit-packed mask over rows, one bit per row, LSB-first within each word.
pub type MaskRef = Arc<[u64]>;

/// Explicit row indices, strictly increasing.
pub type IndexRef = Arc<[u32]>;

/// The active rows of a chunk.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Selection {
    /// Every row of the chunk is active. Holds nothing.
    All,
    /// No row is active.
    None,
    /// A dense bitmap. Bits at or beyond `row_count` are ignored.
    Mask(MaskRef),
    /// Explicit row indices, strictly increasing.
    Indices(IndexRef),
}

impl Selection {
    /// Every row of the chunk is active.
    pub fn all() -> Self {
        Selection::All
    }

    /// No row is active.
    pub fn none() -> Self {
        Selection::None
    }

    /// A selection from explicit row indices.
    ///
    /// # Panics
    ///
    /// In debug builds, if `indices` is not strictly increasing.
    pub fn from_indices(indices: Vec<u32>) -> Self {
        debug_assert!(
            indices.windows(2).all(|pair| pair[0] < pair[1]),
            "selection indices must be strictly increasing"
        );
        Selection::Indices(indices.into())
    }

    /// A selection from a dense bitmap.
    pub fn from_mask(words: Vec<u64>) -> Self {
        Selection::Mask(words.into())
    }

    /// Number of active rows.
    pub fn active_count(&self, row_count: u32) -> u32 {
        match self {
            Selection::All => row_count,
            Selection::None => 0,
            Selection::Indices(indices) => indices.len() as u32,
            Selection::Mask(mask) => popcount_within(mask, row_count),
        }
    }

    /// Whether no row is active.
    pub fn is_empty(&self, row_count: u32) -> bool {
        self.active_count(row_count) == 0
    }

    /// The row index at `position` among the active rows.
    pub fn row_at(&self, position: u32, row_count: u32) -> Option<u32> {
        match self {
            Selection::All => (position < row_count).then_some(position),
            Selection::None => None,
            Selection::Indices(indices) => indices.get(position as usize).copied(),
            Selection::Mask(_) => self.iter(row_count).nth(position as usize),
        }
    }

    /// Iterates the active row indices in ascending order.
    ///
    /// This is the primitive kernels consume; it borrows and never allocates.
    /// `row_count` bounds `All` and `Mask`; `Indices` is self-describing, so the
    /// parameter is unused there rather than re-filtering every element on the
    /// hot path.
    pub fn iter(&self, row_count: u32) -> SelectionIter<'_> {
        match self {
            Selection::All => SelectionIter::All { next: 0, end: row_count },
            Selection::None => SelectionIter::None,
            Selection::Indices(indices) => SelectionIter::Indices { indices, next: 0 },
            Selection::Mask(words) => SelectionIter::Mask {
                words,
                word: 0,
                bits: words.first().copied().unwrap_or(0),
                end: row_count,
            },
        }
    }

    /// Materialises the active rows as explicit indices.
    ///
    /// Allocates for `All` and `Mask`; prefer [`Self::iter`] in kernels.
    pub fn to_indices(&self, row_count: u32) -> IndexRef {
        match self {
            Selection::Indices(indices) => Arc::clone(indices),
            other => other.iter(row_count).collect::<Vec<u32>>().into(),
        }
    }

    /// Keeps the active rows for which `keep` returns `true`.
    ///
    /// A selection that survives filtering intact is returned unchanged, so a
    /// filter that keeps everything does not materialise an index list and a
    /// dense selection stays dense. That mirrors the engine's existing
    /// selection semantics rather than inventing new ones.
    pub fn filter(&self, row_count: u32, mut keep: impl FnMut(u32) -> bool) -> Selection {
        let mut kept = Vec::new();
        let mut dropped_any = false;
        for row in self.iter(row_count) {
            if keep(row) {
                kept.push(row);
            } else {
                dropped_any = true;
            }
        }
        if !dropped_any {
            return self.clone();
        }
        if kept.is_empty() {
            return Selection::None;
        }
        Selection::from_indices(kept)
    }

    /// Keeps the active rows where `mask` has a set bit.
    ///
    /// This is the filter kernel: a predicate evaluates to a bit per row, and
    /// the selection narrows to the rows whose bit is set.
    ///
    /// Implemented as a **bitmap intersection**, not a row-by-row filter.
    /// Intersecting two bitmaps is O(rows/64) and the result stays a `Mask`, so a
    /// filter neither walks every row nor materialises an index list —
    /// `01-final-architecture.md` §11 ranks avoiding materialisation above
    /// avoiding branches. Measured on a 500k-row scan, the row-by-row version
    /// was 4× slower than the Arrow path it replaces.
    pub fn intersect_mask(&self, row_count: u32, mask: &[u64]) -> Selection {
        let length = crate::words_for(row_count);
        match self {
            Selection::None => Selection::None,
            Selection::All => {
                let mut words = vec![0u64; length];
                for (index, word) in words.iter_mut().enumerate() {
                    *word = mask.get(index).copied().unwrap_or(0);
                }
                crate::trim_words(&mut words, row_count);
                mask_selection(words)
            }
            Selection::Mask(existing) => {
                let mut words = vec![0u64; length];
                for (index, word) in words.iter_mut().enumerate() {
                    let left = existing.get(index).copied().unwrap_or(0);
                    let right = mask.get(index).copied().unwrap_or(0);
                    *word = left & right;
                }
                crate::trim_words(&mut words, row_count);
                mask_selection(words)
            }
            Selection::Indices(indices) => {
                let kept: Vec<u32> = indices
                    .iter()
                    .copied()
                    .filter(|row| crate::bit_is_set(mask, *row))
                    .collect();
                if kept.is_empty() {
                    Selection::None
                } else if kept.len() == indices.len() {
                    self.clone()
                } else {
                    Selection::from_indices(kept)
                }
            }
        }
    }

    /// Keeps at most `limit` active rows, skipping the first `offset`.
    ///
    /// A window that covers every active row is returned unchanged. Otherwise
    /// the window is materialised as indices: `01-final-architecture.md` §4
    /// defines `Selection` as `All | None | Mask | Indices`, with no
    /// contiguous-range form, so a window of a dense selection cannot stay
    /// dense. Adding a range variant would need an ADR, since it changes the
    /// selection contract.
    pub fn slice(&self, row_count: u32, offset: u32, limit: u32) -> Selection {
        let active = self.active_count(row_count);
        if offset == 0 && limit >= active {
            return self.clone();
        }
        if offset >= active || limit == 0 {
            return Selection::None;
        }
        let kept: Vec<u32> = self
            .iter(row_count)
            .skip(offset as usize)
            .take(limit as usize)
            .collect();
        Selection::from_indices(kept)
    }
}

/// A `Mask` selection, or `None` when no bit is set.
///
/// Normalising an all-zero mask to `None` keeps "selects nothing" as one state
/// rather than two, so consumers cannot disagree about it.
fn mask_selection(words: Vec<u64>) -> Selection {
    if words.iter().all(|word| *word == 0) {
        Selection::None
    } else {
        Selection::Mask(words.into())
    }
}

/// Iterator over a [`Selection`]'s active row indices.
#[derive(Debug)]
pub enum SelectionIter<'a> {
    /// Ascending range.
    All {
        /// Next row to yield.
        next: u32,
        /// Exclusive end.
        end: u32,
    },
    /// Yields nothing.
    None,
    /// Walks a bitmap.
    Mask {
        /// The bitmap words.
        words: &'a [u64],
        /// Current word.
        word: usize,
        /// Remaining set bits of `word`.
        bits: u64,
        /// Exclusive row bound.
        end: u32,
    },
    /// Walks explicit indices.
    Indices {
        /// The indices.
        indices: &'a [u32],
        /// Next index to yield.
        next: usize,
    },
}

impl Iterator for SelectionIter<'_> {
    type Item = u32;

    fn next(&mut self) -> Option<u32> {
        match self {
            SelectionIter::All { next, end } => {
                let row = *next;
                *next += 1;
                (row < *end).then_some(row)
            }
            SelectionIter::None => None,
            SelectionIter::Indices { indices, next } => {
                let row = indices.get(*next).copied();
                *next += 1;
                row
            }
            SelectionIter::Mask { words, word, bits, end } => loop {
                if *bits != 0 {
                    let bit = bits.trailing_zeros();
                    *bits &= *bits - 1;
                    let row = (*word as u32) * 64 + bit;
                    if row >= *end {
                        return None;
                    }
                    return Some(row);
                }
                *word += 1;
                if *word >= words.len() {
                    return None;
                }
                *bits = words[*word];
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Rows {0, 2, 5} of 8, expressed as each selection state.
    fn indices_state() -> Selection {
        Selection::from_indices(vec![0, 2, 5])
    }

    fn sparse_mask_state() -> Selection {
        Selection::from_mask(vec![0b0010_0101])
    }

    fn dense_mask_state() -> Selection {
        Selection::from_mask(vec![0b1111_1111])
    }

    #[test]
    fn all_covers_every_row() {
        let selection = Selection::all();
        assert_eq!(selection, Selection::All);
        assert_eq!(selection.active_count(5), 5);
        assert_eq!(selection.iter(5).collect::<Vec<_>>(), vec![0, 1, 2, 3, 4]);
        assert_eq!(selection.active_count(0), 0);
    }

    #[test]
    fn none_is_empty() {
        let selection = Selection::none();
        assert!(selection.is_empty(5));
        assert_eq!(selection.iter(5).count(), 0);
        assert_eq!(selection.row_at(0, 5), None);
    }

    #[test]
    fn every_state_agrees_on_the_same_row_set() {
        let expected = vec![0, 2, 5];
        for selection in [indices_state(), sparse_mask_state()] {
            assert_eq!(selection.active_count(8), 3);
            assert_eq!(selection.iter(8).collect::<Vec<_>>(), expected);
            assert_eq!(&*selection.to_indices(8), &expected[..]);
            for (position, row) in expected.iter().enumerate() {
                assert_eq!(selection.row_at(position as u32, 8), Some(*row));
            }
            assert_eq!(selection.row_at(3, 8), None);
        }
    }

    #[test]
    fn dense_mask_matches_all() {
        let dense = dense_mask_state();
        assert_eq!(dense.active_count(8), Selection::All.active_count(8));
        assert_eq!(
            dense.iter(8).collect::<Vec<_>>(),
            Selection::All.iter(8).collect::<Vec<_>>()
        );
    }

    /// A mask's trailing bits beyond `row_count` belong to no row and must be
    /// ignored — the boundary case that makes mask handling easy to get wrong.
    #[test]
    fn mask_ignores_bits_beyond_the_row_count() {
        let selection = sparse_mask_state();
        assert_eq!(selection.active_count(8), 3);
        assert_eq!(selection.active_count(6), 3);
        assert_eq!(selection.active_count(3), 2);
        assert_eq!(selection.active_count(1), 1);
        assert_eq!(selection.active_count(0), 0);
        assert_eq!(selection.iter(3).collect::<Vec<_>>(), vec![0, 2]);
    }

    #[test]
    fn full_and_partial_word_boundaries() {
        // 70 rows: spans two words with a partial second word.
        let mut words = vec![u64::MAX; 2];
        words[1] = 0b11; // rows 64 and 65 only
        let selection = Selection::from_mask(words);
        assert_eq!(selection.active_count(70), 66);
        assert_eq!(selection.active_count(66), 66);
        assert_eq!(selection.active_count(65), 65);

        let rows: Vec<u32> = selection.iter(66).collect();
        assert_eq!(rows.len(), 66);
        assert_eq!(rows[64], 64);
        assert_eq!(rows[65], 65);
    }

    #[test]
    fn empty_and_single_element() {
        assert_eq!(Selection::all().iter(0).count(), 0);
        // `Indices` carries its own bound, so a zero row count does not truncate
        // it; only `All` and `Mask` are bounded by the chunk's row count.
        assert_eq!(indices_state().iter(0).count(), 3);
        assert_eq!(Selection::from_mask(vec![0b0010_0101]).iter(0).count(), 0);
        assert_eq!(Selection::from_indices(vec![0]).iter(1).collect::<Vec<_>>(), vec![0]);
        assert!(Selection::from_mask(vec![]).is_empty(5));
    }

    #[test]
    fn to_indices_of_indices_shares_the_buffer() {
        let selection = indices_state();
        let shared = match &selection {
            Selection::Indices(original) => Arc::ptr_eq(original, &selection.to_indices(8)),
            _ => false,
        };
        assert!(shared);
    }

    #[test]
    fn filtering_a_selection_that_survives_intact_keeps_its_representation() {
        assert_eq!(Selection::all().filter(4, |_| true), Selection::All);

        let dense = dense_mask_state();
        assert_eq!(dense.clone().filter(8, |_| true), dense);

        // An `Indices` selection shares its buffer rather than rebuilding it.
        let indices = indices_state();
        let kept = indices.clone().filter(8, |_| true);
        let (Selection::Indices(before), Selection::Indices(after)) = (&indices, &kept) else {
            panic!("expected indices");
        };
        assert!(Arc::ptr_eq(before, after));
    }

    #[test]
    fn filtering_drops_rows_and_reports_none_when_nothing_survives() {
        let selection = Selection::all();
        assert_eq!(
            selection
                .filter(5, |row| row % 2 == 0)
                .iter(5)
                .collect::<Vec<_>>(),
            vec![0, 2, 4]
        );
        assert_eq!(selection.filter(5, |_| false), Selection::None);
    }

    #[test]
    fn mask_intersection_narrows_to_set_bits() {
        // Rows 0, 2 and 5 are active; the mask keeps 0 and 5.
        let selection = indices_state();
        assert_eq!(
            selection
                .intersect_mask(8, &[0b0010_0001])
                .iter(8)
                .collect::<Vec<_>>(),
            vec![0, 5]
        );
        // An all-zero mask selects nothing.
        assert!(selection.intersect_mask(8, &[0]).is_empty(8));
    }

    #[test]
    fn slicing_a_window_out_of_a_dense_selection() {
        let selection = Selection::all();
        assert_eq!(
            selection.slice(10, 3, 4).iter(10).collect::<Vec<_>>(),
            vec![3, 4, 5, 6]
        );
        // A window covering everything is returned unchanged.
        assert_eq!(selection.slice(10, 0, 10), Selection::All);
        assert_eq!(selection.slice(10, 0, 99), Selection::All);
        // A window past the end, or of zero length, selects nothing.
        assert_eq!(selection.slice(10, 10, 1), Selection::None);
        assert_eq!(selection.slice(10, 0, 0), Selection::None);
    }

    #[test]
    fn slicing_a_sparse_selection_walks_the_active_rows() {
        // Active rows are 0, 2 and 5; a window addresses them by position.
        let selection = indices_state();
        assert_eq!(selection.slice(8, 1, 1).iter(8).collect::<Vec<_>>(), vec![2]);
        assert_eq!(
            selection.slice(8, 1, 2).iter(8).collect::<Vec<_>>(),
            vec![2, 5]
        );
    }
}
