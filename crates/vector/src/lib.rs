//! Vein's execution-time vector representation.
//!
//! This crate is Phase 1 of `vein_design/07-development-plan.md`: the
//! representation the engine executes on, rather than Arrow's object model.
//!
//! Rule 22 of `vein_design/05-llm-rules.md` keeps Arrow at the interoperability
//! boundary and forbids it becoming the unavoidable internal hot-path
//! representation; this crate is where the internal representation lives.

#![deny(missing_docs)]

/// Which rows of a chunk are active.
pub mod selection;
/// Row validity: which rows are non-null.
pub mod validity;
/// Ref-counted byte buffers backing vector values.
pub mod buffer;
/// Chunks of columnar data in flight.
pub mod chunk;
/// Interoperability boundary with Arrow.
pub mod arrow;
/// Typed value vectors.
pub mod vector;
/// Borrowed views over a vector and a selection.
pub mod view;

pub use buffer::BufferRef;
pub use chunk::DataChunk;
pub use selection::{IndexRef, MaskRef, Selection, SelectionIter};
pub use vector::{Encoding, Vector, VectorRef};
pub use view::{I64Values, Utf8Values, VectorView};
pub use validity::Validity;

/// Counts set bits, ignoring bits at or beyond `row_count`.
///
/// Shared by [`Selection`] and [`Validity`], which both index rows with a
/// LSB-first bitmap; a second copy would be the kind of duplicated abstraction
/// Rule 15 of `vein_design/05-llm-rules.md` forbids.
pub(crate) fn popcount_within(words: &[u64], row_count: u32) -> u32 {
    let mut count = 0;
    for (index, word) in words.iter().enumerate() {
        let base = index as u32 * 64;
        if base >= row_count {
            break;
        }
        let remaining = row_count - base;
        let word = if remaining >= 64 {
            *word
        } else {
            word & ((1u64 << remaining) - 1)
        };
        count += word.count_ones();
    }
    count
}

/// Number of 64-bit words needed to hold `row_count` bits.
pub fn words_for(row_count: u32) -> usize {
    (row_count as usize).div_ceil(64)
}

/// Clears bits at or beyond `row_count`.
///
/// Shared by every bitmap in the engine: a mask that is inverted or intersected
/// can otherwise claim rows that do not exist.
pub fn trim_words(words: &mut [u64], row_count: u32) {
    let remainder = row_count % 64;
    if remainder != 0
        && let Some(last) = words.last_mut()
    {
        *last &= (1u64 << remainder) - 1;
    }
}

/// Whether bit `row` is set in a LSB-first bitmap.
///
/// Bits beyond the words present are unset.
pub(crate) fn bit_is_set(words: &[u64], row: u32) -> bool {
    let index = row as usize / 64;
    let shift = row % 64;
    words.get(index).is_some_and(|word| (word >> shift) & 1 == 1)
}
