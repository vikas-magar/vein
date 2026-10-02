//! Ref-counted, aligned byte buffers.
//!
//! ADR 0001 (`vein_design/adr/0001-vector-value-representation.md`) keeps Vein
//! owning the hot-path bytes while Arrow remains an interoperability boundary
//! (Rule 22). ADR 0002 fixes the alignment rule: alignment is a property of the
//! buffer, not of the call site, so the boundary can share memory in both
//! directions instead of copying.
//!
//! Storage is word-backed for that reason. `Vec<u8>` is declared with alignment
//! 1, and relying on the allocator's de-facto wider alignment would fail as an
//! assertion or a misaligned read depending on the build. Backing the buffer
//! with `u64` gives an 8-byte guarantee that covers every fixed-width value type
//! the engine has.
//!
//! Phase 1 hands out a `BufferRef`; Phase 3's `MemoryManager` replaces the
//! handle without changing vector semantics (Rule 34 deviation, follow-up 03 §8).

use std::sync::Arc;

/// A fixed-width value that may be viewed directly out of a buffer.
///
/// Sealed: only primitive types with no padding and no invalid bit patterns are
/// admitted, which is what makes the cast in [`BufferRef::as_aligned`] sound.
/// Types outside this list are read through the byte accessors instead.
pub trait AlignedValue: sealed::Sealed + Copy + Sized + 'static {}

mod sealed {
    /// Prevents downstream implementations of [`super::AlignedValue`].
    pub trait Sealed {}
}

macro_rules! aligned_value {
    ($($type:ty),* $(,)?) => {
        $(
            impl sealed::Sealed for $type {}
            impl AlignedValue for $type {}
        )*
    };
}

aligned_value!(u8, i8, u16, i16, u32, i32, u64, i64, f32, f64);

/// A shared, word-aligned byte buffer, or a window into one.
#[derive(Debug, Clone)]
pub struct BufferRef {
    /// The aligned storage. Never reallocated once created.
    words: Arc<[u64]>,
    /// Meaningful bytes, at most `words.len() * 8`.
    byte_len: usize,
    /// Start of this handle's window, in bytes.
    offset: usize,
    /// Length of this handle's window, in bytes.
    len: usize,
}

impl BufferRef {
    /// Copies a byte vector into aligned storage.
    pub fn from_vec(bytes: Vec<u8>) -> Self {
        let byte_len = bytes.len();
        let mut words = vec![0u64; byte_len.div_ceil(8)];
        for (slot, chunk) in words.iter_mut().zip(bytes.chunks(8)) {
            let mut word = [0u8; 8];
            word[..chunk.len()].copy_from_slice(chunk);
            *slot = u64::from_ne_bytes(word);
        }
        Self {
            words: words.into(),
            byte_len,
            offset: 0,
            len: byte_len,
        }
    }

    /// Adopts word-aligned storage without copying.
    ///
    /// `byte_len` is the number of meaningful bytes at the front of `words`; the
    /// remainder of the final word is padding. Values must already be stored in
    /// their little-endian byte order, which is what the byte view of a word
    /// yields on any target when the producer uses `to_le`.
    pub fn from_words(words: Vec<u64>, byte_len: usize) -> Self {
        assert!(
            byte_len <= words.len() * 8,
            "byte length must fit within the supplied words"
        );
        Self {
            words: words.into(),
            byte_len,
            offset: 0,
            len: byte_len,
        }
    }

    /// Shares an existing allocation.
    pub fn from_bytes(bytes: Arc<[u8]>) -> Self {
        Self::from_vec(bytes.to_vec())
    }

    /// Bytes in this window.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Whether this window is empty.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// The alignment this buffer guarantees.
    pub fn alignment(&self) -> usize {
        std::mem::align_of::<u64>()
    }

    /// The window's bytes.
    pub fn as_slice(&self) -> &[u8] {
        // SAFETY: `u64` has no padding, and `align_of::<u8>() <= align_of::<u64>()`,
        // so viewing the word storage as bytes is sound. The range is in bounds:
        // every handle is built with `offset + len <= byte_len <= words.len() * 8`.
        // The borrow is tied to `self`, so it cannot outlive the `Arc`.
        let bytes = unsafe {
            std::slice::from_raw_parts(self.words.as_ptr().cast::<u8>(), self.words.len() * 8)
        };
        &bytes[self.offset..self.offset + self.len]
    }

    /// Views this window as `T` values, or `None` if it is not exactly aligned
    /// for `T` or does not contain a whole number of them.
    ///
    /// Fails closed rather than reading misaligned memory: a window created by
    /// slicing at a byte offset that is not a multiple of `align_of::<T>()`
    /// reports `None`, and callers fall back to the byte accessors.
    pub fn as_aligned<T: AlignedValue>(&self) -> Option<&[T]> {
        let size = std::mem::size_of::<T>();
        let align = std::mem::align_of::<T>();
        if size == 0 || !self.len.is_multiple_of(size) {
            return None;
        }
        let start = self.words.as_ptr() as usize + self.offset;
        if !start.is_multiple_of(align) {
            return None;
        }
        // SAFETY: alignment was checked above; `len % size == 0` makes a whole
        // number of `T`; `offset + len <= byte_len <= words.len() * 8` keeps the
        // range in bounds. `T` is `Copy` and every bit pattern is valid (the
        // trait is sealed to primitives with no padding), so no invalid `T` can
        // be produced. The borrow is tied to `self`.
        Some(unsafe { std::slice::from_raw_parts(start as *const T, self.len / size) })
    }

    /// A zero-copy view of `len` bytes from `start`, in window-relative
    /// coordinates. Returns `None` if the range is out of bounds.
    pub fn slice(&self, start: usize, len: usize) -> Option<Self> {
        let end = start.checked_add(len)?;
        if end > self.len {
            return None;
        }
        Some(Self {
            words: Arc::clone(&self.words),
            byte_len: self.byte_len,
            offset: self.offset + start,
            len,
        })
    }

    /// Whether two handles point at the same allocation.
    ///
    /// Used by tests to prove a view shares memory rather than copying it.
    pub fn shares_allocation_with(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.words, &other.words)
    }

    /// Reads one byte.
    pub fn get_u8(&self, index: usize) -> Option<u8> {
        self.as_slice().get(index).copied()
    }

    /// Reads a `u64` at value index `index`.
    ///
    /// Value buffers are native-endian, matching Arrow's in-memory layout, so
    /// these accessors agree with [`Self::as_aligned`]. Little-endian byte order
    /// belongs to file formats and is handled at the storage boundary.
    pub fn get_u64(&self, index: usize) -> Option<u64> {
        self.read_array::<8>(index).map(u64::from_ne_bytes)
    }

    /// Reads an `i64` at value index `index`.
    pub fn get_i64(&self, index: usize) -> Option<i64> {
        self.read_array::<8>(index).map(i64::from_ne_bytes)
    }

    /// Reads an `f64` at value index `index`.
    pub fn get_f64(&self, index: usize) -> Option<f64> {
        self.read_array::<8>(index).map(f64::from_ne_bytes)
    }

    /// Reads a `u32` at value index `index`.
    pub fn get_u32(&self, index: usize) -> Option<u32> {
        self.read_array::<4>(index).map(u32::from_ne_bytes)
    }

    /// Reads an `i32` at value index `index`.
    pub fn get_i32(&self, index: usize) -> Option<i32> {
        self.read_array::<4>(index).map(i32::from_ne_bytes)
    }

    /// Reads `N` bytes at value index `index`, or `None` if out of bounds.
    fn read_array<const N: usize>(&self, index: usize) -> Option<[u8; N]> {
        let start = index.checked_mul(N)?;
        let slice = self.as_slice().get(start..start.checked_add(N)?)?;
        slice.try_into().ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Word-backed storage, as the fixed-width path builds it.
    fn aligned_values(values: &[i64]) -> BufferRef {
        let words: Vec<u64> = values.iter().map(|value| *value as u64).collect();
        BufferRef::from_words(words, values.len() * 8)
    }

    fn bytes(values: &[i64]) -> BufferRef {
        let mut raw = Vec::with_capacity(values.len() * 8);
        for value in values {
            raw.extend_from_slice(&value.to_ne_bytes());
        }
        BufferRef::from_vec(raw)
    }

    #[test]
    fn reads_fixed_width_values_by_index() {
        let buffer = bytes(&[1, -2, i64::MAX, i64::MIN]);
        assert_eq!(buffer.len(), 32);
        assert_eq!(buffer.get_i64(0), Some(1));
        assert_eq!(buffer.get_i64(1), Some(-2));
        assert_eq!(buffer.get_i64(2), Some(i64::MAX));
        assert_eq!(buffer.get_i64(3), Some(i64::MIN));
        assert_eq!(buffer.get_i64(4), None);
    }

    #[test]
    fn reads_narrow_and_float_widths() {
        let mut raw = Vec::new();
        raw.extend_from_slice(&7u32.to_ne_bytes());
        raw.extend_from_slice(&(-9i32).to_ne_bytes());
        raw.extend_from_slice(&1.5f64.to_ne_bytes());
        let buffer = BufferRef::from_vec(raw);
        assert_eq!(buffer.get_u32(0), Some(7));
        assert_eq!(buffer.get_i32(1), Some(-9));
        assert_eq!(buffer.get_f64(1), Some(1.5));
        assert_eq!(buffer.get_i32(4), None);
    }

    #[test]
    fn passes_are_aligned_for_fixed_width_values() {
        let buffer = BufferRef::from_vec(vec![1, 2, 3, 4, 5, 6, 7, 8, 9]);
        assert_eq!(buffer.alignment(), std::mem::align_of::<u64>());
        assert!(buffer.as_aligned::<u8>().is_some());
        assert!(buffer.as_aligned::<u64>().is_none(), "9 bytes is not whole u64s");
        assert!(buffer.slice(0, 8).unwrap().as_aligned::<u64>().is_some());
    }

    #[test]
    fn aligned_view_and_byte_accessors_agree() {
        let buffer = aligned_values(&[1, -2, i64::MIN, i64::MAX]);
        let view = buffer.as_aligned::<i64>().expect("aligned for i64");
        assert_eq!(view, &[1, -2, i64::MIN, i64::MAX]);
        for (index, expected) in view.iter().enumerate() {
            assert_eq!(buffer.get_i64(index), Some(*expected));
        }
    }

    /// A window starting mid-word cannot be viewed as `i64`; callers must fall
    /// back to the byte accessors rather than read misaligned memory.
    #[test]
    fn misaligned_or_partial_windows_fail_closed() {
        let buffer = aligned_values(&[1, 2, 3, 4]);

        let misaligned = buffer.slice(4, 16).expect("in bounds");
        assert!(misaligned.as_aligned::<i64>().is_none());
        assert!(misaligned.as_aligned::<u32>().is_some(), "4-byte aligned");

        let partial = buffer.slice(0, 12).expect("in bounds");
        assert!(partial.as_aligned::<i64>().is_none());
        assert!(partial.as_aligned::<u8>().is_some());

        // A whole-word window stays viewable.
        assert!(buffer.slice(8, 16).unwrap().as_aligned::<i64>().is_some());
    }

    #[test]
    fn slices_share_the_allocation_instead_of_copying() {
        let buffer = bytes(&[10, 20, 30, 40]);
        let window = buffer.slice(8, 16).expect("in bounds");
        assert_eq!(window.len(), 16);
        assert!(window.shares_allocation_with(&buffer));
        // Values are read relative to the window, not the parent.
        assert_eq!(window.get_i64(0), Some(20));
        assert_eq!(window.get_i64(1), Some(30));
        assert_eq!(window.get_i64(2), None);
    }

    #[test]
    fn slices_can_be_nested_and_bounds_are_checked() {
        let buffer = bytes(&[10, 20, 30, 40]);
        let inner = buffer.slice(8, 16).and_then(|view| view.slice(8, 8));
        assert_eq!(inner.as_ref().map(BufferRef::len), Some(8));
        assert_eq!(inner.and_then(|view| view.get_i64(0)), Some(30));

        assert!(buffer.slice(32, 1).is_none());
        assert!(buffer.slice(31, 2).is_none());
        assert_eq!(buffer.slice(32, 0).map(|view| view.len()), Some(0));
    }

    #[test]
    fn empty_buffer_yields_nothing() {
        let buffer = BufferRef::from_vec(Vec::new());
        assert!(buffer.is_empty());
        assert_eq!(buffer.as_slice(), &[] as &[u8]);
        assert_eq!(buffer.get_u64(0), None);
        assert_eq!(buffer.get_u8(0), None);
        assert_eq!(buffer.as_aligned::<i64>().map(<[i64]>::len), Some(0));
    }
}
