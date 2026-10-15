//! Reusable primitives for merging sorted spill runs.
//!
//! Arrow IPC readers and row references stay in the sort operator. This module
//! owns only the ordered frontier, so other sorted spill formats can reuse it
//! without depending on Arrow or the sort plan.

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::mem::size_of;

use common::Result;
use error::VeinError;

use crate::memory::{MemoryReservation, QueryMemoryTracker};

/// Reports heap bytes owned outside the inline value stored in a frontier entry.
///
/// The entry allocation already includes each value's inline representation.
/// Implementations therefore report only separately allocated capacity. Shared
/// backing state is charged by its owner and should report zero here.
pub(super) trait HeapSize {
    fn heap_size(&self) -> usize;
}

macro_rules! impl_inline_heap_size {
    ($($ty:ty),+ $(,)?) => {
        $(
            impl HeapSize for $ty {
                fn heap_size(&self) -> usize { 0 }
            }
        )+
    };
}

impl_inline_heap_size!(u8, u16, u32, u64, u128, usize, i8, i16, i32, i64, i128, isize, bool, char, f32, f64);

impl HeapSize for &str {
    fn heap_size(&self) -> usize {
        0
    }
}

impl<T> HeapSize for &T {
    fn heap_size(&self) -> usize {
        0
    }
}

impl HeapSize for (usize, usize) {
    fn heap_size(&self) -> usize {
        0
    }
}

impl<T> HeapSize for Vec<T> {
    fn heap_size(&self) -> usize {
        self.capacity().saturating_mul(size_of::<T>())
    }
}

impl HeapSize for String {
    fn heap_size(&self) -> usize {
        self.capacity()
    }
}

/// A queued item from one sorted run.
///
/// The heap reverses key and tie-breaker ordering so its root is the next item
/// in ascending merge order. The payload is generic and is never inspected by
/// the queue.
struct Entry<K, I> {
    key: K,
    tie_breaker: (usize, usize),
    item: I,
    reserved_bytes: usize,
}

impl<K: Ord, I> PartialEq for Entry<K, I> {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key && self.tie_breaker == other.tie_breaker
    }
}

impl<K: Ord, I> Eq for Entry<K, I> {}

impl<K: Ord, I> PartialOrd for Entry<K, I> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<K: Ord, I> Ord for Entry<K, I> {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .key
            .cmp(&self.key)
            .then_with(|| other.tie_breaker.cmp(&self.tie_breaker))
    }
}

/// Generic min-frontier for a k-way merge over already sorted runs.
///
/// Callers provide a stable (run, position) tie-breaker and retain ownership
/// of the payload. This avoids constraining the merge algorithm to Arrow rows,
/// encoded sort keys, or a particular spill format.
pub(super) struct KWayMerge<K, I> {
    entries: BinaryHeap<Entry<K, I>>,
    memory: MemoryReservation,
}

impl<K: Ord + HeapSize, I: Copy + HeapSize> KWayMerge<K, I> {
    pub(super) fn new(tracker: &QueryMemoryTracker) -> Result<Self> {
        Ok(Self {
            entries: BinaryHeap::new(),
            memory: tracker.reserve(0, "sort merge frontier")?,
        })
    }

    pub(super) fn push(
        &mut self,
        key: K,
        tie_breaker: (usize, usize),
        item: I,
    ) -> Result<()> {
        let reserved_bytes = size_of::<Entry<K, I>>()
            .checked_add(key.heap_size())
            .and_then(|bytes| bytes.checked_add(item.heap_size()))
            .ok_or_else(|| VeinError::internal("sort merge frontier size overflow"))?;
        self.memory
            .grow(reserved_bytes, "sort merge frontier")?;
        self.entries.push(Entry {
            key,
            tie_breaker,
            item,
            reserved_bytes,
        });
        Ok(())
    }

    pub(super) fn pop(&mut self) -> Option<I> {
        let entry = self.entries.pop()?;
        let Entry {
            key,
            tie_breaker: _,
            item,
            reserved_bytes,
        } = entry;
        drop(key);
        self.memory.shrink(reserved_bytes);
        Some(item)
    }
}

#[cfg(test)]
mod tests;
