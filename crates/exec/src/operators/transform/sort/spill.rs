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
mod tests {
    use std::mem::size_of;

    use super::{Entry, KWayMerge};
    use crate::memory::QueryMemoryTracker;
    use error::VeinError;

    #[test]
    fn generic_frontier_merges_keys_and_stably_breaks_ties() {
        let tracker = QueryMemoryTracker::unlimited(common::cancellation::CancellationToken::new());
        let mut merge = KWayMerge::new(&tracker).expect("create generic merge frontier");
        merge.push(3, (0, 0), "run0-row0").unwrap();
        merge.push(1, (1, 0), "run1-row0").unwrap();
        merge.push(1, (0, 4), "run0-row4").unwrap();
        merge.push(1, (0, 2), "run0-row2").unwrap();
        merge.push(2, (1, 1), "run1-row1").unwrap();

        assert_eq!(
            std::iter::from_fn(|| merge.pop()).collect::<Vec<_>>(),
            vec!["run0-row2", "run0-row4", "run1-row0", "run1-row1", "run0-row0"]
        );
    }

    #[test]
    fn generic_frontier_accepts_non_arrow_payloads() {
        let tracker = QueryMemoryTracker::unlimited(common::cancellation::CancellationToken::new());
        let mut merge = KWayMerge::new(&tracker).expect("create generic merge frontier");
        merge.push("b", (0, 0), 20_u64).unwrap();
        merge.push("a", (1, 0), 10_u64).unwrap();

        assert_eq!(merge.pop(), Some(10));
        assert_eq!(merge.pop(), Some(20));
        assert_eq!(merge.pop(), None);
        assert_eq!(tracker.used(), 0, "popping releases frontier reservations");
    }

    #[test]
    fn generic_frontier_respects_budget_and_releases_on_drop() {
        let tracker = QueryMemoryTracker::new(1, common::cancellation::CancellationToken::new());
        let mut merge = KWayMerge::<u64, u64>::new(&tracker).expect("create empty frontier");

        assert!(matches!(
            merge.push(1, (0, 0), 2),
            Err(VeinError::ResourceExhausted { .. })
        ));
        assert_eq!(tracker.used(), 0);

        drop(merge);
        assert_eq!(tracker.used(), 0);
    }

    #[test]
    fn frontier_accounts_inline_entry_once_and_owned_key_capacity_separately() {
        let tracker = QueryMemoryTracker::unlimited(common::cancellation::CancellationToken::new());
        let mut merge = KWayMerge::<String, usize>::new(&tracker).expect("create frontier");
        let key = String::from("owned sort key");
        let expected = size_of::<Entry<String, usize>>() + key.capacity();

        merge.push(key, (0, 0), 0).expect("retain owned key");

        assert_eq!(tracker.used(), expected);
        assert_eq!(merge.pop(), Some(0));
        assert_eq!(tracker.used(), 0, "popping releases entry and owned key bytes");
    }
}
