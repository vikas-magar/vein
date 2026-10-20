//! A bounded-memory, fallible min-heap for ordered merge frontiers.

use std::cmp::Ordering;
use std::mem::size_of;

use common::Result;
use error::VeinError;

use crate::memory::{MemoryReservation, QueryMemoryTracker};

/// Heap allocations owned by a key or payload beyond its inline representation.
pub(in crate::operators::transform::sort) trait HeapSize {
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

impl_inline_heap_size!(u8, u16, u32, u64, u128, usize, i8, i16, i32, i64, i128, isize, bool, char, f32, f64, (usize, usize));

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

/// A merge entry. The heap stores keys and payloads without interpreting them.
pub(super) struct Entry<K, I> {
    key: K,
    tie_breaker: (usize, usize),
    item: I,
    reserved_bytes: usize,
}

/// Generic min-frontier for sorted runs with fallible, caller-defined ordering.
///
/// Unlike `BinaryHeap`, comparisons can return execution errors. This lets a
/// merge compare rows directly in their source arrays without encoding keys.
pub(in crate::operators::transform::sort) struct KWayMerge<K, I> {
    entries: Vec<Entry<K, I>>,
    memory: MemoryReservation,
}

impl<K: HeapSize, I: Copy + HeapSize> KWayMerge<K, I> {
    pub(in crate::operators::transform::sort) fn new(tracker: &QueryMemoryTracker) -> Result<Self> {
        Ok(Self {
            entries: Vec::new(),
            memory: tracker.reserve(0, "sort merge frontier")?,
        })
    }

    pub(in crate::operators::transform::sort) fn push(
        &mut self,
        key: K,
        tie_breaker: (usize, usize),
        item: I,
        compare: &mut impl FnMut(&K, &K) -> Result<Ordering>,
    ) -> Result<()> {
        let reserved_bytes = size_of::<Entry<K, I>>()
            .checked_add(key.heap_size())
            .and_then(|bytes| bytes.checked_add(item.heap_size()))
            .ok_or_else(|| VeinError::internal("sort merge frontier size overflow"))?;
        self.memory.grow(reserved_bytes, "sort merge frontier")?;
        self.entries.push(Entry {
            key,
            tie_breaker,
            item,
            reserved_bytes,
        });
        let index = self.entries.len() - 1;
        if let Err(error) = self.sift_up(index, compare) {
            let entry = self.entries.pop().expect("new frontier entry");
            self.memory.shrink(entry.reserved_bytes);
            return Err(error);
        }
        Ok(())
    }

    pub(in crate::operators::transform::sort) fn pop(
        &mut self,
        compare: &mut impl FnMut(&K, &K) -> Result<Ordering>,
    ) -> Result<Option<I>> {
        if self.entries.is_empty() {
            return Ok(None);
        }
        let last = self.entries.len() - 1;
        self.entries.swap(0, last);
        let entry = self.entries.pop().expect("non-empty frontier");
        self.memory.shrink(entry.reserved_bytes);
        if !self.entries.is_empty() {
            self.sift_down(0, compare)?;
        }
        drop(entry.key);
        Ok(Some(entry.item))
    }

    fn sift_up(
        &mut self,
        mut index: usize,
        compare: &mut impl FnMut(&K, &K) -> Result<Ordering>,
    ) -> Result<()> {
        let mut destination = index;
        while destination > 0 {
            let parent = (destination - 1) / 2;
            if self.compare_entries(index, parent, compare)? != Ordering::Less {
                break;
            }
            destination = parent;
        }
        while index > destination {
            let parent = (index - 1) / 2;
            self.entries.swap(index, parent);
            index = parent;
        }
        Ok(())
    }

    fn sift_down(
        &mut self,
        mut parent: usize,
        compare: &mut impl FnMut(&K, &K) -> Result<Ordering>,
    ) -> Result<()> {
        loop {
            let left = parent * 2 + 1;
            if left >= self.entries.len() {
                return Ok(());
            }
            let right = left + 1;
            let mut smallest = left;
            if right < self.entries.len()
                && self.compare_entries(right, left, compare)? == Ordering::Less
            {
                smallest = right;
            }
            if self.compare_entries(smallest, parent, compare)? != Ordering::Less {
                return Ok(());
            }
            self.entries.swap(parent, smallest);
            parent = smallest;
        }
    }

    fn compare_entries(
        &self,
        left: usize,
        right: usize,
        compare: &mut impl FnMut(&K, &K) -> Result<Ordering>,
    ) -> Result<Ordering> {
        let left = &self.entries[left];
        let right = &self.entries[right];
        Ok(compare(&left.key, &right.key)?.then_with(|| left.tie_breaker.cmp(&right.tie_breaker)))
    }
}
