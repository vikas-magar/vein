//! Compares sort keys directly in their retained Arrow source arrays.

use std::cmp::Ordering;
use std::collections::HashMap;
use std::mem::size_of;

use arrow::compute::SortOptions;
use arrow_cmp::{DynComparator, make_comparator};
use common::protocol::{SortKey, SortPlan};
use error::VeinError;

use crate::memory::{MemoryReservation, QueryMemoryTracker};
use crate::pipeline::DataChunkView;

use super::RowRef;

type ComparatorKey = (usize, usize, usize);

/// A cached Arrow comparator over two source arrays, keyed by sort key and array identity.
pub(super) struct ViewRowOrder<'a> {
    plan: &'a SortPlan,
    comparators: HashMap<ComparatorKey, DynComparator>,
    cache_memory: MemoryReservation,
    cache_bytes: usize,
    comparisons: u64,
    cache_entries_created: u64,
}

impl<'a> ViewRowOrder<'a> {
    pub(super) fn new(plan: &'a SortPlan, tracker: QueryMemoryTracker) -> common::Result<Self> {
        if plan.keys.is_empty() {
            return Err(VeinError::planning(
                "ORDER requires at least one sort key",
                None,
            ));
        }
        Ok(Self {
            plan,
            comparators: HashMap::new(),
            cache_memory: tracker.reserve(0, "sort comparator cache")?,
            cache_bytes: 0,
            comparisons: 0,
            cache_entries_created: 0,
        })
    }

    pub(super) fn compare_rows(
        &mut self,
        chunks: &[Option<DataChunkView>],
        left: RowRef,
        right: RowRef,
    ) -> common::Result<Ordering> {
        let left_view = chunks
            .get(left.chunk)
            .and_then(Option::as_ref)
            .ok_or_else(|| VeinError::internal("sort row references a released input chunk"))?;
        let right_view = chunks
            .get(right.chunk)
            .and_then(Option::as_ref)
            .ok_or_else(|| VeinError::internal("sort row references a released input chunk"))?;
        self.compare_view_rows(left_view, left.row, right_view, right.row)
            .map(|ordering| ordering.then_with(|| left.chunk.cmp(&right.chunk)).then_with(|| left.row.cmp(&right.row)))
    }

    pub(super) fn compare_view_rows(
        &mut self,
        left: &DataChunkView,
        left_row: usize,
        right: &DataChunkView,
        right_row: usize,
    ) -> common::Result<Ordering> {
        self.comparisons = self.comparisons.saturating_add(1);
        for (key_index, key) in self.plan.keys.iter().enumerate() {
            let left_value = left.resolve(key.column as usize, left_row)?;
            let right_value = right.resolve(key.column as usize, right_row)?;
            let ordering = match (left_value, right_value) {
                (None, None) => Ordering::Equal,
                (None, Some(_)) => null_order(key.nulls_first, true),
                (Some(_), None) => null_order(key.nulls_first, false),
                (Some((left_array, left_index)), Some((right_array, right_index))) => {
                    self.compare_arrays(
                        key_index,
                        key,
                        (left_array, left_index),
                        (right_array, right_index),
                    )?
                }
            };
            if ordering != Ordering::Equal {
                return Ok(ordering);
            }
        }
        Ok(Ordering::Equal)
    }

    fn compare_arrays(
        &mut self,
        key_index: usize,
        key: &SortKey,
        left: (&arrow::array::ArrayRef, usize),
        right: (&arrow::array::ArrayRef, usize),
    ) -> common::Result<Ordering> {
        let (left, left_index) = left;
        let (right, right_index) = right;
        let left_identity = std::sync::Arc::as_ptr(left) as *const () as usize;
        let right_identity = std::sync::Arc::as_ptr(right) as *const () as usize;
        let cache_key = (key_index, left_identity, right_identity);
        if !self.comparators.contains_key(&cache_key) {
            let bytes = size_of::<(ComparatorKey, DynComparator)>();
            self.cache_memory.grow(bytes, "sort comparator cache")?;
            let comparator = make_comparator(
                left.as_ref(),
                right.as_ref(),
                SortOptions {
                    descending: !key.ascending,
                    nulls_first: key.nulls_first,
                },
            )
                .map_err(|error| {
                    self.cache_memory.shrink(bytes);
                    VeinError::planning(format!("ORDER BY key cannot be compared: {error}"), None)
                })?;
            self.comparators.insert(cache_key, comparator);
            self.cache_bytes = self.cache_bytes.saturating_add(bytes);
            self.cache_entries_created = self.cache_entries_created.saturating_add(1);
        }
        Ok(self.comparators[&cache_key](left_index, right_index))
    }

    pub(super) fn clear_cache(&mut self) {
        self.comparators.clear();
        self.cache_memory.shrink(self.cache_bytes);
        self.cache_bytes = 0;
    }

    pub(super) fn comparisons(&self) -> u64 {
        self.comparisons
    }

    pub(super) fn cache_entries_created(&self) -> u64 {
        self.cache_entries_created
    }
}

fn null_order(nulls_first: bool, left_is_null: bool) -> Ordering {
    match (nulls_first, left_is_null) {
        (true, true) | (false, false) => Ordering::Less,
        (true, false) | (false, true) => Ordering::Greater,
    }
}
