//! Typed, reference-backed access to join keys in selected chunk views.
//!
//! Resolving a key retains typed Arrow array handles and row lineage. It never
//! interleaves selected key values into a temporary Arrow array.

mod array;

use std::mem::size_of;
use std::time::Instant;

use common::Result;
use error::VeinError;
use types::LogicalLiteral;

use crate::pipeline::DataChunkView;

use super::JoinKeys;
use array::KeyArray;

#[derive(Debug, Clone)]
pub(super) struct ChunkKeys {
    columns: Vec<KeyArray>,
    source_columns: Vec<usize>,
}

impl ChunkKeys {
    pub(super) fn resolve_view(
        keys: &JoinKeys,
        view: &DataChunkView,
    ) -> Result<(Self, u64)> {
        let started = Instant::now();
        let columns = keys
            .columns
            .iter()
            .map(|&column| KeyArray::resolve(column, view.source_arrays_for_column(column)?))
            .collect::<Result<Vec<_>>>()?;
        let elapsed = u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX);
        Ok((
            Self {
                columns,
                source_columns: keys.columns.clone(),
            },
            elapsed,
        ))
    }

    fn source_location(
        &self,
        key_index: usize,
        view: &DataChunkView,
        row: usize,
    ) -> Result<Option<(usize, usize)>> {
        let column = *self.source_columns
            .get(key_index)
            .ok_or_else(|| VeinError::internal("join key source mapping is incomplete"))?;
        view.source_row(column, row)
    }

    pub(super) fn any_null(&self, view: &DataChunkView, row: usize) -> Result<bool> {
        for (key_index, column) in self.columns.iter().enumerate() {
            let Some((source, source_row)) = self.source_location(key_index, view, row)? else {
                return Ok(true);
            };
            if column.is_null(source, source_row)? {
                return Ok(true);
            }
        }
        Ok(false)
    }

    pub(super) fn hash_and_single_i64(
        &self,
        row: usize,
        view: &DataChunkView,
    ) -> Result<Option<(u64, Option<i64>)>> {
        if self.is_single_i64() {
            let Some((source, source_row)) = self.source_location(0, view, row)? else {
                return Ok(None);
            };
            let Some(value) = self.columns[0].int64_value(source, source_row)? else {
                return Ok(None);
            };
            return Ok(Some((super::fnv1a(&value.to_ne_bytes(), super::FNV_OFFSET), Some(value))));
        }

        let mut hash = super::FNV_OFFSET;
        for (key_index, column) in self.columns.iter().enumerate() {
            let Some((source, source_row)) = self.source_location(key_index, view, row)? else {
                return Ok(None);
            };
            let Some(next) = column.hash_into(source, source_row, hash)? else {
                return Ok(None);
            };
            hash = next;
        }
        Ok(Some((hash, None)))
    }

    pub(super) fn is_single_i64(&self) -> bool {
        matches!(self.columns.as_slice(), [column] if column.is_single_i64())
    }

    pub(super) fn single_i64_value(
        &self,
        view: &DataChunkView,
        row: usize,
    ) -> Result<Option<i64>> {
        let Some((source, source_row)) = self.source_location(0, view, row)? else {
            return Ok(None);
        };
        self.columns[0].int64_value(source, source_row)
    }

    pub(super) fn equals(
        &self,
        row: usize,
        view: &DataChunkView,
        other: &Self,
        other_row: usize,
        other_view: &DataChunkView,
    ) -> Result<bool> {
        if self.columns.len() != other.columns.len() {
            return Ok(false);
        }
        for (key_index, (left, right)) in self.columns.iter().zip(&other.columns).enumerate() {
            let Some((left_source, left_row)) = self.source_location(key_index, view, row)? else {
                return Ok(false);
            };
            let Some((right_source, right_row)) =
                other.source_location(key_index, other_view, other_row)?
            else {
                return Ok(false);
            };
            if !left.value_eq(left_source, left_row, right, right_source, right_row)? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    pub(super) fn compare_value(
        &self,
        key_index: usize,
        row: usize,
        view: &DataChunkView,
        other: &Self,
        other_row: usize,
        other_view: &DataChunkView,
    ) -> Result<Option<std::cmp::Ordering>> {
        let Some((source, row)) = self.source_location(key_index, view, row)? else {
            return Ok(None);
        };
        let Some((other_source, other_row)) =
            other.source_location(key_index, other_view, other_row)?
        else {
            return Ok(None);
        };
        self.columns[key_index].compare_values(source, row, &other.columns[key_index], other_source, other_row)
    }

    pub(super) fn logical_literal(
        &self,
        key_index: usize,
        row: usize,
        view: &DataChunkView,
    ) -> Result<LogicalLiteral> {
        let (source, row) = self
            .source_location(key_index, view, row)?
            .ok_or_else(|| VeinError::internal("join key bound references a null-extended row"))?;
        self.columns[key_index].logical_literal(source, row)
    }

    pub(super) fn has_compatible_types(&self, other: &Self) -> bool {
        self.columns.len() == other.columns.len()
            && self.columns.iter().zip(&other.columns).all(|(left, right)| left.same_type(right))
    }

    pub(super) fn metadata_bytes(&self) -> usize {
        self.columns
            .capacity()
            .saturating_mul(size_of::<KeyArray>())
            .saturating_add(
                self.columns
                    .iter()
                    .map(KeyArray::metadata_bytes)
                    .fold(0usize, usize::saturating_add),
            )
            .saturating_add(
                self.source_columns
                    .capacity()
                    .saturating_mul(size_of::<usize>()),
            )
    }
}
