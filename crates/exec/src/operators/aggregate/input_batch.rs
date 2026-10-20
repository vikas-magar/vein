//! Shared aggregate input layout for materialized chunks and reference-backed views.

use super::*;
use crate::pipeline::{DataChunk, DataChunkView};

#[derive(Clone)]
pub(super) enum AggregateColumn {
    Array(ArrayRef),
    View(DataChunkView),
}

impl AggregateColumn {
    pub(super) fn source_arrays(&self) -> Result<Vec<ArrayRef>> {
        match self {
            Self::Array(array) => Ok(vec![Arc::clone(array)]),
            Self::View(view) => Ok(view.source_arrays_for_column(0)?.to_vec()),
        }
    }

    pub(super) fn source_row(&self, row: usize) -> Result<Option<(usize, usize)>> {
        match self {
            Self::Array(array) if row < array.len() => Ok(Some((0, row))),
            Self::Array(array) => Err(VeinError::internal(format!(
                "aggregate input row {row} exceeds array length {}",
                array.len()
            ))),
            Self::View(view) => view.source_row(0, row),
        }
    }

    pub(super) fn source_array(&self, source: usize) -> Result<&ArrayRef> {
        match self {
            Self::Array(array) if source == 0 => Ok(array),
            Self::View(view) => view
                .source_arrays_for_column(0)?
                .get(source)
                .ok_or_else(|| VeinError::internal("aggregate source array is out of bounds")),
            Self::Array(_) => Err(VeinError::internal("aggregate source array is out of bounds")),
        }
    }

    pub(super) fn is_null(&self, row: usize) -> Result<bool> {
        let Some((source, physical_row)) = self.source_row(row)? else {
            return Ok(true);
        };
        Ok(self.source_array(source)?.is_null(physical_row))
    }

    pub(super) fn source_value(&self, row: usize) -> Result<(ArrayRef, usize)> {
        let Some((source, physical_row)) = self.source_row(row)? else {
            return Ok((new_null_array(self.data_type()?, 1), 0));
        };
        Ok((Arc::clone(self.source_array(source)?), physical_row))
    }

    fn data_type(&self) -> Result<&arrow::datatypes::DataType> {
        match self {
            Self::Array(array) => Ok(array.data_type()),
            Self::View(view) => view.column_data_type(0),
        }
    }
}

/// Aggregate arguments are Arrow arrays; grouping keys may remain mapped views.
#[derive(Clone)]
pub(super) struct AggregateBatch {
    pub(super) selection: SelectionVector,
    pub(super) key_columns: Vec<Option<GroupKeyColumn>>,
    pub(super) key_arrays: Vec<Vec<ArrayRef>>,
    pub(super) aggregate_columns: Vec<AggregateColumn>,
}

impl AggregateBatch {
    pub(super) fn from_chunk(
        chunk: &DataChunk,
        groups: usize,
        aggregate_count: usize,
        grouping_sets: &[Vec<u32>],
    ) -> Result<Self> {
        Self::validate_width(chunk.num_columns(), groups, aggregate_count)?;
        let mut batch = Self::empty(chunk.selection.clone(), groups, aggregate_count);
        for &column in grouping_sets.iter().flatten() {
            let column = column as usize;
            if batch.key_columns[column].is_none() {
                let array = chunk.column(column);
                batch.key_columns[column] = Some(GroupKeyColumn::new(array)?);
                batch.key_arrays[column].push(Arc::clone(array));
            }
        }
        batch.aggregate_columns = (0..aggregate_count)
            .map(|offset| AggregateColumn::Array(Arc::clone(chunk.column(groups + offset))))
            .collect();
        Ok(batch)
    }

    pub(super) fn from_view(
        view: &DataChunkView,
        groups: usize,
        aggregate_count: usize,
        grouping_sets: &[Vec<u32>],
    ) -> Result<Self> {
        Self::validate_width(view.num_columns(), groups, aggregate_count)?;
        let mut batch = Self::empty(SelectionVector::new_dense(view.num_rows()), groups, aggregate_count);
        for &column in grouping_sets.iter().flatten() {
            let column = column as usize;
            if batch.key_columns[column].is_none() {
                let key_view = view.project(&[column])?.compact_active_rows()?;
                batch.key_arrays[column] = key_view
                    .source_arrays_for_column(0)?
                    .iter()
                    .cloned()
                    .collect();
                batch.key_columns[column] = Some(GroupKeyColumn::from_view(key_view)?);
            }
        }
        batch.aggregate_columns = (0..aggregate_count)
            .map(|offset| {
                view.project(&[groups + offset])?
                    .compact_active_rows()
                    .map(AggregateColumn::View)
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(batch)
    }

    pub(super) fn with_selection(&self, selection: SelectionVector) -> Self {
        Self {
            selection,
            key_columns: self.key_columns.clone(),
            key_arrays: self.key_arrays.clone(),
            aggregate_columns: self.aggregate_columns.clone(),
        }
    }

    fn empty(selection: SelectionVector, groups: usize, aggregate_count: usize) -> Self {
        Self {
            selection,
            key_columns: vec![None; groups],
            key_arrays: vec![Vec::new(); groups],
            aggregate_columns: Vec::with_capacity(aggregate_count),
        }
    }

    fn validate_width(actual: usize, groups: usize, aggregate_count: usize) -> Result<()> {
        let expected = groups
            .checked_add(aggregate_count)
            .ok_or_else(|| VeinError::internal("aggregate input width overflow"))?;
        if actual != expected {
            return Err(VeinError::internal(format!(
                "aggregate expects {expected} columns, got {actual}"
            )));
        }
        Ok(())
    }
}
