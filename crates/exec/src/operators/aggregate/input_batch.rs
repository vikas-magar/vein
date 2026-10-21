//! Aggregate input layout over reference-backed view columns.

use super::*;
use crate::pipeline::DataChunkView;

#[path = "input_batch/source.rs"]
mod source;
pub(super) use source::AggregateFunctionInputSource;

#[derive(Clone)]
pub(super) enum AggregateColumn {
    View(DataChunkView),
}

impl AggregateColumn {
    pub(super) fn source_row(&self, row: usize) -> Result<Option<(usize, usize)>> {
        let Self::View(view) = self;
        view.source_row(0, row)
    }

    pub(super) fn source_array(&self, source: usize) -> Result<&ArrayRef> {
        let Self::View(view) = self;
        view.source_arrays_for_column(0)?
            .get(source)
            .ok_or_else(|| VeinError::internal("aggregate source array is out of bounds"))
    }

    pub(super) fn data_type(&self) -> Result<&arrow::datatypes::DataType> {
        let Self::View(view) = self;
        view.column_data_type(0)
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
                    .to_vec();
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
