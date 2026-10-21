//! Arrow-backed key references retained by aggregate spill records.

use std::sync::Arc;

use arrow::array::ArrayRef;
use common::Result;
use error::VeinError;

use crate::operators::aggregate::group_key::GroupKeyColumn;

#[derive(Clone)]
pub(in crate::operators::aggregate) enum AggregateGroupKeyRef {
    Arrow { array: ArrayRef, row: usize },
    Column { column: Arc<GroupKeyColumn>, row: usize },
}

impl AggregateGroupKeyRef {
    pub(in crate::operators::aggregate) fn from_array(array: ArrayRef, row: usize) -> Result<Self> {
        if row >= array.len() {
            return Err(VeinError::internal("aggregate key row is outside its Arrow array"));
        }
        Ok(Self::Arrow { array, row })
    }

    pub(in crate::operators::aggregate) fn from_column(column: Arc<GroupKeyColumn>, row: usize) -> Result<Self> {
        if row >= column.len() {
            return Err(VeinError::internal("aggregate key row is outside its key column"));
        }
        Ok(Self::Column { column, row })
    }

    pub(in crate::operators::aggregate) fn row(&self) -> usize {
        match self {
            Self::Arrow { row, .. } | Self::Column { row, .. } => *row,
        }
    }

    pub(in crate::operators::aggregate) fn group_key_column(&self) -> Result<GroupKeyColumn> {
        match self {
            Self::Arrow { array, .. } => GroupKeyColumn::new(array),
            Self::Column { column, .. } => Ok((**column).clone()),
        }
    }

    pub(in crate::operators::aggregate) fn source_arrays(&self) -> Result<Vec<ArrayRef>> {
        match self {
            Self::Arrow { array, .. } => Ok(vec![Arc::clone(array)]),
            Self::Column { column, .. } => column.source_arrays(),
        }
    }

    pub(in crate::operators::aggregate) fn source_row(&self) -> Result<Option<(ArrayRef, usize)>> {
        match self {
            Self::Arrow { array, row } => Ok(Some((Arc::clone(array), *row))),
            Self::Column { column, row } => column.source_row(*row),
        }
    }
}
