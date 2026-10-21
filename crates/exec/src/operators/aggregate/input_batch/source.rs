//! Zero-copy adapter from aggregate view mappings to function input segments.

use std::sync::Arc;

use arrow::datatypes::DataType;
use common::arrow::SelectionVector;
use common::Result;
use error::VeinError;
use functions::{FunctionInputRow, FunctionInputSegment, FunctionInputSource};

use super::AggregateColumn;

/// Presents a selected aggregate argument as contiguous Arrow-backed runs.
pub(in crate::operators::aggregate) struct AggregateFunctionInputSource<'a> {
    column: &'a AggregateColumn,
    selection: &'a SelectionVector,
    data_type: DataType,
}

impl<'a> AggregateFunctionInputSource<'a> {
    pub(in crate::operators::aggregate) fn new(
        column: &'a AggregateColumn,
        selection: &'a SelectionVector,
    ) -> Result<Self> {
        Ok(Self {
            column,
            selection,
            data_type: column.data_type()?.clone(),
        })
    }

    fn selected_row(&self, position: usize) -> Result<Option<(usize, usize)>> {
        self.column.source_row(self.selection.get_index(position))
    }

    fn same_run(
        &self,
        start: usize,
        length: usize,
        source: usize,
        first_row: usize,
        repeated: bool,
    ) -> Result<bool> {
        let Some((next_source, next_row)) = self.selected_row(start + length)? else {
            return Ok(false);
        };
        Ok(next_source == source
            && if repeated {
                next_row == first_row
            } else {
                next_row == first_row + length
            })
    }

    fn mapped_segment(
        &self,
        start: usize,
        max_len: usize,
        source: usize,
        row: usize,
    ) -> Result<FunctionInputSegment> {
        let repeated = max_len > 1 && self.same_run(start, 1, source, row, true)?;
        let sequential = max_len > 1 && self.same_run(start, 1, source, row, false)?;
        let mut len = 1;
        if repeated || sequential {
            while len < max_len && self.same_run(start, len, source, row, repeated)? {
                len += 1;
            }
        }
        let array = Arc::clone(self.column.source_array(source)?).slice(row, if repeated { 1 } else { len });
        FunctionInputSegment::new(Some(array), repeated, len, self.data_type.clone())
    }

    fn null_segment(&self, start: usize, max_len: usize) -> Result<FunctionInputSegment> {
        let mut len = 1;
        while len < max_len && self.selected_row(start + len)?.is_none() {
            len += 1;
        }
        FunctionInputSegment::new(None, false, len, self.data_type.clone())
    }
}

impl FunctionInputSource for AggregateFunctionInputSource<'_> {
    fn data_type(&self) -> &DataType {
        &self.data_type
    }

    fn is_scalar(&self) -> bool {
        false
    }

    fn segment(
        &self,
        start: usize,
        max_len: usize,
        row_count: usize,
    ) -> Result<FunctionInputSegment> {
        if row_count != self.selection.size()
            || max_len == 0
            || start.checked_add(max_len).is_none_or(|end| end > row_count)
        {
            return Err(VeinError::internal("aggregate function segment is outside its selection"));
        }
        match self.selected_row(start)? {
            Some((source, row)) => self.mapped_segment(start, max_len, source, row),
            None => self.null_segment(start, max_len),
        }
    }

    fn source_row(&self, row: usize, row_count: usize) -> Result<Option<FunctionInputRow>> {
        if row_count != self.selection.size() || row >= row_count {
            return Err(VeinError::internal("aggregate function row is outside its selection"));
        }
        let Some((source, physical_row)) = self.selected_row(row)? else {
            return Ok(None);
        };
        Ok(Some(FunctionInputRow::new(
            Arc::clone(self.column.source_array(source)?),
            physical_row,
        )))
    }
}
