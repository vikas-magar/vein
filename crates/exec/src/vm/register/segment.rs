use std::sync::Arc;

use arrow::array::ArrayRef;
use common::Result;
use error::VeinError;

use super::{RegValue, RegisterDatum, ViewColumnValue};

/// One kernel input run; a missing array denotes an outer-join NULL run.
pub(crate) struct RegisterSegment {
    array: Option<ArrayRef>,
    scalar: bool,
    len: usize,
}

impl RegisterSegment {
    pub(crate) fn array(array: ArrayRef, len: usize) -> Self {
        Self { array: Some(array), scalar: false, len }
    }

    pub(crate) fn scalar(array: ArrayRef, len: usize) -> Self {
        Self { array: Some(array), scalar: true, len }
    }

    pub(crate) fn null_extended(len: usize) -> Self {
        Self { array: None, scalar: false, len }
    }

    pub(crate) fn len(&self) -> usize { self.len }
    pub(crate) fn is_null_extended(&self) -> bool { self.array.is_none() }

    pub(crate) fn take_prefix(&mut self, len: usize) -> Self {
        let array = self.array.as_ref().map(|array| {
            if self.scalar { Arc::clone(array) } else { array.slice(0, len) }
        });
        if !self.scalar && let Some(array) = &mut self.array {
            *array = array.slice(len, self.len - len);
        }
        self.len -= len;
        Self { array, scalar: self.scalar, len }
    }

    pub(crate) fn datum(&self) -> Option<RegisterDatum<'_>> {
        self.array.as_ref().map(|array| RegisterDatum { array, scalar: self.scalar })
    }
}

impl RegValue {
    /// Returns zero-copy Arrow slices covering one contiguous input segment.
    pub(crate) fn segment(&self, start: usize, max_len: usize, row_count: usize) -> Result<RegisterSegment> {
        let end = start.checked_add(max_len)
            .ok_or_else(|| VeinError::internal("VM input segment range overflow"))?;
        if max_len == 0 || end > row_count {
            return Err(VeinError::internal(format!("VM input segment {start}..{end} is outside {row_count} rows")));
        }
        match self {
            Self::Array(array) => {
                if array.len() != row_count {
                    return Err(VeinError::internal(format!("VM array input has {} rows, expected {row_count}", array.len())));
                }
                Ok(RegisterSegment::array(array.slice(start, max_len), max_len))
            }
            Self::Scalar(array) => Ok(RegisterSegment::scalar(Arc::clone(array), max_len)),
            Self::ViewColumn(column) => column.segment(start, max_len, row_count),
            Self::Segments(segments) => {
                let (array, len) = segments.segment(start, max_len, row_count)?;
                Ok(RegisterSegment::array(array, len))
            }
        }
    }
}

impl ViewColumnValue {
    fn segment(&self, start: usize, max_len: usize, row_count: usize) -> Result<RegisterSegment> {
        let view = &self.0.view;
        if view.num_rows() != row_count || start.checked_add(max_len).is_none_or(|end| end > row_count) {
            return Err(VeinError::internal("VM view input segment is outside its row range"));
        }
        let Some((first_array, first_row)) = view.resolve(self.0.column, start)? else {
            let mut len = 1;
            while len < max_len && view.resolve(self.0.column, start + len)?.is_none() { len += 1; }
            return Ok(RegisterSegment::null_extended(len));
        };
        let mut len = 1;
        while len < max_len {
            match view.resolve(self.0.column, start + len)? {
                Some((array, row)) if Arc::ptr_eq(first_array, array)
                    && first_row.checked_add(len) == Some(row) => len += 1,
                _ => break,
            }
        }
        Ok(RegisterSegment::array(first_array.slice(first_row, len), len))
    }
}
