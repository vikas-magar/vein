use std::sync::Arc;
#[cfg(test)]
use std::sync::OnceLock;

use arrow::array::ArrayRef;
#[cfg(test)]
use arrow::array::Array;
#[cfg(test)]
use arrow::compute::concat;
use arrow::datatypes::DataType;
use common::Result;
use error::VeinError;

/// Immutable Arrow arrays representing one logical expression result in row order.
#[derive(Debug, Clone)]
pub struct SegmentedArray(Arc<SegmentedArrayInner>);

#[derive(Debug)]
struct SegmentedArrayInner {
    arrays: Arc<[ArrayRef]>,
    end_offsets: Arc<[usize]>,
    row_count: usize,
    data_type: DataType,
    #[cfg(test)]
    materialized: OnceLock<std::result::Result<ArrayRef, String>>,
}

impl SegmentedArray {
    pub(crate) fn new(arrays: Vec<ArrayRef>) -> Result<Self> {
        let arrays = arrays
            .into_iter()
            .filter(|array| !array.is_empty())
            .collect::<Vec<_>>();
        let data_type = arrays
            .first()
            .ok_or_else(|| VeinError::internal("segmented expression value cannot be empty"))?
            .data_type()
            .clone();
        let mut row_count = 0usize;
        let mut end_offsets = Vec::with_capacity(arrays.len());
        for (index, array) in arrays.iter().enumerate() {
            if array.data_type() != &data_type {
                return Err(VeinError::internal(format!(
                    "segmented expression array {index} has type {:?}, expected {data_type:?}",
                    array.data_type()
                )));
            }
            row_count = row_count.checked_add(array.len()).ok_or_else(|| {
                VeinError::internal("segmented expression row count overflow")
            })?;
            end_offsets.push(row_count);
        }
        Ok(Self(Arc::new(SegmentedArrayInner {
            arrays: Arc::from(arrays),
            end_offsets: Arc::from(end_offsets),
            row_count,
            data_type,
            #[cfg(test)]
            materialized: OnceLock::new(),
        })))
    }

    pub(crate) fn len(&self) -> usize {
        self.0.row_count
    }

    pub(crate) fn data_type(&self) -> &DataType {
        &self.0.data_type
    }

    #[cfg(test)]
    pub(crate) fn source_arrays(&self) -> Arc<[ArrayRef]> {
        Arc::clone(&self.0.arrays)
    }

    pub(crate) fn source_at(
        &self,
        row: usize,
        expected_rows: usize,
    ) -> Result<(&ArrayRef, usize)> {
        if expected_rows != self.0.row_count || row >= expected_rows {
            return Err(VeinError::internal(format!(
                "segmented expression row {row} is invalid for {expected_rows} rows"
            )));
        }
        let index = self.0.end_offsets.partition_point(|offset| *offset <= row);
        let segment_start = index
            .checked_sub(1)
            .map_or(0, |previous| self.0.end_offsets[previous]);
        let array = self.0.arrays.get(index).ok_or_else(|| {
            VeinError::internal("segmented expression source offset is out of bounds")
        })?;
        Ok((array, row - segment_start))
    }

    #[cfg(test)]
    pub(crate) fn is_materialized(&self) -> bool {
        self.0.materialized.get().is_some()
    }

    pub(crate) fn segment(
        &self,
        start: usize,
        max_len: usize,
        expected_rows: usize,
    ) -> Result<(ArrayRef, usize)> {
        let end = start.checked_add(max_len).ok_or_else(|| {
            VeinError::internal("segmented expression range overflow")
        })?;
        if expected_rows != self.0.row_count || max_len == 0 || end > expected_rows {
            return Err(VeinError::internal(format!(
                "segmented expression range {start}..{end} is invalid for {expected_rows} rows"
            )));
        }
        let index = self.0.end_offsets.partition_point(|offset| *offset <= start);
        let segment_start = index.checked_sub(1)
            .map_or(0, |previous| self.0.end_offsets[previous]);
        let segment_end = *self.0.end_offsets.get(index).ok_or_else(|| {
            VeinError::internal("segmented expression offset is out of bounds")
        })?;
        let len = max_len.min(segment_end - start);
        Ok((self.0.arrays[index].slice(start - segment_start, len), len))
    }

    #[cfg(test)]
    pub(crate) fn materialized_array(&self) -> Result<ArrayRef> {
        let cached = self.0.materialized.get_or_init(|| {
            let arrays: Vec<&dyn Array> = self.0.arrays.iter().map(AsRef::as_ref).collect();
            concat(&arrays).map_err(|error| error.to_string())
        });
        cached.as_ref().map(Arc::clone).map_err(|error| {
            VeinError::internal(format!("segmented expression materialization failed: {error}"))
        })
    }
}
