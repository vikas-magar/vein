use std::sync::Arc;
#[cfg(test)]
use std::sync::OnceLock;

use arrow::array::ArrayRef;
#[cfg(test)]
use arrow::array::{Array, UInt64Array};
use arrow::datatypes::DataType;
use common::Result;
use error::VeinError;
use functions::OutputSegment;

/// Ordered computed runs with independent physical and logical lengths.
#[derive(Debug, Clone)]
pub struct SegmentedArray(Arc<SegmentedArrayInner>);

#[derive(Debug)]
struct SegmentedArrayInner {
    runs: Arc<[OutputSegment]>,
    end_offsets: Arc<[usize]>,
    row_count: usize,
    data_type: DataType,
    #[cfg(test)]
    materialized: OnceLock<std::result::Result<ArrayRef, String>>,
}

impl SegmentedArray {
    #[cfg(test)]
    pub(crate) fn new(arrays: Vec<ArrayRef>) -> Result<Self> {
        Self::from_runs(arrays.into_iter().map(OutputSegment::dense).collect())
    }

    pub(crate) fn from_runs(runs: Vec<OutputSegment>) -> Result<Self> {
        let runs = runs.into_iter().filter(|run| !run.is_empty()).collect::<Vec<_>>();
        let data_type = runs.first()
            .ok_or_else(|| VeinError::internal("segmented expression value cannot be empty"))?
            .data_type().clone();
        let mut row_count = 0usize;
        let mut end_offsets = Vec::with_capacity(runs.len());
        for run in &runs {
            if run.data_type() != &data_type {
                return Err(VeinError::internal("segmented expression runs have inconsistent types"));
            }
            row_count = row_count.checked_add(run.len())
                .ok_or_else(|| VeinError::internal("segmented expression row count overflow"))?;
            end_offsets.push(row_count);
        }
        Ok(Self(Arc::new(SegmentedArrayInner {
            runs: Arc::from(runs),
            end_offsets: Arc::from(end_offsets),
            row_count,
            data_type,
            #[cfg(test)]
            materialized: OnceLock::new(),
        })))
    }

    pub(crate) fn len(&self) -> usize { self.0.row_count }
    pub(crate) fn data_type(&self) -> &DataType { &self.0.data_type }

    #[cfg(test)]
    pub(crate) fn source_arrays(&self) -> Arc<[ArrayRef]> {
        self.0.runs.iter().map(|run| Arc::clone(run.array())).collect()
    }

    fn locate(&self, row: usize, expected_rows: usize) -> Result<(usize, usize)> {
        if expected_rows != self.0.row_count || row >= expected_rows {
            return Err(VeinError::internal("segmented expression row is outside its population"));
        }
        let index = self.0.end_offsets.partition_point(|offset| *offset <= row);
        let start = index.checked_sub(1).map_or(0, |previous| self.0.end_offsets[previous]);
        Ok((index, row - start))
    }

    pub(crate) fn source_at(&self, row: usize, expected_rows: usize) -> Result<(&ArrayRef, usize)> {
        let (index, offset) = self.locate(row, expected_rows)?;
        let run = &self.0.runs[index];
        Ok((run.array(), if run.is_scalar() { 0 } else { offset }))
    }

    #[cfg(test)]
    pub(crate) fn is_materialized(&self) -> bool { self.0.materialized.get().is_some() }

    pub(crate) fn segment(&self, start: usize, max_len: usize, expected_rows: usize)
                          -> Result<OutputSegment>
    {
        if max_len == 0 || start.checked_add(max_len).is_none_or(|end| end > expected_rows) {
            return Err(VeinError::internal("segmented expression range is outside its population"));
        }
        let (index, offset) = self.locate(start, expected_rows)?;
        let run = &self.0.runs[index];
        let len = max_len.min(run.len() - offset);
        let array = if run.is_scalar() { Arc::clone(run.array()) } else { run.array().slice(offset, len) };
        OutputSegment::new(array, len, run.is_scalar())
    }

    #[cfg(test)]
    pub(crate) fn materialized_array(&self) -> Result<ArrayRef> {
        let cached = self.0.materialized.get_or_init(|| {
            let expanded = self.0.runs.iter().map(|run| {
                if run.is_scalar() {
                    let indices = UInt64Array::from(vec![0; run.len()]);
                    arrow::compute::take(run.array().as_ref(), &indices, None)
                } else { Ok(Arc::clone(run.array())) }
            }).collect::<arrow::error::Result<Vec<_>>>().map_err(|error| error.to_string())?;
            let arrays: Vec<&dyn Array> = expanded.iter().map(AsRef::as_ref).collect();
            arrow::compute::concat(&arrays).map_err(|error| error.to_string())
        });
        cached.as_ref().map(Arc::clone)
            .map_err(|error| VeinError::internal(format!("test result materialization failed: {error}")))
    }
}
