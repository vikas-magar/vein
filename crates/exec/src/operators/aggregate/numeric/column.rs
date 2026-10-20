//! Typed, source-backed aggregate arguments.

use arrow::array::{Array, Float16Array};
use common::Result;
use error::VeinError;
use types::VeinDataType;

use super::super::input_batch::AggregateColumn;
use crate::pipeline::DataChunkView;

/// Typed wrappers over retained source arrays plus optional row lineage.
pub(in crate::operators::aggregate) struct MappedArray<'a, T> {
    sources: Vec<T>,
    view: Option<&'a DataChunkView>,
}

impl<'a, T: Array + Clone + 'static> MappedArray<'a, T> {
    pub(super) fn new(column: &'a AggregateColumn, expected: &VeinDataType) -> Result<Self> {
        let arrays = column.source_arrays()?;
        let sources = arrays
            .iter()
            .enumerate()
            .map(|(index, array)| {
                array.as_any().downcast_ref::<T>().cloned().ok_or_else(|| {
                    VeinError::internal(format!(
                        "aggregate input source {index} does not match {expected:?}"
                    ))
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let AggregateColumn::View(view) = column;
        let view = Some(view);
        Ok(Self { sources, view })
    }

    /// Reads a non-null typed value through the input's row mapping.
    pub(in crate::operators::aggregate) fn value<R>(
        &self,
        row: usize,
        read: impl FnOnce(&T, usize) -> R,
    ) -> Result<Option<R>> {
        let location = if let Some(view) = self.view {
            view.source_row(0, row)?
        } else {
            let array = self
                .sources
                .first()
                .ok_or_else(|| VeinError::internal("aggregate input has no source arrays"))?;
            if row >= array.len() {
                return Err(VeinError::internal(format!(
                    "aggregate input row {row} exceeds array length {}",
                    array.len()
                )));
            }
            Some((0, row))
        };
        let Some((source, physical_row)) = location else {
            return Ok(None);
        };
        let array = self
            .sources
            .get(source)
            .ok_or_else(|| VeinError::internal("aggregate source array is out of bounds"))?;
        if physical_row >= array.len() {
            return Err(VeinError::internal("aggregate source row is out of bounds"));
        }
        if array.is_null(physical_row) {
            return Ok(None);
        }
        Ok(Some(read(array, physical_row)))
    }
}

impl MappedArray<'_, Float16Array> {
    pub(in crate::operators::aggregate) fn value_as_f32(&self, row: usize) -> Result<Option<f32>> {
        self.value(row, |array, physical_row| array.value(physical_row).to_f32())
    }

    pub(in crate::operators::aggregate) fn value_as_f64(&self, row: usize) -> Result<Option<f64>> {
        self.value(row, |array, physical_row| array.value(physical_row).to_f64())
    }
}
