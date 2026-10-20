use std::sync::Arc;

use arrow::array::{Array, ArrayRef, UInt32Array, new_empty_array, new_null_array};
use arrow::compute::take;
use common::protocol::RegId;
use common::{Result, VeinError};

use super::{RegValue, RegisterFile};
use crate::pipeline::{DataChunkView, MaterializationReport, RowMapping, ViewColumn};

impl RegisterFile {
    /// Materializes a mapped or segmented value when a dense-only operator reads it.
    pub fn materialize_register(&mut self, reg: RegId) -> Result<MaterializationReport> {
        let value = self.get(reg)?.clone();
        let (array, report) = match value {
            RegValue::ViewColumn(column) => column.materialize_with_report()?,
            RegValue::Segments(segments) => {
                (segments.materialized_array()?, MaterializationReport::default())
            }
            RegValue::Array(_) | RegValue::Scalar(_) => return Ok(MaterializationReport::default()),
        };
        self.set(reg, array)?;
        Ok(report)
    }

    /// Original source column index, if a register retains that lineage.
    pub fn view_column(&self, reg: RegId) -> Result<Option<usize>> {
        Ok(self.get(reg)?.view_column_index())
    }

    /// Input-view column index only when the register still references that view.
    pub(crate) fn view_column_from(
        &self,
        reg: RegId,
        view: &Arc<DataChunkView>,
    ) -> Result<Option<usize>> {
        Ok(match self.get(reg)? {
            RegValue::ViewColumn(column) => column.column_in_view(view),
            RegValue::Array(_) | RegValue::Scalar(_) | RegValue::Segments(_) => None,
        })
    }

    /// Retrieves a register by ID.
    #[inline]
    pub fn get(&self, reg: RegId) -> Result<&RegValue> {
        self.slots.get(reg as usize).ok_or_else(|| {
            VeinError::internal(format!(
                "Register out of bounds: R{reg} accessed but capacity is {}",
                self.slots.len()
            ))
        })
    }

    /// Stores a full-length array in a register.
    #[inline]
    pub fn set(&mut self, reg: RegId, array: ArrayRef) -> Result<()> {
        self.set_value(reg, RegValue::Array(array))
    }

    /// Stores a length-one broadcast scalar in a register.
    pub fn set_scalar(&mut self, reg: RegId, array: ArrayRef) -> Result<()> {
        if array.len() != 1 {
            return Err(VeinError::internal(format!(
                "broadcast scalar for R{reg} must have length 1, got {}",
                array.len()
            )));
        }
        self.set_value(reg, RegValue::Scalar(array))
    }

    /// Stores any register representation in a slot.
    #[inline]
    pub fn set_value(&mut self, reg: RegId, value: RegValue) -> Result<()> {
        let capacity = self.slots.len();
        let slot = self.slots.get_mut(reg as usize).ok_or_else(|| {
            VeinError::internal(format!(
                "Register out of bounds: R{reg} written but capacity is {}",
                capacity
            ))
        })?;
        *slot = value;
        Ok(())
    }

    /// Returns a register as an Arrow array, materializing segments only on demand.
    pub fn to_array(&self, reg: RegId) -> Result<ArrayRef> {
        match self.get(reg)? {
            RegValue::Array(array) | RegValue::Scalar(array) => Ok(Arc::clone(array)),
            RegValue::ViewColumn(column) => column
                .materialize_with_report()
                .map(|(array, _)| array),
            RegValue::Segments(segments) => segments.materialized_array(),
        }
    }

    /// Exposes a computed register through a reference-backed view column.
    /// Segmented results and scalar broadcasts retain their source arrays.
    pub fn to_view_column(&self, reg: RegId, row_count: usize) -> Result<ViewColumn> {
        let value = self.get(reg)?;
        let (arrays, mapping) = match value {
            RegValue::Array(array) => {
                if array.len() != row_count {
                    return Err(VeinError::internal(format!(
                        "VM output register R{reg} has {} rows, expected {row_count}",
                        array.len()
                    )));
                }
                let mapping = RowMapping::identity(Arc::from(vec![row_count]), 0, row_count)?;
                (Arc::from(vec![Arc::clone(array)]), mapping)
            }
            RegValue::Scalar(array) => {
                if array.len() != 1 {
                    return Err(VeinError::internal(format!(
                        "VM scalar output register R{reg} has {} values, expected one",
                        array.len()
                    )));
                }
                let mapping = RowMapping::repeated(Arc::from(vec![1]), 0, 0, row_count)?;
                (Arc::from(vec![Arc::clone(array)]), mapping)
            }
            RegValue::Segments(segments) => {
                if segments.len() != row_count {
                    return Err(VeinError::internal(format!(
                        "VM segmented output register R{reg} has {} rows, expected {row_count}",
                        segments.len()
                    )));
                }
                let arrays = segments.source_arrays();
                let lengths = arrays.iter().map(|array| array.len()).collect::<Vec<_>>();
                let mapping = RowMapping::concatenated(Arc::from(lengths))?;
                (arrays, mapping)
            }
            RegValue::ViewColumn(column) => return column.to_view_column(row_count),
        };
        ViewColumn::new(
            arrays,
            value.data_type().clone(),
            Arc::new(mapping),
        )
    }

    /// Extracts selected registers as arrays, materializing only at this boundary.
    pub fn project(&self, out_regs: &[RegId]) -> Result<Vec<ArrayRef>> {
        out_regs.iter().map(|&reg| self.to_array(reg)).collect()
    }

    /// Produces dense columns for the legacy `DataChunk` execution boundary.
    /// Scalar registers are broadcast here; the view execution path keeps them
    /// as repeated row mappings and does not allocate a per-row column.
    pub fn project_dense(&self, out_regs: &[RegId], row_count: usize) -> Result<Vec<ArrayRef>> {
        out_regs
            .iter()
            .map(|&reg| match self.get(reg)? {
                RegValue::Scalar(array) => broadcast_scalar(array, row_count),
                _ => self.to_array(reg),
            })
            .collect()
    }
}

fn broadcast_scalar(array: &ArrayRef, row_count: usize) -> Result<ArrayRef> {
    if array.len() != 1 {
        return Err(VeinError::internal(format!(
            "cannot broadcast scalar array with {} values",
            array.len()
        )));
    }
    if row_count == 0 {
        return Ok(new_empty_array(array.data_type()));
    }
    if row_count == 1 {
        return Ok(Arc::clone(array));
    }
    if array.is_null(0) {
        return Ok(new_null_array(array.data_type(), row_count));
    }

    let indices = UInt32Array::from(vec![0; row_count]);
    take(array.as_ref(), &indices, None)
        .map_err(|error| VeinError::internal(format!("broadcast scalar column failed: {error}")))
}
