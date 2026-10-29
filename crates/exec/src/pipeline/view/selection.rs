//! Zero-copy selection operations for reference-backed views.

use common::Result;
use common::arrow::SelectionVector;
use error::VeinError;

use super::{DataChunkView, MappingRows, RowMapping, ViewColumn};
use std::collections::HashSet;
use std::mem::size_of;

impl DataChunkView {
    /// Estimated owned metadata bytes, including full retained row maps and selections.
    /// Shared row maps are counted once within this view. This excludes shared
    /// schema metadata, Arrow payloads, allocator headers and allocator rounding.
    pub fn retained_metadata_bytes(&self) -> Result<usize> {
        let mut bytes = size_of::<Self>();
        let mut seen = HashSet::new();
        let mut add = |count: usize, width: usize| -> Result<()> {
            bytes = count.checked_mul(width).and_then(|extra| bytes.checked_add(extra))
                .ok_or_else(|| VeinError::internal("view metadata footprint overflow"))?;
            Ok(())
        };
        add(self.columns.capacity(), size_of::<ViewColumn>())?;
        add(self.selection.retained_bytes(), 1)?;
        for column in &self.columns {
            add(column.memory_leases.capacity(), size_of::<Option<std::sync::Arc<dyn super::ViewMemoryLease>>>())?;
            if seen.insert((0, column.arrays.as_ptr() as usize)) {
                add(column.arrays.len(), size_of::<arrow::array::ArrayRef>())?;
            }
            let mapping = column.row_mapping();
            if seen.insert((1, std::sync::Arc::as_ptr(mapping) as usize)) {
                add(1, size_of::<RowMapping>())?;
                if seen.insert((2, mapping.source_lengths.as_ptr() as usize)) {
                    add(mapping.source_lengths.len(), size_of::<usize>())?;
                }
                match &mapping.rows {
                    MappingRows::Explicit(rows) if seen.insert((3, rows.as_ptr() as usize)) =>
                        add(rows.len(), size_of::<super::SourceRow>())?,
                    MappingRows::Concatenated { end_offsets, .. }
                    if seen.insert((4, end_offsets.as_ptr() as usize)) => add(end_offsets.len(), size_of::<usize>())?,
                    _ => {}
                }
            }
        }
        Ok(bytes)
    }

    /// Selects logical rows by composing a new selection over the same arrays.
    ///
    /// Repeated or reordered rows remain references. No Arrow column is gathered
    /// until an external writer explicitly requires contiguous arrays.
    pub fn select_logical_rows(&self, rows: &[usize]) -> Result<Self> {
        if rows.len() == self.num_rows()
            && rows.iter().enumerate().all(|(position, &row)| position == row)
        {
            return Ok(self.clone());
        }

        let mut mapped = Vec::new();
        mapped.try_reserve_exact(rows.len()).map_err(|error| {
            VeinError::internal(format!("cannot reserve view row selection: {error}"))
        })?;
        for &logical_row in rows {
            if logical_row >= self.num_rows() {
                return Err(VeinError::internal(format!(
                    "view row selection references logical row {logical_row}, but the view has {} rows",
                    self.num_rows()
                )));
            }
            let physical_row = self.selection().get_index(logical_row);
            mapped.push(u32::try_from(physical_row).map_err(|_| {
                VeinError::internal(format!(
                    "view row {physical_row} exceeds the selection index capacity"
                ))
            })?);
        }

        self.with_selection(SelectionVector::new_sparse(mapped))
    }
}
