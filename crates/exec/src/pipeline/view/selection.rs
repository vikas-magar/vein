//! Zero-copy selection operations for reference-backed views.

use common::Result;
use common::arrow::SelectionVector;
use error::VeinError;

use super::DataChunkView;

impl DataChunkView {
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
