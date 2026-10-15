//! Batch representations and operations shared by pipeline transforms.

use common::Result;

use super::{DataChunk, DataChunkView};

/// Operations required by row-selection-only pipeline transforms.
///
/// Implementations must change only row-selection metadata and retain all
/// referenced Arrow payload buffers. The trait is intentionally small so
/// transforms can support both materialized chunks and reference-backed views
/// without branching on concrete batch types.
pub trait PipelineBatch: Send + 'static + Sized {
    /// Number of currently selected rows.
    fn num_rows(&self) -> usize;

    /// Return the selected row range `[offset, offset + len)` while retaining
    /// the underlying arrays and row mappings.
    fn slice_rows(self, offset: usize, len: usize) -> Result<Self>;
}

impl PipelineBatch for DataChunk {
    fn num_rows(&self) -> usize {
        DataChunk::num_rows(self)
    }

    fn slice_rows(mut self, offset: usize, len: usize) -> Result<Self> {
        validate_slice(offset, len, self.num_rows())?;
        self.selection = self.selection.slice(offset, len);
        Ok(self)
    }
}

impl PipelineBatch for DataChunkView {
    fn num_rows(&self) -> usize {
        DataChunkView::num_rows(self)
    }

    fn slice_rows(self, offset: usize, len: usize) -> Result<Self> {
        validate_slice(offset, len, self.num_rows())?;
        let selection = self.selection().slice(offset, len);
        self.into_selection(selection)
    }
}

fn validate_slice(offset: usize, len: usize, row_count: usize) -> Result<()> {
    if offset > row_count || len > row_count - offset {
        return Err(error::VeinError::internal(format!(
            "batch row slice [{offset}, {}) exceeds selected row count {row_count}",
            offset.saturating_add(len)
        )));
    }
    Ok(())
}
