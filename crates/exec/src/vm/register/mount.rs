use std::sync::Arc;

use arrow::array::ArrayRef;
use common::protocol::RegId;
use common::Result;
use error::VeinError;

use crate::pipeline::{DataChunk, DataChunkView};

use super::{RegValue, RegisterFile};

impl RegisterFile {
    /// Mounts all chunk vectors into the lowest registers.
    pub fn mount_chunk(&mut self, chunk: &DataChunk) -> Result<()> {
        self.mount_columns(
            chunk.physical_len(),
            chunk.vectors.iter().cloned().enumerate().map(|(register, array)| {
                (register as RegId, array)
            }),
        )
    }

    /// Mounts selected arrays into their corresponding registers without copying payloads.
    pub fn mount_columns(
        &mut self,
        chunk_len: usize,
        columns: impl IntoIterator<Item=(RegId, ArrayRef)>,
    ) -> Result<()> {
        self.chunk_len = chunk_len;
        self.slots.fill(RegValue::Array(Arc::clone(&self.empty)));
        for (register, array) in columns {
            let index = register as usize;
            self.validate_register(register, index)?;
            if array.len() != chunk_len {
                return Err(VeinError::internal(format!(
                    "input register R{register} has {} rows, expected {chunk_len}",
                    array.len()
                )));
            }
            self.slots[index] = RegValue::Array(array);
        }
        Ok(())
    }

    /// Mounts selected view columns as row-map references, without gathering values.
    pub fn mount_view_columns(
        &mut self,
        chunk_len: usize,
        view: Arc<DataChunkView>,
        columns: impl IntoIterator<Item=RegId>,
    ) -> Result<()> {
        if view.num_rows() != chunk_len {
            return Err(VeinError::internal(format!(
                "view has {} selected rows, expected {chunk_len}",
                view.num_rows()
            )));
        }
        self.chunk_len = chunk_len;
        self.slots.fill(RegValue::Array(Arc::clone(&self.empty)));
        for register in columns {
            let index = register as usize;
            self.validate_register(register, index)?;
            self.slots[index] = RegValue::view_column(Arc::clone(&view), index)?;
        }
        Ok(())
    }

    fn validate_register(&self, register: RegId, index: usize) -> Result<()> {
        if index >= self.slots.len() {
            return Err(VeinError::internal(format!(
                "input register R{register} exceeds register file capacity {}",
                self.slots.len()
            )));
        }
        Ok(())
    }
}
