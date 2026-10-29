//! Size-limited Arrow IPC packets for the browser transport boundary.

mod writer;
#[cfg(test)]
mod tests;

use arrow::array::RecordBatch;
use arrow::ipc::writer::StreamWriter;
use common::{Result, VeinError};
use writer::LimitedBuffer;

/// Keeps a measured row-size hint across packets; the byte-limited writer enforces the bound.
pub(crate) struct PacketEncoder {
    limit: usize,
    rows_hint: Option<usize>,
}

impl PacketEncoder {
    pub(crate) fn new(limit: usize) -> Self { Self { limit, rows_hint: None } }

    /// Encodes the next row slice as an independent IPC stream, without a result-wide buffer.
    pub(crate) fn encode(&mut self, batch: &RecordBatch, offset: usize) -> Result<(Vec<u8>, usize)> {
        let limit = self.limit;
        if limit == 0 || offset > batch.num_rows() {
            return Err(VeinError::internal("invalid IPC packet limit or offset"));
        }
        // Estimate a fitting slice before serialization. The capped writer remains the authority
        // for variable-width values, dictionaries, padding and schema overhead.
        let logical_bytes = batch.columns().iter().try_fold(0usize, |sum, column| {
            column.to_data().get_slice_memory_size().map(|bytes| sum.saturating_add(bytes))
        })?;
        let estimated_rows = limit.saturating_sub(1024).saturating_mul(batch.num_rows())
            / logical_bytes.max(1);
        let mut rows = (batch.num_rows() - offset).min(8192).min(self.rows_hint.unwrap_or(estimated_rows).max(1));
        loop {
            let mut output = LimitedBuffer::new(limit);
            let encoded = (|| -> arrow::error::Result<()> {
                let mut writer = StreamWriter::try_new(&mut output, batch.schema().as_ref())?;
                writer.write(&batch.slice(offset, rows))?;
                writer.finish()
            })();
            if !output.exceeded() {
                encoded?;
                let bytes = output.into_bytes();
                if rows > 0 { self.rows_hint = Some(rows.saturating_mul(limit.saturating_sub(1024)) / bytes.len().max(1)); }
                return Ok((bytes, rows));
            }
            if rows <= 1 {
                return Err(VeinError::unsupported(
                    format!("a result row or its schema exceeds the {limit}-byte IPC packet limit"), None,
                ));
            }
            rows /= 2;
        }
    }
}
