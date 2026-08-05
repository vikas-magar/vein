use arrow::array::{Float64Builder, Int32Builder, StringBuilder};
use arrow::buffer::Buffer;
use arrow::ipc::reader::StreamDecoder;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use futures::stream::Stream;
use std::pin::Pin;
use std::sync::Arc;

use error::{Result, VeinError};

/// A mask indicating which row indices in a ValueVector are valid.
#[derive(Debug, Clone)]
pub struct SelectionVector {
    /// Array of valid indices. If None, all rows [0..size) are valid.
    indices: Option<Vec<u32>>,
    /// Number of valid rows currently selected.
    size: usize,
}

impl SelectionVector {
    /// Creates a dense selection vector of the given size (all rows valid).
    pub fn new_dense(size: usize) -> Self {
        Self { indices: None, size }
    }
    
    /// Creates a sparse selection vector from the given array of valid indices.
    pub fn new_sparse(indices: Vec<u32>) -> Self {
        Self { size: indices.len(), indices: Some(indices) }
    }
    
    /// Returns the true physical index for a given logical index.
    #[inline(always)]
    pub fn get_index(&self, logical_idx: usize) -> usize {
        if let Some(indices) = &self.indices {
            indices[logical_idx] as usize
        } else {
            logical_idx
        }
    }
    
    /// Returns the number of valid rows currently selected.
    pub fn size(&self) -> usize {
        self.size
    }
}


/// A pinned, boxed, sendable stream of `Result<RecordBatch>`.
pub type SendableRecordBatchStream = Pin<Box<dyn Stream<Item = Result<RecordBatch>> + Send>>;

/// Encodes Arrow `RecordBatch`es into the Arrow IPC streaming format.
pub struct IpcStreamEncoder {
    writer: Option<StreamWriter<Vec<u8>>>,
}

impl Default for IpcStreamEncoder {
    fn default() -> Self {
        Self::new()
    }
}

impl IpcStreamEncoder {
    /// Create a new encoder.
    pub fn new() -> Self {
        Self { writer: None }
    }

    /// Encode a batch into the internal buffer. Returns a reference to the encoded bytes.
    pub fn encode(&mut self, batch: &RecordBatch) -> Result<&[u8]> {
        if self.writer.is_none() {
            let buf = Vec::new();
            let writer = StreamWriter::try_new(buf, &batch.schema())
                .map_err(|e| VeinError::internal(format!("init IPC writer: {e}")))?;
            self.writer = Some(writer);
        }
        if let Some(writer) = &mut self.writer {
            writer
                .write(batch)
                .map_err(|e| VeinError::internal(format!("IPC write: {e}")))?;
        }
        Ok(&[])
    }

    /// Finish the stream and return all encoded bytes.
    pub fn finalize(&mut self) -> Result<Vec<u8>> {
        if let Some(mut writer) = self.writer.take() {
            writer
                .finish()
                .map_err(|e| VeinError::internal(format!("IPC finish: {e}")))?;
            let buf = writer
                .into_inner()
                .map_err(|e| VeinError::internal(format!("IPC extract: {e}")))?;
            Ok(buf)
        } else {
            Ok(Vec::new())
        }
    }

    /// Write a batch (convenience wrapper around `encode`).
    pub fn write_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        self.encode(batch)?;
        Ok(())
    }

    /// Finish and consume the encoder (convenience wrapper).
    pub fn finish(self) -> Result<Vec<u8>> {
        let mut this = self;
        this.finalize()
    }
}

/// Decodes Arrow IPC streaming format bytes into `RecordBatch`es.
pub struct AsyncIpcDecoder {
    decoder: StreamDecoder,
}

impl Default for AsyncIpcDecoder {
    fn default() -> Self {
        Self::new()
    }
}

impl AsyncIpcDecoder {
    /// Create a new decoder.
    pub fn new() -> Self {
        Self {
            decoder: StreamDecoder::new(),
        }
    }

    /// Push a chunk of IPC bytes and get any fully-decoded batches.
    pub fn push_chunk(&mut self, chunk: bytes::Bytes) -> Result<Vec<RecordBatch>> {
        let mut arrow_buf = Buffer::from(chunk);
        let mut batches = Vec::new();
        while !arrow_buf.is_empty() {
            match self.decoder.decode(&mut arrow_buf) {
                Ok(Some(batch)) => batches.push(batch),
                Ok(None) => break,
                Err(e) => {
                    return Err(VeinError::internal(format!("IPC decode: {e}")));
                }
            }
        }
        Ok(batches)
    }

    /// Push a chunk and get the first decoded batch (if any).
    pub fn next_batch(&mut self, chunk: bytes::Bytes) -> Result<Option<RecordBatch>> {
        let batches = self.push_chunk(chunk)?;
        Ok(batches.into_iter().next())
    }
}

/// Convert an `mpsc::Receiver` into a `SendableRecordBatchStream`.
pub fn receiver_to_stream(
    mut rx: tokio::sync::mpsc::Receiver<Result<RecordBatch>>,
) -> SendableRecordBatchStream {
    Box::pin(async_stream::stream! {
        while let Some(item) = rx.recv().await {
            yield item;
        }
    })
}

/// Build a mock `RecordBatch` with `name`, `age`, and `score` columns.
pub fn make_mock_batch(rows: usize) -> RecordBatch {
    let mut names = StringBuilder::new();
    let mut ages = Int32Builder::new();
    let mut scores = Float64Builder::new();
    for i in 0..rows {
        names.append_value(format!("User{i}"));
        ages.append_value((i % 100) as i32);
        scores.append_value(i as f64 * 1.5);
    }
    RecordBatch::try_from_iter(vec![
        ("name", Arc::new(names.finish()) as _),
        ("age", Arc::new(ages.finish()) as _),
        ("score", Arc::new(scores.finish()) as _),
    ])
    .expect("mock batch should always build")
}

#[cfg(test)]
#[allow(missing_docs)]
mod tests {
    use super::*;

    #[test]
    fn test_ipc_roundtrip() {
        let batch = make_mock_batch(10);
        let mut encoder = IpcStreamEncoder::new();
        encoder.write_batch(&batch).unwrap();
        let bytes = encoder.finalize().unwrap();
        assert!(!bytes.is_empty());

        let mut decoder = AsyncIpcDecoder::new();
        let decoded = decoder.push_chunk(bytes::Bytes::from(bytes)).unwrap();
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].num_rows(), 10);
    }

    #[test]
    fn test_ipc_multiple_batches() {
        let mut encoder = IpcStreamEncoder::new();
        for _ in 0..3 {
            let batch = make_mock_batch(5);
            encoder.write_batch(&batch).unwrap();
        }
        let bytes = encoder.finalize().unwrap();

        let mut decoder = AsyncIpcDecoder::new();
        let decoded = decoder.push_chunk(bytes::Bytes::from(bytes)).unwrap();
        assert_eq!(decoded.len(), 3);
        assert_eq!(decoded.iter().map(|b| b.num_rows()).sum::<usize>(), 15);
    }
}
