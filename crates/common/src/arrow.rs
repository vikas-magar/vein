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
    /// Sparse physical indices. When `None`, the selection is the dense
    /// physical range `offset..offset + size`.
    indices: Option<Vec<u32>>,
    /// Physical row at which a dense selection starts.
    offset: usize,
    /// Number of valid rows currently selected.
    size: usize,
}

impl SelectionVector {
    /// Creates a dense selection vector of the given size (all rows valid).
    pub fn new_dense(size: usize) -> Self {
        Self {
            indices: None,
            offset: 0,
            size,
        }
    }

    /// Creates a sparse selection vector from the given array of valid indices.
    pub fn new_sparse(indices: Vec<u32>) -> Self {
        Self {
            size: indices.len(),
            indices: Some(indices),
            offset: 0,
        }
    }

    /// Returns the true physical index for a given logical index.
    #[inline(always)]
    pub fn get_index(&self, logical_idx: usize) -> usize {
        match &self.indices {
            Some(indices) => indices[logical_idx] as usize,
            None => self.offset + logical_idx,
        }
    }

    /// Returns the number of valid rows currently selected.
    pub fn size(&self) -> usize {
        self.size
    }

    /// Returns the selected rows as one contiguous physical range when possible.
    pub fn contiguous_range(&self) -> Option<std::ops::Range<usize>> {
        if self.indices.is_none() {
            return Some(self.offset..self.offset.checked_add(self.size)?);
        }
        let first = *self.indices.as_ref()?.first()? as usize;
        let end = first.checked_add(self.size)?;
        self.indices
            .as_ref()?
            .iter()
            .enumerate()
            .all(|(offset, &index)| index as usize == first + offset)
            .then_some(first..end)
    }

    /// Whether this selection vector has zero valid rows.
    pub fn is_empty(&self) -> bool {
        self.size == 0
    }

    /// Returns a [`SelectionVector`] of the currently selected rows that satisfy
    /// `predicate`.
    ///
    /// Preserves prior filter decisions: only previously surviving rows are evaluated.
    /// A sparse index list is materialized only once a row is actually rejected, so a
    /// dense selection whose rows all survive stays dense and allocation-free.
    pub fn filter(&self, mut predicate: impl FnMut(usize) -> bool) -> Self {
        let mut surviving: Option<Vec<u32>> = None;
        for i in 0..self.size {
            let row_idx = self.get_index(i);
            if predicate(row_idx) {
                if let Some(surviving) = &mut surviving {
                    surviving.push(row_idx as u32);
                }
            } else if surviving.is_none() {
                let mut kept = Vec::with_capacity(i + 1);
                kept.extend((0..i).map(|j| self.get_index(j) as u32));
                surviving = Some(kept);
            }
        }
        match surviving {
            None => self.clone(),
            Some(surviving) => Self::new_sparse(surviving),
        }
    }

    /// Returns a new [`SelectionVector`] sliced by `offset` and taking up to `limit` rows.
    pub fn slice(&self, offset: usize, limit: usize) -> Self {
        if offset >= self.size || limit == 0 {
            return Self::new_dense(0);
        }
        let count = limit.min(self.size - offset);
        match &self.indices {
            // Contiguous selections stay contiguous: just move the window.
            None => Self {
                indices: None,
                offset: self.offset + offset,
                size: count,
            },
            Some(_) => {
                let mut sliced = Vec::with_capacity(count);
                for i in offset..(offset + count) {
                    sliced.push(self.get_index(i) as u32);
                }
                Self::new_sparse(sliced)
            }
        }
    }
}

/// A pinned, boxed, sendable stream of `Result<RecordBatch>`.
pub type SendableRecordBatchStream = Pin<Box<dyn Stream<Item=Result<RecordBatch>> + Send>>;

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

    #[test]
    fn test_selection_vector_dense() {
        let sv = SelectionVector::new_dense(5);
        assert_eq!(sv.size(), 5);
        assert_eq!(sv.get_index(0), 0);
        assert_eq!(sv.get_index(3), 3);
    }

    #[test]
    fn selection_reports_dense_contiguous_ranges() {
        assert_eq!(SelectionVector::new_dense(5).contiguous_range(), Some(0..5));
        assert_eq!(
            SelectionVector::new_sparse(vec![4, 5, 6]).contiguous_range(),
            Some(4..7)
        );
        assert_eq!(
            SelectionVector::new_sparse(vec![4, 6]).contiguous_range(),
            None
        );
    }

    #[test]
    fn test_selection_vector_sparse() {
        let sv = SelectionVector::new_sparse(vec![2, 4, 7]);
        assert_eq!(sv.size(), 3);
        assert_eq!(sv.get_index(0), 2);
        assert_eq!(sv.get_index(1), 4);
        assert_eq!(sv.get_index(2), 7);
    }

    #[test]
    fn test_selection_vector_filter() {
        let sv = SelectionVector::new_dense(10);
        // Keep even indices
        let evens = sv.filter(|idx| idx % 2 == 0);
        assert_eq!(evens.size(), 5);
        assert_eq!(evens.get_index(0), 0);
        assert_eq!(evens.get_index(1), 2);
        assert_eq!(evens.get_index(2), 4);
        assert_eq!(evens.get_index(3), 6);
        assert_eq!(evens.get_index(4), 8);

        // Chain another filter: keep > 3
        let chained = evens.filter(|idx| idx > 3);
        assert_eq!(chained.size(), 3);
        assert_eq!(chained.get_index(0), 4);
        assert_eq!(chained.get_index(1), 6);
        assert_eq!(chained.get_index(2), 8);
    }

    #[test]
    fn test_selection_vector_slice() {
        let sv = SelectionVector::new_dense(10);
        let sliced = sv.slice(3, 4);
        assert_eq!(sliced.size(), 4);
        assert_eq!(sliced.get_index(0), 3);
        assert_eq!(sliced.get_index(1), 4);
        assert_eq!(sliced.get_index(2), 5);
        assert_eq!(sliced.get_index(3), 6);
        // A dense selection stays a contiguous range; no index list is materialized.
        assert_eq!(sliced.contiguous_range(), Some(3..7));

        // Slice beyond bounds
        let empty = sv.slice(15, 5);
        assert!(empty.is_empty());
        assert_eq!(empty.contiguous_range(), Some(0..0));
    }

    #[test]
    fn dense_filter_that_keeps_every_row_stays_contiguous() {
        let sv = SelectionVector::new_dense(5);
        assert_eq!(sv.filter(|_| true).contiguous_range(), Some(0..5));

        // A window that survives filtering intact keeps its physical offset.
        let windowed = sv.slice(2, 3);
        assert_eq!(windowed.filter(|_| true).contiguous_range(), Some(2..5));

        // Dropping a row still falls back to a sparse index list.
        assert_eq!(sv.filter(|row| row != 2).contiguous_range(), None);
    }
}
