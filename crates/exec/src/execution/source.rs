//! A scan producing the engine's own representation.

use common::Result;
use vector::DataChunk;

use crate::operators::source::scan::ScanSource;
use crate::pipeline::bridge;
use crate::pipeline::{DataChunk as ArrowChunk, PipelineNode, Source};

/// Scans storage and emits chunks in the engine's representation.
///
/// The Parquet reader still produces Arrow chunks, so this wraps it and converts
/// each chunk as it arrives. That conversion is a **boundary cost, not a
/// permanent one**: when the reader produces Vein vectors directly this wrapper
/// disappears. Until then it is what lets a real query run end to end on the new
/// representation, and it keeps the Arrow path serving as the reference oracle
/// (`09-testing-and-correctness.md` §4, Rule 33).
///
/// Conversion failures are propagated, never swallowed into an empty result: a
/// column type the bridge does not cover must stop the query rather than
/// silently drop a column.
pub struct VectorScanSource {
    inner: ScanSource,
    converted_chunks: u64,
}

impl VectorScanSource {
    /// Wraps an Arrow scan.
    pub fn new(inner: ScanSource) -> Self {
        Self {
            inner,
            converted_chunks: 0,
        }
    }

    /// Chunks converted so far, for instrumentation.
    pub fn converted_chunks(&self) -> u64 {
        self.converted_chunks
    }

    /// The wrapped scan.
    pub fn inner(&self) -> &ScanSource {
        &self.inner
    }
}

#[async_trait::async_trait]
impl Source<DataChunk> for VectorScanSource {
    async fn execute(&mut self, next: &mut dyn PipelineNode<DataChunk>) -> Result<()> {
        let Self {
            inner,
            converted_chunks,
        } = self;
        let mut bridge = Bridge {
            next,
            converted_chunks,
        };
        inner.execute(&mut bridge).await
    }

    fn bytes_read(&self) -> u64 {
        self.inner.bytes_read()
    }
}

/// Converts each Arrow chunk and forwards it.
struct Bridge<'a> {
    next: &'a mut dyn PipelineNode<DataChunk>,
    converted_chunks: &'a mut u64,
}

impl PipelineNode<ArrowChunk> for Bridge<'_> {
    fn push(&mut self, chunk: ArrowChunk) -> Result<()> {
        let converted = bridge::arrow_chunk_to_vector(&chunk)?;
        *self.converted_chunks += 1;
        self.next.push(converted)
    }

    fn flush(&mut self) -> Result<()> {
        self.next.flush()
    }
}
