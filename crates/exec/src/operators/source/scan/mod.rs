use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::pipeline::{DataChunk, PipelineNode, Source};
use catalog::MetadataStore;
use common::protocol::FileFormat;
use common::Result;
use error::VeinError;
use opendal::Operator;
use types::VeinSchemaRef;

/// Parquet source backed by a local-filesystem OpenDAL operator.
pub mod parquet;

pub use parquet::PARQUET_FOOTER;

/// One file of a scan, identified both by the path storage is asked for and by
/// the absolute path that keys the shared footer store.
#[derive(Debug, Clone)]
pub struct ScanFile {
    /// Path relative to the scan's OpenDAL operator root.
    pub relative: String,
    /// Canonical absolute path of the same file.
    ///
    /// Canonical because it keys the shared footer store: the catalog publishes
    /// a parsed footer under the key it resolved, and the scan has to derive the
    /// same key from the same file without resolving the path a second time.
    pub absolute: PathBuf,
}

/// Reads local Parquet files into the execution pipeline.
pub struct ScanSource {
    operator: Operator,
    files: Vec<ScanFile>,
    format: FileFormat,
    projected: VeinSchemaRef,
    metadata_store: MetadataStore,
    /// Bytes this scan has read from storage, shared with the Parquet reader so
    /// the runtime can report real I/O volume in execution metrics.
    bytes_read: Arc<AtomicU64>,
}

impl ScanSource {
    /// Creates a scan over the provided files.
    pub fn new(
        operator: Operator,
        files: Vec<ScanFile>,
        format: FileFormat,
        projected: VeinSchemaRef,
        metadata_store: MetadataStore,
    ) -> Self {
        Self {
            operator,
            files,
            format,
            projected,
            metadata_store,
            bytes_read: Arc::new(AtomicU64::new(0)),
        }
    }
}

#[async_trait::async_trait]
impl Source<DataChunk> for ScanSource {
    async fn execute(&mut self, next: &mut dyn PipelineNode<DataChunk>) -> Result<()> {
        match self.format {
            FileFormat::Parquet => {
                parquet::execute_parquet_scan(
                    &self.operator,
                    &self.files,
                    &self.projected,
                    &self.metadata_store,
                    &self.bytes_read,
                    next,
                )
                    .await?
            }
            _ => return Err(VeinError::internal("local executor supports Parquet files only")),
        }
        next.flush()
    }

    fn bytes_read(&self) -> u64 {
        self.bytes_read.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operators::sink::DiscardSink;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use ::parquet::arrow::ArrowWriter;

    /// Writes an `Int64` Parquet file of `rows` rows into `dir` and returns its path.
    fn write_parquet(dir: &std::path::Path, relative: &str, rows: i64) -> PathBuf {
        let path = dir.join(relative);
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let values = (0..rows).collect::<Vec<i64>>();
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(values))])
            .expect("valid batch");
        let file = std::fs::File::create(&path).expect("create Parquet file");
        let mut writer = ArrowWriter::try_new(file, schema, None).expect("create writer");
        writer.write(&batch).expect("write batch");
        writer.close().expect("close writer");
        path
    }

    /// Builds a scan over `dir`, with `files` given as `(relative, absolute)` pairs.
    fn scan_over(dir: &std::path::Path, files: &[(&str, PathBuf)]) -> ScanSource {
        let builder = opendal::services::Fs::default().root(&dir.to_string_lossy());
        let operator = opendal::Operator::new(builder).expect("local fs operator");
        let files = files
            .iter()
            .map(|(relative, absolute)| ScanFile {
                relative: (*relative).to_string(),
                absolute: std::fs::canonicalize(absolute).unwrap_or_else(|_| absolute.clone()),
            })
            .collect();
        ScanSource::new(
            operator,
            files,
            FileFormat::Parquet,
            Arc::new(types::VeinSchema::empty()),
            MetadataStore::new(),
        )
    }

    /// [`Source::bytes_read`] feeds the runtime's task metrics, so a scan that read
    /// storage must never report zero, and reading an extra file must report more
    /// bytes than reading one.
    #[tokio::test]
    async fn scan_reports_bytes_read_from_storage() {
        let dir = tempfile::tempdir().expect("temporary data root");
        let small = write_parquet(dir.path(), "part-0.parquet", 1_024);
        let large = write_parquet(dir.path(), "part-1.parquet", 8_192);

        let mut one = scan_over(dir.path(), &[("part-0.parquet", small.clone())]);
        one.execute(&mut DiscardSink::new())
            .await
            .expect("scan one file");
        let one_bytes = one.bytes_read();
        assert!(
            one_bytes > 0,
            "a completed scan must report the bytes it read, not the default zero"
        );

        let mut both = scan_over(
            dir.path(),
            &[("part-0.parquet", small), ("part-1.parquet", large)],
        );
        both.execute(&mut DiscardSink::new())
            .await
            .expect("scan two files");
        let both_bytes = both.bytes_read();
        assert!(
            both_bytes > one_bytes,
            "bytes read must accumulate across files: {both_bytes} <= {one_bytes}"
        );
    }
}
