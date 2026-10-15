use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::pipeline::{DataChunkView, PipelineNode, Source};
use crate::runtime_filter::RuntimeFilterSet;
use crate::udf::FunctionRegistry;
use catalog::MetadataStore;
use common::protocol::FileFormat;
use common::protocol::ScanPredicate;
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
    /// Optional range of reader-defined data units assigned to this file.
    /// Parquet interprets these ordinals as row groups.
    pub scan_unit_range: Option<std::ops::Range<usize>>,
}

/// Reads local Parquet files into the execution pipeline.
pub struct ScanSource {
    operator: Operator,
    files: Vec<ScanFile>,
    format: FileFormat,
    projected: VeinSchemaRef,
    /// Projected columns whose Parquet dictionary encoding should be retained.
    dictionary_columns: Vec<u32>,
    /// Predicate the reader evaluates before materializing rows.
    pushdown: Option<Arc<ScanPredicate>>,
    /// Query-local necessary-condition filters, evaluated before payload decode
    /// by source adapters that advertise support.
    runtime_filters: RuntimeFilterSet,
    metadata_store: MetadataStore,
    /// Function registry the compiled predicate program runs against.
    registry: Arc<FunctionRegistry>,
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
        dictionary_columns: Vec<u32>,
        pushdown: Option<Arc<ScanPredicate>>,
        metadata_store: MetadataStore,
        registry: Arc<FunctionRegistry>,
    ) -> Self {
        Self::new_with_runtime_filters(
            operator,
            files,
            format,
            projected,
            dictionary_columns,
            pushdown,
            metadata_store,
            registry,
            RuntimeFilterSet::empty(),
        )
    }

    /// Creates a scan that may apply query-local runtime filters before reading
    /// the projected payload columns.
    pub fn new_with_runtime_filters(
        operator: Operator,
        files: Vec<ScanFile>,
        format: FileFormat,
        projected: VeinSchemaRef,
        dictionary_columns: Vec<u32>,
        pushdown: Option<Arc<ScanPredicate>>,
        metadata_store: MetadataStore,
        registry: Arc<FunctionRegistry>,
        runtime_filters: RuntimeFilterSet,
    ) -> Self {
        Self {
            operator,
            files,
            format,
            projected,
            dictionary_columns,
            pushdown,
            runtime_filters,
            metadata_store,
            registry,
            bytes_read: Arc::new(AtomicU64::new(0)),
        }
    }
}

#[async_trait::async_trait]
impl Source<DataChunkView> for ScanSource {
    async fn execute(&mut self, next: &mut dyn PipelineNode<DataChunkView>) -> Result<()> {
        match self.format {
            FileFormat::Parquet => {
                parquet::execute_parquet_scan(
                    &self.operator,
                    &self.files,
                    &self.projected,
                    &self.dictionary_columns,
                    self.pushdown.as_deref(),
                    &self.metadata_store,
                    &self.registry,
                    &self.bytes_read,
                    &self.runtime_filters,
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
    use crate::runtime_filter::RuntimeFilter;
    use arrow::array::{Array, BooleanArray, Int64Array, StringArray};
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

    fn write_string_parquet(dir: &std::path::Path, relative: &str) -> PathBuf {
        let path = dir.join(relative);
        let schema = Arc::new(Schema::new(vec![Field::new("status", DataType::Utf8, true)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(StringArray::from(vec![Some("O"), Some("F"), None]))],
        )
            .expect("valid string batch");
        let file = std::fs::File::create(&path).expect("create Parquet file");
        let mut writer = ArrowWriter::try_new(file, schema, None).expect("create writer");
        writer.write(&batch).expect("write batch");
        writer.close().expect("close writer");
        path
    }

    fn write_multi_row_group_parquet(dir: &std::path::Path, relative: &str) -> PathBuf {
        let path = dir.join(relative);
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let file = std::fs::File::create(&path).expect("create Parquet file");
        let mut writer = ArrowWriter::try_new(file, schema.clone(), None).expect("create writer");
        for start in [0_i64, 2, 4] {
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(Int64Array::from(vec![start, start + 1]))],
            )
            .expect("valid row-group batch");
            writer.write(&batch).expect("write row group");
            writer.flush().expect("finish row group");
        }
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
                scan_unit_range: None,
            })
            .collect();
        ScanSource::new(
            operator,
            files,
            FileFormat::Parquet,
            Arc::new(types::VeinSchema::empty()),
            Vec::new(),
            None,
            MetadataStore::new(),
            Arc::new(FunctionRegistry::new()),
        )
    }

    #[derive(Debug)]
    struct KeepIds {
        column: types::ColumnId,
        ids: Arc<[i64]>,
    }

    impl RuntimeFilter for KeepIds {
        fn name(&self) -> &'static str {
            "test_keep_ids"
        }

        fn required_columns(&self) -> &[types::ColumnId] {
            std::slice::from_ref(&self.column)
        }

        fn memory_bytes(&self) -> usize {
            self.ids.len() * std::mem::size_of::<i64>()
        }

        fn evaluate(
            &self,
            batch: &RecordBatch,
            column_positions: &[usize],
        ) -> Result<BooleanArray> {
            let values = batch
                .column(*column_positions.first().ok_or_else(|| {
                    VeinError::internal("test runtime filter has no bound key column")
                })?)
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| VeinError::internal("test runtime filter requires Int64"))?;
            let mut selection = arrow::array::builder::BooleanBuilder::with_capacity(batch.num_rows());
            for row in 0..batch.num_rows() {
                selection.append_value(values.is_valid(row) && self.ids.contains(&values.value(row)));
            }
            Ok(selection.finish())
        }
    }

    #[derive(Default)]
    struct Int64Collector(Vec<i64>);

    impl PipelineNode<DataChunkView> for Int64Collector {
        fn push(&mut self, chunk: DataChunkView) -> Result<()> {
            let column = chunk.materialize_column(0)?;
            let values = column
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| VeinError::internal("expected Int64 scan output"))?;
            self.0.extend(
                (0..values.len())
                    .filter(|row| values.is_valid(*row))
                    .map(|row| values.value(row)),
            );
            Ok(())
        }

        fn flush(&mut self) -> Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn parquet_scan_applies_runtime_filter_before_emitting_projected_rows() {
        let dir = tempfile::tempdir().expect("temporary data root");
        let path = write_parquet(dir.path(), "part-0.parquet", 64);
        let mut scan = scan_over(dir.path(), &[("part-0.parquet", path)]);
        scan.projected = Arc::new(types::VeinSchema::new(vec![Arc::new(
            types::ResolvedColumn::new(
                types::ColumnId(9),
                "id",
                None,
                types::VeinDataType::Int64,
            ),
        )]));
        scan.runtime_filters = RuntimeFilterSet::new(vec![Arc::new(KeepIds {
            column: types::ColumnId(9),
            ids: Arc::from([3, 7, 42]),
        })]);
        let mut sink = Int64Collector::default();

        scan.execute(&mut sink)
            .await
            .expect("apply runtime filter during Parquet scan");

        assert_eq!(sink.0, vec![3, 7, 42].into_iter().filter(|id| *id < 64).collect::<Vec<_>>());
        assert!(scan.runtime_filters.rows_seen() >= 64);
        assert!(scan.runtime_filters.rows_kept() < scan.runtime_filters.rows_seen());
    }

    #[tokio::test]
    async fn parquet_scan_reads_only_its_assigned_row_group_range() {
        let dir = tempfile::tempdir().expect("temporary data root");
        let path = write_multi_row_group_parquet(dir.path(), "part-0.parquet");
        let mut scan = scan_over(dir.path(), &[("part-0.parquet", path)]);
        scan.projected = Arc::new(types::VeinSchema::new(vec![Arc::new(
            types::ResolvedColumn::new(
                types::ColumnId(0),
                "id",
                None,
                types::VeinDataType::Int64,
            ),
        )]));
        scan.files[0].scan_unit_range = Some(1..2);
        let mut sink = Int64Collector::default();

        scan.execute(&mut sink)
            .await
            .expect("scan assigned row-group range");

        assert_eq!(sink.0, vec![2, 3]);
    }

    #[derive(Default)]
    struct ArrayTypeSink(Option<DataType>);

    impl PipelineNode<DataChunkView> for ArrayTypeSink {
        fn push(&mut self, batch: DataChunkView) -> Result<()> {
            self.0 = Some(batch.materialize_column(0)?.data_type().clone());
            Ok(())
        }

        fn flush(&mut self) -> Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn scan_preserves_dictionary_encoding_when_requested() {
        let dir = tempfile::tempdir().expect("temporary data root");
        let path = write_string_parquet(dir.path(), "part-0.parquet");
        let mut scan = scan_over(dir.path(), &[("part-0.parquet", path)]);
        scan.projected = Arc::new(types::VeinSchema::new(vec![Arc::new(
            types::ResolvedColumn::new(
                types::ColumnId(0),
                "status",
                None,
                types::VeinDataType::Utf8,
            ),
        )]));
        scan.dictionary_columns = vec![0];
        let mut sink = ArrayTypeSink::default();

        scan.execute(&mut sink).await.expect("scan dictionary column");

        assert_eq!(
            sink.0,
            Some(DataType::Dictionary(
                Box::new(DataType::Int32),
                Box::new(DataType::Utf8)
            ))
        );
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
