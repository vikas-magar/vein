use std::path::PathBuf;
use std::sync::Arc;

use crate::scan::ParquetScanProvider;
use arrow::array::{Array, BooleanArray, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use catalog::DataFileFormatId;
use catalog::MetadataStore;
use common::Result;
use common::statistics::RuntimeStatisticsStore;
use error::VeinError;
use exec::expression::eval::ExpressionKernelRegistry;
use exec::operators::source::scan::{
    ScanFile, ScanProviderRegistry, ScanSource, ScanSourceConfig,
};
use exec::pipeline::{DataChunkView, PipelineNode};
use exec::runtime::filter::{RuntimeFilter, RuntimeFilterSet};
use functions::FunctionRegistry;
use parquet::arrow::ArrowWriter;
use storage::{
    ReadStorageFactory, StorageLocation, opendal::OpenDalReadStorageFactory,
};

pub(super) fn write_parquet(dir: &std::path::Path, relative: &str, rows: i64) -> PathBuf {
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

pub(super) fn write_string_parquet(dir: &std::path::Path, relative: &str) -> PathBuf {
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

pub(super) fn write_multi_row_group_parquet(dir: &std::path::Path, relative: &str) -> PathBuf {
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
pub(super) fn scan_over(dir: &std::path::Path, files: &[(&str, PathBuf)]) -> ScanSource {
    scan_with_config(scan_config(dir, files))
}

pub(super) fn scan_with_config(config: ScanSourceConfig) -> ScanSource {
    let mut providers = ScanProviderRegistry::empty();
    providers
        .register(ParquetScanProvider::default())
        .expect("register Parquet scan provider");
    let provider = providers
        .resolve(&DataFileFormatId::new("parquet"))
        .expect("resolve registered Parquet scan provider");
    ScanSource::new(config, provider)
}

pub(super) fn scan_config(
    dir: &std::path::Path,
    files: &[(&str, PathBuf)],
) -> ScanSourceConfig {
    let storage = OpenDalReadStorageFactory
        .open(&StorageLocation::local(dir.to_path_buf()))
        .expect("local filesystem storage");
    let files = files
        .iter()
        .map(|(relative, absolute)| ScanFile {
            relative: (*relative).to_string(),
            absolute: std::fs::canonicalize(absolute).unwrap_or_else(|_| absolute.clone()),
            storage_metadata: None,
            scan_unit_range: None,
        })
        .collect();
    ScanSourceConfig {
        storage,
        files,
        projected: Arc::new(types::VeinSchema::empty()),
        column_encoding_hints: Vec::new(),
        pushdown: None,
        runtime_filters: RuntimeFilterSet::empty(),
        metadata_store: MetadataStore::new(),
        registry: Arc::new(FunctionRegistry::new()),
        expression_kernels: Arc::new(ExpressionKernelRegistry::with_builtins()),
        runtime_statistics: RuntimeStatisticsStore::default(),
        statistics_scan: None,
    }
}

#[derive(Debug)]
pub(super) struct KeepIds {
    pub(super) column: types::ColumnId,
    pub(super) ids: Arc<[i64]>,
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
        view: &DataChunkView,
        column_positions: &[usize],
    ) -> Result<BooleanArray> {
        let column = *column_positions.first().ok_or_else(|| {
            VeinError::internal("test runtime filter has no bound key column")
        })?;
        let mut selection = arrow::array::builder::BooleanBuilder::with_capacity(view.num_rows());
        for row in 0..view.num_rows() {
            let keep = match view.resolve(column, row)? {
                Some((array, index)) => {
                    let values = array
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .ok_or_else(|| VeinError::internal("test runtime filter requires Int64"))?;
                    values.is_valid(index) && self.ids.contains(&values.value(index))
                }
                None => false,
            };
            selection.append_value(keep);
        }
        Ok(selection.finish())
    }
}

#[derive(Default)]
pub(super) struct Int64Collector(pub(super) Vec<i64>);

#[async_trait::async_trait]
impl PipelineNode for Int64Collector {
    async fn push(&mut self, chunk: DataChunkView) -> Result<()> {
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

    async fn flush(&mut self) -> Result<()> {
        Ok(())
    }
}

#[derive(Default)]
pub(super) struct ArrayTypeSink(pub(super) Option<DataType>);

#[async_trait::async_trait]
impl PipelineNode for ArrayTypeSink {
    async fn push(&mut self, batch: DataChunkView) -> Result<()> {
        self.0 = Some(batch.materialize_column(0)?.data_type().clone());
        Ok(())
    }

    async fn flush(&mut self) -> Result<()> {
        Ok(())
    }
}
