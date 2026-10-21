use std::fs::{self, File, OpenOptions};
use std::io::BufWriter;
use std::path::PathBuf;
use std::sync::Arc;

use crate::catalog::manifest::TableManifest;
use arrow::datatypes::SchemaRef;
use common::statistics::StatisticsCollector;
use common::{Result, VeinError};
use exec::operators::sink::{WriteReceipt, WriteTransaction};
use exec::pipeline::{DataChunkView, MaterializationReport};
use parquet::arrow::ArrowWriter;

use super::ParquetWriteStorage;
use super::filesystem;

pub(super) struct ParquetWriteTransaction {
    pub(super) storage: ParquetWriteStorage,
    pub(super) table: String,
    pub(super) mode: String,
    pub(super) base_manifest: Option<TableManifest>,
    pub(super) staging_dir: PathBuf,
    pub(super) generation: String,
    pub(super) writer: Option<ArrowWriter<BufWriter<File>>>,
    pub(super) schema: SchemaRef,
    pub(super) rows: u64,
    pub(super) schema_fingerprint: String,
    pub(super) statistics: Option<StatisticsCollector>,
}

impl ParquetWriteTransaction {
    pub(super) fn new(
        storage: ParquetWriteStorage, table: String, mode: String,
        base_manifest: Option<TableManifest>, staging_dir: PathBuf, generation: String,
        schema: SchemaRef, schema_fingerprint: String, statistics: Option<StatisticsCollector>,
    ) -> Self {
        Self {
            storage,
            table,
            mode,
            base_manifest,
            staging_dir,
            generation,
            writer: None,
            schema,
            rows: 0,
            schema_fingerprint,
            statistics,
        }
    }

    pub(super) fn ensure_writer(&mut self) -> Result<&mut ArrowWriter<BufWriter<File>>> {
        if self.writer.is_none() {
            let path = self.staging_dir.join("part.parquet");
            let file = OpenOptions::new().write(true).create_new(true).open(&path)
                .map_err(|error| VeinError::io(format!("create staged Parquet file '{}'", path.display()), error))?;
            self.writer = Some(ArrowWriter::try_new(BufWriter::new(file), self.schema.clone(), None)
                .map_err(|error| VeinError::internal(format!("initialize Parquet writer: {error}")))?);
        }
        self.writer.as_mut().ok_or_else(|| VeinError::internal("Parquet writer was not initialized"))
    }

    pub(super) fn validate_append_schema(&self, manifest: Option<&TableManifest>) -> Result<()> {
        let existing = manifest.and_then(|manifest| manifest.files.first())
            .ok_or_else(|| VeinError::internal("append target has no committed data files"))?;
        let existing_path = filesystem::resolve_manifest_file(&self.storage.root, existing)?;
        let file = File::open(&existing_path)
            .map_err(|error| VeinError::io(format!("open existing table artifact '{existing}'"), error))?;
        let existing_schema = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|error| VeinError::internal(format!("read existing table schema: {error}")))?
            .schema().clone();
        if existing_schema.as_ref() != self.schema.as_ref() {
            return Err(VeinError::planning("append schema must exactly match the existing table schema", None));
        }
        Ok(())
    }
}

impl WriteTransaction for ParquetWriteTransaction {
    fn write_view(&mut self, view: &DataChunkView) -> Result<MaterializationReport> {
        view.validate_schema(&self.schema)?;
        if let Some(statistics) = &mut self.statistics { statistics.update_input(view)?; }
        let (batch, report) = exec::pipeline::arrow_adapter::to_record_batch_with_report(view, Arc::clone(&self.schema))?;
        let rows = self.rows.checked_add(view.num_rows() as u64)
            .ok_or_else(|| VeinError::internal("WRITE row count overflow"))?;
        self.ensure_writer()?.write(&batch)
            .map_err(|error| VeinError::internal(format!("write Parquet batch: {error}")))?;
        self.rows = rows;
        Ok(report)
    }

    fn commit(self: Box<Self>) -> Result<WriteReceipt> {
        super::commit::commit(self)
    }
}

impl Drop for ParquetWriteTransaction {
    fn drop(&mut self) { let _ = fs::remove_dir_all(&self.staging_dir); }
}
