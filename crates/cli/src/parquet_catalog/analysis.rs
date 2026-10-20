use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use arrow::datatypes::SchemaRef;
use catalog::AnalyzedTableStatistics;
use common::statistics::analysis::{
    revision_from_identities, StatisticsAnalysisProvider, StatisticsAnalysisSnapshot,
};
use common::statistics::StatisticsInput;
use common::statistics::schema_fingerprint;
use error::{Result, VeinError};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use plan::CatalogProvider;

use super::ParquetCatalog;

#[async_trait]
impl StatisticsAnalysisProvider for ParquetCatalog {
    async fn open_analysis(
        &self,
        table_name: &str,
        batch_size: usize,
    ) -> Result<StatisticsAnalysisSnapshot> {
        if batch_size == 0 {
            return Err(VeinError::internal(
                "statistics scan batch size must be greater than zero",
            ));
        }
        let metadata = self.get_table(table_name).await?;
        let first_file = metadata.files.first().ok_or_else(|| {
            VeinError::not_found(format!("table '{table_name}' has no Parquet files to analyze"))
        })?;
        let first_file = PathBuf::from(first_file);
        let arrow_schema = tokio::task::spawn_blocking(move || {
            let file = File::open(&first_file).map_err(|error| {
                VeinError::io(format!("open ANALYZE source '{}'", first_file.display()), error)
            })?;
            let builder = ParquetRecordBatchReaderBuilder::try_new(file)
                .map_err(|error| VeinError::internal(format!("read ANALYZE source schema: {error}")))?;
            Ok::<SchemaRef, VeinError>(builder.schema().clone())
        })
            .await
            .map_err(|error| VeinError::internal(format!("ANALYZE schema worker failed: {error}")))??;

        let manifest = self.storage.read_manifest(table_name)?;
        let source_identities = if let Some(manifest) = &manifest {
            self.storage.source_identities(&manifest.files)?
        } else {
            self.storage.source_identities_from_paths(&metadata.files)?
        };
        let source_revision = manifest.as_ref().map_or_else(
            || revision_from_identities(&source_identities),
            |manifest| manifest.revision,
        );

        let files = metadata.files.clone();
        let expected_schema = Arc::clone(&arrow_schema);
        let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
        let worker = tokio::task::spawn_blocking(move || {
            if let Err(error) = send_parquet_batches(files, expected_schema, batch_size, sender.clone()) {
                let _ = sender.blocking_send(Err(error));
            }
        });
        let batches = Box::pin(async_stream::stream! {
            while let Some(batch) = receiver.recv().await {
                yield batch;
            }
            match worker.await {
                Ok(()) => {}
                Err(error) => yield Err(VeinError::internal(format!("ANALYZE batch worker failed: {error}"))),
            }
        });

        Ok(StatisticsAnalysisSnapshot {
            arrow_schema,
            logical_schema: metadata.schema.as_ref().clone(),
            source_revision,
            source_identities,
            expected_num_rows: metadata.statistics.as_ref().and_then(|stats| stats.num_rows),
            inputs: batches,
        })
    }

    async fn publish_analysis(
        &self,
        table_name: &str,
        statistics: &AnalyzedTableStatistics,
    ) -> Result<()> {
        let metadata = self.get_table(table_name).await?;
        let fingerprint = schema_fingerprint(&metadata.schema);
        let manifest = self.storage.read_manifest(table_name)?;
        let (revision, identities) = if let Some(manifest) = &manifest {
            (manifest.revision, self.storage.source_identities(&manifest.files)?)
        } else {
            let identities = self.storage.source_identities_from_paths(&metadata.files)?;
            (revision_from_identities(&identities), identities)
        };
        if statistics.source_revision != revision
            || statistics.schema_fingerprint != fingerprint
            || statistics.source_identities != identities
            || statistics.statistics.num_rows
            != metadata.statistics.as_ref().and_then(|stats| stats.num_rows)
        {
            return Err(VeinError::planning(
                format!("table '{table_name}' changed while ANALYZE was running; retry against the current snapshot"),
                None,
            ));
        }

        if manifest.is_some() {
            self.storage.publish_managed_statistics(table_name, statistics)
        } else {
            self.storage.publish_external_statistics(table_name, statistics)
        }
    }
}

fn send_parquet_batches(
    files: Vec<String>,
    expected_schema: SchemaRef,
    batch_size: usize,
    sender: tokio::sync::mpsc::Sender<Result<Box<dyn StatisticsInput + Send>>>,
) -> Result<()> {
    for file_path in files {
        let path = PathBuf::from(&file_path);
        let file = File::open(&path)
            .map_err(|error| VeinError::io(format!("open ANALYZE source '{}'", path.display()), error))?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|error| VeinError::internal(format!("read ANALYZE source '{}': {error}", path.display())))?;
        if builder.schema().as_ref() != expected_schema.as_ref() {
            return Err(VeinError::internal(format!(
                "Parquet schema changed while analyzing '{}'",
                path.display()
            )));
        }
        let reader = builder
            .with_batch_size(batch_size)
            .build()
            .map_err(|error| VeinError::internal(format!("create ANALYZE reader '{}': {error}", path.display())))?;
        for batch in reader {
            let batch = batch.map_err(|error| VeinError::internal(format!(
                "read ANALYZE batch from '{}': {error}",
                path.display()
            )))?;
            let view = exec::pipeline::arrow_adapter::from_record_batch(batch)?;
            sender
                .blocking_send(Ok(Box::new(view)))
                .map_err(|_| VeinError::internal("ANALYZE consumer closed its input stream"))?;
        }
    }
    Ok(())
}
