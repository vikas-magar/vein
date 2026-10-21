use std::sync::Arc;

use crate::scan::{analysis_inputs, load_footer_from_storage};
use async_trait::async_trait;
use catalog::AnalyzedTableStatistics;
use common::statistics::analysis::{
    StatisticsAnalysisProvider, StatisticsAnalysisSnapshot, revision_from_identities,
};
use common::statistics::schema_fingerprint;
use error::VeinError;
use plan::CatalogProvider;

use super::discovery::read_manifest;
use super::provider::ParquetCatalog;
use super::snapshot::source_identities;

#[async_trait]
impl StatisticsAnalysisProvider for ParquetCatalog {
    async fn open_analysis(
        &self,
        table_name: &str,
        batch_size: usize,
    ) -> common::Result<StatisticsAnalysisSnapshot> {
        if batch_size == 0 {
            return Err(VeinError::internal(
                "statistics scan batch size must be greater than zero",
            ));
        }
        let metadata = self.get_table(table_name).await?;
        let sources = metadata.storage_metadata.clone();
        let first = sources
            .first()
            .ok_or_else(|| VeinError::not_found(format!("table '{table_name}' has no files to analyze")))?;
        let first_path = first.identity.path().clone();
        let (arrow_schema, _, _) = load_footer_from_storage(
            &self.storage,
            &first_path,
            first,
            &self.metadata_store,
        )
            .await?;
        let manifest = read_manifest(self.storage.as_ref(), table_name).await?;
        let managed = manifest.is_some();
        let identities = source_identities(&metadata, &self.location, managed);
        let source_revision = manifest
            .as_ref()
            .map_or_else(|| revision_from_identities(&identities), |manifest| manifest.revision);
        let expected_num_rows = metadata.statistics.as_ref().and_then(|stats| stats.num_rows);
        let files = sources
            .into_iter()
            .map(|source| (source.identity.path().clone(), source))
            .collect();
        let inputs = analysis_inputs(
            Arc::clone(&self.storage),
            files,
            self.metadata_store.clone(),
            batch_size,
        );
        Ok(StatisticsAnalysisSnapshot {
            arrow_schema,
            logical_schema: metadata.schema.as_ref().clone(),
            source_revision,
            source_identities: identities,
            expected_num_rows,
            inputs,
        })
    }

    async fn publish_analysis(
        &self,
        table_name: &str,
        statistics: &AnalyzedTableStatistics,
    ) -> common::Result<()> {
        let metadata = self.get_table(table_name).await?;
        let manifest = read_manifest(self.storage.as_ref(), table_name).await?;
        let managed_revision = manifest.as_ref().map(|manifest| manifest.revision);
        let identities = source_identities(&metadata, &self.location, managed_revision.is_some());
        let revision = managed_revision.unwrap_or_else(|| revision_from_identities(&identities));
        let row_count = metadata.statistics.as_ref().and_then(|stats| stats.num_rows);
        if statistics.source_revision != revision
            || statistics.schema_fingerprint != schema_fingerprint(&metadata.schema)
            || statistics.source_identities != identities
            || statistics.statistics.num_rows != row_count
        {
            return Err(VeinError::planning(
                format!("table '{table_name}' changed while ANALYZE was running; retry against the current snapshot"),
                None,
            ));
        }
        let publisher = self.publisher.clone().ok_or_else(|| {
            VeinError::unsupported("ANALYZE publication requires a configured Parquet writer", None)
        })?;
        let table = table_name.to_owned();
        let statistics = statistics.clone();
        tokio::task::spawn_blocking(move || match managed_revision {
            Some(_) => publisher.publish_managed(&table, &statistics),
            None => publisher.publish_external(&table, &statistics),
        })
            .await
            .map_err(|error| VeinError::internal(format!("ANALYZE publisher failed: {error}")))?
    }
}
