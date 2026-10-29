//! Format-neutral `ANALYZE` orchestration over bounded Arrow batch streams.
//!
//! Storage adapters own snapshot identity, batch production, and publication.
//! The collector owns only synopsis algorithms and never retains input batches.

use std::collections::HashSet;
use std::pin::Pin;

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use catalog::{AnalyzedTableStatistics, StatisticsSourceIdentity, TableStatistics};
use futures::{Stream, StreamExt};
use types::VeinSchema;

use super::{
    ArrowScalarEncoder, ArrowValueEncoder, HyperLogLog, MergeableDistinctSketch,
    StatisticsCollector, StatisticsConfig, StatisticsInput, stable_hash,
};
use crate::{Result, VeinError};

/// A back-pressured stream of owned execution inputs from one stable snapshot.
pub type StatisticsInputStream = Pin<
    Box<dyn Stream<Item=Result<Box<dyn StatisticsInput + Send>>> + Send + 'static>,
>;

/// Source metadata and batches pinned to one table snapshot for analysis.
pub struct StatisticsAnalysisSnapshot {
    /// Physical Arrow schema used by the batch reader.
    pub arrow_schema: SchemaRef,
    /// Logical schema published to the catalog and optimizer.
    pub logical_schema: VeinSchema,
    /// Adapter-defined stable revision for the analyzed snapshot.
    pub source_revision: u64,
    /// Stable identities and versions for all contributing source objects.
    pub source_identities: Vec<StatisticsSourceIdentity>,
    /// Exact row count supplied by storage metadata, when available.
    pub expected_num_rows: Option<u64>,
    /// Incremental reference-backed inputs. Implementations should apply backpressure.
    pub inputs: StatisticsInputStream,
}

/// Catalog/storage boundary for generic table analysis.
///
/// The source decides how to read and identify data; the core analysis path is
/// independent of Parquet, file names, and catalog persistence details.
#[async_trait]
pub trait StatisticsAnalysisProvider: Send + Sync {
    /// Opens a batch stream and pins it to source identities and revision.
    async fn open_analysis(
        &self,
        table_name: &str,
        batch_size: usize,
    ) -> Result<StatisticsAnalysisSnapshot>;

    /// Publishes a complete summary if the same source snapshot is still current.
    async fn publish_analysis(
        &self,
        table_name: &str,
        statistics: &AnalyzedTableStatistics,
    ) -> Result<()>;
}

/// Analyze a table using Vein's standard HLL and Arrow scalar implementations.
pub async fn analyze_table(
    provider: &dyn StatisticsAnalysisProvider,
    table_name: &str,
    config: StatisticsConfig,
) -> Result<AnalyzedTableStatistics> {
    analyze_table_with_encoder::<HyperLogLog, ArrowScalarEncoder>(
        provider,
        table_name,
        config,
        ArrowScalarEncoder,
    )
        .await
}

/// Analyze a table with pluggable mergeable distinct sketches and scalar encoding.
pub async fn analyze_table_with_encoder<S, E>(
    provider: &dyn StatisticsAnalysisProvider,
    table_name: &str,
    config: StatisticsConfig,
    encoder: E,
) -> Result<AnalyzedTableStatistics>
where
    S: MergeableDistinctSketch,
    E: ArrowValueEncoder + 'static,
{
    if config.scan_batch_size == 0 {
        return Err(VeinError::internal(
            "statistics scan batch size must be greater than zero",
        ));
    }

    let started = crate::time::Instant::now();
    let mut snapshot = provider
        .open_analysis(table_name, config.scan_batch_size)
        .await?;
    let derived_schema = VeinSchema::try_from(snapshot.arrow_schema.as_ref())?;
    if derived_schema != snapshot.logical_schema {
        return Err(VeinError::internal(
            "analysis provider returned inconsistent Arrow and logical schemas",
        ));
    }
    canonicalize_identities(&mut snapshot.source_identities)?;

    let mut collector = StatisticsCollector::<S, E>::with_encoder(
        snapshot.arrow_schema.clone(),
        config,
        encoder,
    )?;
    let mut batch_count = 0_u64;
    while let Some(input) = snapshot.inputs.next().await {
        collector.update_input(input?.as_ref())?;
        batch_count = batch_count
            .checked_add(1)
            .ok_or_else(|| VeinError::internal("analysis batch count overflow"))?;
    }

    let statistics = collector.finish();
    let rows = statistics
        .num_rows
        .ok_or_else(|| VeinError::internal("analysis did not produce a row count"))?;
    if snapshot.expected_num_rows.is_some_and(|expected| expected != rows) {
        return Err(VeinError::internal(format!(
            "table '{table_name}' changed during analysis: storage reports {} rows, scan observed {rows}",
            snapshot.expected_num_rows.unwrap_or_default()
        )));
    }
    validate_statistics(&snapshot.logical_schema, &statistics, rows)?;

    let analyzed = AnalyzedTableStatistics {
        source_revision: snapshot.source_revision,
        schema_fingerprint: super::schema_fingerprint(&snapshot.logical_schema),
        source_identities: snapshot.source_identities,
        statistics,
    };
    provider.publish_analysis(table_name, &analyzed).await?;
    tracing::info!(
        target: "vein.audit",
        event = "statistics_analysis_completed",
        table = %table_name,
        revision = analyzed.source_revision,
        row_count = rows,
        batch_count,
        column_count = analyzed.statistics.column_statistics.len(),
        group_count = analyzed.statistics.column_groups.len(),
        elapsed_us = started.elapsed().as_micros() as u64,
        "statistics_analysis_completed"
    );
    Ok(analyzed)
}

/// Deterministically derive a revision for sources without a catalog generation.
///
/// The source identities are sorted and encoded with length prefixes, so
/// different identifier/version boundaries cannot produce the same byte stream.
pub fn revision_from_identities(identities: &[StatisticsSourceIdentity]) -> u64 {
    let mut ordered = identities.iter().collect::<Vec<_>>();
    ordered.sort_by(|left, right| left.identifier.cmp(&right.identifier));
    let mut bytes = Vec::new();
    for identity in ordered {
        for value in [&identity.identifier, &identity.version] {
            bytes.extend_from_slice(&(value.len() as u64).to_le_bytes());
            bytes.extend_from_slice(value.as_bytes());
        }
    }
    stable_hash(0xfd, &bytes)
}

fn canonicalize_identities(identities: &mut [StatisticsSourceIdentity]) -> Result<()> {
    identities.sort_by(|left, right| left.identifier.cmp(&right.identifier));
    if identities.iter().any(|identity| identity.identifier.is_empty())
        || identities.windows(2).any(|pair| pair[0].identifier == pair[1].identifier)
    {
        return Err(VeinError::internal(
            "analysis source identities must have unique, non-empty identifiers",
        ));
    }
    Ok(())
}

fn validate_statistics(schema: &VeinSchema, statistics: &TableStatistics, rows: u64) -> Result<()> {
    if statistics.num_rows != Some(rows)
        || statistics.column_statistics.len() != schema.fields.len()
    {
        return Err(VeinError::internal(
            "analysis synopsis does not cover the scanned table schema",
        ));
    }

    let mut names = HashSet::with_capacity(schema.fields.len());
    for field in &schema.fields {
        if !names.insert(field.name.as_str()) {
            return Err(VeinError::internal(format!(
                "analysis cannot publish ambiguous column name '{}'",
                field.name
            )));
        }
        let Some(column) = statistics.column_statistics.get(&field.name) else {
            return Err(VeinError::internal(format!(
                "analysis is missing statistics for column '{}'",
                field.name
            )));
        };
        if !column.is_consistent_with_rows(rows) {
            return Err(VeinError::internal(format!(
                "analysis produced inconsistent statistics for column '{}'",
                field.name
            )));
        }
    }

    let mut groups = HashSet::with_capacity(statistics.column_groups.len());
    for group in &statistics.column_groups {
        let positions = group
            .columns
            .iter()
            .map(|name| schema.fields.iter().position(|field| field.name == *name))
            .collect::<Option<Vec<_>>>();
        if group.columns.len() < 2
            || group.columns.iter().collect::<HashSet<_>>().len() != group.columns.len()
            || positions.as_ref().is_none_or(|positions| {
            positions.windows(2).any(|pair| pair[0] >= pair[1])
        })
            || group.distinct_count.is_none_or(|count| count > rows)
            || group.distinct_sketch.is_none()
            || group.distinct_count_relative_error.is_none_or(|error| {
            !error.is_finite() || !(0.0..=1.0).contains(&error)
        })
            || !groups.insert(group.columns.iter().map(String::as_str).collect::<Vec<_>>())
        {
            return Err(VeinError::internal(
                "analysis produced an invalid multi-column synopsis",
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use catalog::StatisticsSourceIdentity;
    use futures::stream;

    use super::*;

    struct MemoryProvider {
        schema: SchemaRef,
        logical_schema: VeinSchema,
        batches: Vec<RecordBatch>,
        expected_num_rows: Option<u64>,
        published: Mutex<Option<AnalyzedTableStatistics>>,
        requested_batch_size: Mutex<Option<usize>>,
    }

    #[async_trait]
    impl StatisticsAnalysisProvider for MemoryProvider {
        async fn open_analysis(
            &self,
            _table_name: &str,
            batch_size: usize,
        ) -> Result<StatisticsAnalysisSnapshot> {
            *self.requested_batch_size.lock().unwrap() = Some(batch_size);
            let inputs = self.batches.clone().into_iter().map(|batch| {
                Ok(Box::new(batch) as Box<dyn StatisticsInput + Send>)
            });
            Ok(StatisticsAnalysisSnapshot {
                arrow_schema: Arc::clone(&self.schema),
                logical_schema: self.logical_schema.clone(),
                source_revision: 7,
                source_identities: vec![StatisticsSourceIdentity {
                    identifier: "memory://table".to_owned(),
                    version: "v1".to_owned(),
                }],
                expected_num_rows: self.expected_num_rows,
                inputs: Box::pin(stream::iter(inputs)),
            })
        }

        async fn publish_analysis(
            &self,
            _table_name: &str,
            statistics: &AnalyzedTableStatistics,
        ) -> Result<()> {
            *self.published.lock().unwrap() = Some(statistics.clone());
            Ok(())
        }
    }

    fn provider(expected_num_rows: Option<u64>) -> MemoryProvider {
        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Int64, true),
            Field::new("group_key", DataType::Int64, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![Some(1), Some(1), Some(2), None])),
                Arc::new(Int64Array::from(vec![Some(5), Some(5), Some(6), Some(5)])),
            ],
        )
            .unwrap();
        MemoryProvider {
            logical_schema: VeinSchema::try_from(schema.as_ref()).unwrap(),
            schema,
            batches: vec![batch],
            expected_num_rows,
            published: Mutex::new(None),
            requested_batch_size: Mutex::new(None),
        }
    }

    #[tokio::test]
    async fn generic_analysis_streams_batches_and_publishes_requested_joint_stats() {
        let provider = provider(Some(4));
        let config = StatisticsConfig {
            scan_batch_size: 3,
            multi_column_sets: vec![vec!["group_key".to_owned(), "key".to_owned()]],
            ..StatisticsConfig::default()
        };
        let result = analyze_table(&provider, "memory_table", config)
            .await
            .expect("analyze memory source");

        assert_eq!(*provider.requested_batch_size.lock().unwrap(), Some(3));
        assert_eq!(result.statistics.num_rows, Some(4));
        assert_eq!(result.statistics.column_groups[0].columns, vec!["key", "group_key"]);
        assert_eq!(result.statistics.column_groups[0].distinct_count, Some(3));
        assert_eq!(provider.published.lock().unwrap().as_ref(), Some(&result));
    }

    #[tokio::test]
    async fn generic_analysis_does_not_publish_when_source_row_count_changed() {
        let provider = provider(Some(99));
        let error = analyze_table(&provider, "memory_table", StatisticsConfig::default())
            .await
            .expect_err("mismatched snapshot must fail");
        assert!(error.to_string().contains("changed during analysis"));
        assert!(provider.published.lock().unwrap().is_none());
    }
}
