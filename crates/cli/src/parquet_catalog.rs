//! Read-only Parquet catalog with deterministic file discovery and schema checks.

use std::fs::{self, File};
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use crate::parquet_storage::{ParquetStorage, resolve_manifest_file};
use async_trait::async_trait;
use catalog::{
    AnalyzedTableStatistics, MetadataKey, MetadataStore, StatisticsSourceIdentity,
};
use common::statistics::analysis::{
    revision_from_identities, StatisticsAnalysisProvider, StatisticsAnalysisSnapshot,
};
use common::statistics::schema_fingerprint;
use error::{Result, VeinError};
use exec::operators::source::scan::PARQUET_FOOTER;
use parquet::arrow::arrow_reader::{
    ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReaderBuilder,
};
use parquet::file::metadata::ParquetMetaData;
use plan::{CatalogProvider, TableFormat, TableMetadata, VeinSchema};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;

mod statistics;

/// Catalog which resolves table names beneath a local data root.
#[derive(Debug, Clone)]
pub struct ParquetCatalog {
    root: Arc<PathBuf>,
    storage: ParquetStorage,
    metadata_store: MetadataStore,
}

impl ParquetCatalog {
    /// Creates a catalog rooted at `root`.
    pub fn new(root: impl Into<PathBuf>) -> Self {
        let root = root.into();
        Self {
            storage: ParquetStorage::new(root.clone()),
            root: Arc::new(root),
            metadata_store: MetadataStore::new(),
        }
    }

    fn resolve_path(&self, name: &str) -> PathBuf {
        let candidate = Path::new(name);
        if candidate.exists() {
            candidate.to_path_buf()
        } else {
            self.root.join(candidate)
        }
    }
}

#[async_trait]
impl CatalogProvider for ParquetCatalog {
    async fn get_table(&self, name: &str) -> Result<Arc<TableMetadata>> {
        let name = name.to_owned();
        let path = self.resolve_path(&name);
        let storage = self.storage.clone();
        let root = self.root.clone();
        let metadata_store = self.metadata_store.clone();
        tokio::task::spawn_blocking(move || {
            if let Some(manifest) = storage.read_manifest(&name)? {
                let discover_started = Instant::now();
                let files = manifest
                    .files
                    .iter()
                    .map(|file| resolve_manifest_file(&root, file))
                    .collect::<Result<Vec<_>>>()?;
                tracing::info!(
                    target: "vein.audit",
                    event = "catalog_discovery",
                    table = %name,
                    source = "manifest",
                    file_count = files.len(),
                    discover_us = discover_started.elapsed().as_micros() as u64,
                    "catalog_discovery"
                );
                let mut metadata = inspect_table_files(&name, &path, files, &metadata_store)?;
                if let Some(analyzed) = manifest.statistics.as_ref() {
                    let identities = storage.source_identities(&manifest.files)?;
                    apply_analyzed_statistics(
                        &mut metadata,
                        manifest.revision,
                        analyzed,
                        &identities,
                    );
                }
                Ok(metadata)
            } else {
                let mut metadata = inspect_table(&name, &path, &metadata_store)?;
                if let Some(analyzed) = storage.read_external_statistics(&name)? {
                    let identities = storage.source_identities_from_paths(&metadata.files)?;
                    let revision = revision_from_identities(&identities);
                    apply_analyzed_statistics(&mut metadata, revision, &analyzed, &identities);
                }
                Ok(metadata)
            }
        })
            .await
            .map_err(|error| VeinError::internal(format!("Parquet metadata worker failed: {error}")))?
            .map(Arc::new)
    }

    fn metadata_store(&self) -> MetadataStore {
        self.metadata_store.clone()
    }
}

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
            batches,
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
    sender: tokio::sync::mpsc::Sender<Result<RecordBatch>>,
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
                "read ANALYZE batch from '{}': {error}", path.display()
            )))?;
            sender
                .blocking_send(Ok(batch))
                .map_err(|_| VeinError::internal("ANALYZE consumer closed its batch stream"))?;
        }
    }
    Ok(())
}

fn inspect_table(name: &str, path: &Path, metadata_store: &MetadataStore) -> Result<TableMetadata> {
    let discover_started = Instant::now();
    if !path.exists() {
        return Err(VeinError::not_found(format!(
            "Parquet table '{name}' does not exist at '{}'",
            path.display()
        )));
    }

    let mut files = Vec::new();
    collect_parquet_files(path, &mut files)?;
    files.sort();
    if files.is_empty() {
        return Err(VeinError::not_found(format!(
            "no Parquet files found for table '{name}' at '{}'",
            path.display()
        )));
    }
    tracing::info!(
        target: "vein.audit",
        event = "catalog_discovery",
        table = %name,
        source = "directory",
        file_count = files.len(),
        discover_us = discover_started.elapsed().as_micros() as u64,
        "catalog_discovery"
    );

    inspect_table_files(name, path, files, metadata_store)
}

/// Loads value-derived statistics only when the snapshot, schema, row count,
/// and all source object versions still match their analyzed inputs.
fn apply_analyzed_statistics(
    metadata: &mut TableMetadata,
    manifest_revision: u64,
    analyzed: &AnalyzedTableStatistics,
    source_identities: &[StatisticsSourceIdentity],
) -> bool {
    let logical_schema_fingerprint = schema_fingerprint(&metadata.schema);
    let Some(table_statistics) = metadata.statistics.as_mut() else {
        return false;
    };
    let rejection = if analyzed.source_revision != manifest_revision {
        Some("catalog revision changed")
    } else if analyzed.schema_fingerprint != logical_schema_fingerprint {
        Some("logical schema changed")
    } else if analyzed.source_identities != source_identities {
        Some("source file version changed")
    } else if analyzed.statistics.num_rows != table_statistics.num_rows {
        Some("analyzed row count disagrees with storage metadata")
    } else if table_statistics.num_rows.is_some_and(|rows| {
        !column_groups_are_consistent(
            &metadata.schema,
            &analyzed.statistics.column_groups,
            rows,
        )
    }) {
        Some("analyzed multi-column statistics are inconsistent")
    } else {
        None
    };
    if let Some(reason) = rejection {
        tracing::debug!(
            target: "vein.audit",
            event = "catalog_statistics_rejected",
            table = %metadata.name,
            revision = manifest_revision,
            reason,
            "catalog_statistics_rejected"
        );
        return false;
    }

    let Some(rows) = table_statistics.num_rows else {
        return false;
    };
    if analyzed.statistics.column_statistics.len() != metadata.schema.fields.len() {
        tracing::debug!(
            target: "vein.audit",
            event = "catalog_statistics_rejected",
            table = %metadata.name,
            revision = manifest_revision,
            reason = "analyzed column set is incomplete",
            "catalog_statistics_rejected"
        );
        return false;
    }
    for field in &metadata.schema.fields {
        let Some(summary) = analyzed.statistics.column_statistics.get(&field.name) else {
            return false;
        };
        let Some(nulls) = summary.null_count else {
            return false;
        };
        if summary.nan_count.is_none()
            || summary.min_max_complete.is_none()
            || !summary.is_consistent_with_rows(rows)
        {
            tracing::debug!(
                target: "vein.audit",
                event = "catalog_statistics_rejected",
                table = %metadata.name,
                column = %field.name,
                revision = manifest_revision,
                reason = "analyzed column statistics are inconsistent",
                "catalog_statistics_rejected"
            );
            return false;
        }
        if table_statistics
            .column_statistics
            .get(&field.name)
            .and_then(|column| column.null_count)
            .is_some_and(|footer_nulls| footer_nulls != nulls)
        {
            tracing::debug!(
                target: "vein.audit",
                event = "catalog_statistics_rejected",
                table = %metadata.name,
                column = %field.name,
                revision = manifest_revision,
                reason = "footer and analyzed null counts disagree",
                "catalog_statistics_rejected"
            );
            return false;
        }
    }

    for (name, source) in &analyzed.statistics.column_statistics {
        let target = table_statistics
            .column_statistics
            .entry(name.clone())
            .or_default();
        target.null_count = target.null_count.or(source.null_count);
        target.nan_count = target.nan_count.or(source.nan_count);
        target.distinct_count = source.distinct_count;
        target.distinct_count_relative_error = source.distinct_count_relative_error;
        target.distinct_sketch.clone_from(&source.distinct_sketch);
        if source.min_max_complete == Some(true) {
            target.min_value.clone_from(&source.min_value);
            target.max_value.clone_from(&source.max_value);
            target.min_max_complete = Some(true);
        } else if source.min_max_complete == Some(false) {
            target.min_max_complete = Some(false);
        }
        target.numeric.clone_from(&source.numeric);
        target.byte_length.clone_from(&source.byte_length);
        target.most_common_values.clone_from(&source.most_common_values);
        target.histogram.clone_from(&source.histogram);
    }
    table_statistics.column_groups.clone_from(&analyzed.statistics.column_groups);
    tracing::info!(
        target: "vein.audit",
        event = "catalog_statistics_loaded",
        table = %metadata.name,
        revision = manifest_revision,
        row_count = rows,
        column_count = analyzed.statistics.column_statistics.len(),
        distinct_columns = analyzed.statistics.column_statistics.values()
            .filter(|column| column.distinct_count.is_some())
            .count(),
        bounded_columns = analyzed.statistics.column_statistics.values()
            .filter(|column| column.min_max_complete == Some(true))
            .count(),
        numeric_columns = analyzed.statistics.column_statistics.values()
            .filter(|column| column.numeric.is_some())
            .count(),
        variable_length_columns = analyzed.statistics.column_statistics.values()
            .filter(|column| column.byte_length.is_some())
            .count(),
        "catalog_statistics_loaded"
    );
    true
}

fn column_groups_are_consistent(
    schema: &VeinSchema,
    groups: &[catalog::ColumnGroupStatistics],
    rows: u64,
) -> bool {
    let mut seen = HashSet::with_capacity(groups.len());
    for group in groups {
        if group.columns.len() < 2
            || group.distinct_count.is_none_or(|count| count > rows)
            || group.distinct_count_relative_error.is_none_or(|error| {
                !error.is_finite() || !(0.0..=1.0).contains(&error)
            })
            || group.distinct_sketch.is_none()
        {
            return false;
        }
        let Some(positions) = group.columns.iter().map(|name| {
            schema.fields.iter().position(|field| field.name == *name)
        }).collect::<Option<Vec<_>>>() else {
            return false;
        };
        if positions.windows(2).any(|pair| pair[0] >= pair[1]) {
            return false;
        }
        if !seen.insert(group.columns.iter().map(String::as_str).collect::<Vec<_>>()) {
            return false;
        }
    }
    true
}

fn inspect_table_files(
    name: &str,
    path: &Path,
    mut files: Vec<PathBuf>,
    metadata_store: &MetadataStore,
) -> Result<TableMetadata> {
    let started = Instant::now();
    files.sort();
    if files.is_empty() {
        return Err(VeinError::not_found(format!(
            "no committed Parquet files found for table '{name}'"
        )));
    }

    let stat_started = Instant::now();
    let mut file_sizes = Vec::with_capacity(files.len());
    for file in &files {
        if !file.is_file() {
            return Err(VeinError::not_found(format!(
                "committed Parquet artifact '{}' is missing",
                file.display()
            )));
        }
        let file_metadata = fs::metadata(file).map_err(|error| {
            VeinError::io(format!("read Parquet file metadata '{}'", file.display()), error)
        })?;
        file_sizes.push(file_metadata.len());
    }
    let stat_us = stat_started.elapsed().as_micros() as u64;

    let footer_started = Instant::now();
    let footers = parquet_schemas(&files, metadata_store)?;
    let footer_us = footer_started.elapsed().as_micros() as u64;

    let footer_hits = footers
        .iter()
        .filter(|(_, _, served_from_store)| *served_from_store)
        .count() as u64;
    let footer_misses = footers.len() as u64 - footer_hits;

    let expected_schema = Arc::clone(&footers[0].0);
    for (file, (schema, _, _)) in files.iter().zip(footers.iter()).skip(1) {
        if schema.as_ref() != expected_schema.as_ref() {
            return Err(VeinError::internal(format!(
                "Parquet schema mismatch in '{}'; all files in one table must have identical schemas",
                file.display()
            )));
        }
    }
    let file_count = files.len();

    let mut data_units = Vec::new();
    for (file_ordinal, ((schema, footer, _), file_bytes)) in
        footers.iter().zip(file_sizes.iter()).enumerate()
    {
        let first_unit = data_units.len();
        let mut file_units = statistics::file_units(file_ordinal, *file_bytes, schema, footer);
        for unit in &mut file_units {
            if let Some(parent) = &mut unit.parent {
                *parent += first_unit;
            }
        }
        data_units.extend(file_units);
    }
    let table_statistics = statistics::table_statistics(data_units, expected_schema.as_ref());
    let row_group_count = table_statistics.row_group_count.unwrap_or(0);
    let column_statistics_count = table_statistics.column_statistics.len();
    let table_rows = table_statistics.num_rows;
    let table_bytes = table_statistics.total_bytes;
    let compressed_bytes = table_statistics.compressed_bytes;
    let uncompressed_bytes = table_statistics.uncompressed_bytes;

    let table_schema = Arc::new(VeinSchema::try_from(expected_schema.as_ref())?);
    let files = files
        .into_iter()
        .map(|file| file.to_string_lossy().into_owned())
        .collect();
    let mut metadata = TableMetadata::new(name, table_schema)
        .with_location(path.to_string_lossy())
        .with_format(TableFormat::Parquet)
        .with_files(files);
    metadata = metadata.with_statistics(table_statistics);

    tracing::info!(
        target: "vein.audit",
        event = "catalog_schema",
        table = %name,
        file_count,
        row_group_count,
        table_rows,
        table_bytes,
        compressed_bytes,
        uncompressed_bytes,
        column_statistics_count,
        stat_us,
        footer_us,
        footer_hits,
        footer_misses,
        total_us = started.elapsed().as_micros() as u64,
        "catalog_schema"
    );
    Ok(metadata)
}

/// Parses every file's footer concurrently, returning each file's schema,
/// shared footer, and whether the catalog's store already held the metadata.
///
/// Reading a footer opens and decodes one file, which costs hundreds of
/// microseconds and depends on no other file. Reading them one after another
/// made a table's planning time grow with its file count — on the order of a
/// second per three thousand files — for work that is entirely independent, so
/// files are claimed from a shared cursor by as many workers as the machine can
/// run.
///
/// Results come back in file order and the first file to fail is the one
/// reported, so a table with an unreadable file fails the same way it did when
/// the footers were read one at a time.
fn parquet_schemas(
    files: &[PathBuf],
    metadata_store: &MetadataStore,
) -> Result<Vec<(Arc<arrow::datatypes::Schema>, Arc<ParquetMetaData>, bool)>> {
    let slots: Mutex<
        Vec<Option<Result<(Arc<arrow::datatypes::Schema>, Arc<ParquetMetaData>, bool)>>>,
    > =
        Mutex::new((0..files.len()).map(|_| None).collect());
    let next = AtomicUsize::new(0);
    let workers = std::thread::available_parallelism()
        .map(|parallelism| parallelism.get())
        .unwrap_or(1)
        .min(files.len())
        .max(1);

    std::thread::scope(|scope| {
        for _ in 0..workers {
            scope.spawn(|| {
                loop {
                    let index = next.fetch_add(1, Ordering::Relaxed);
                    let Some(file) = files.get(index) else {
                        return;
                    };
                    let parsed = parquet_schema(file, metadata_store);
                    let Ok(mut slots) = slots.lock() else {
                        return;
                    };
                    slots[index] = Some(parsed);
                }
            });
        }
    });

    slots
        .into_inner()
        .map_err(|_| VeinError::internal("Parquet footer parse panicked"))?
        .into_iter()
        .enumerate()
        .map(|(index, slot)| {
            slot.unwrap_or_else(|| {
                Err(VeinError::internal(format!(
                    "Parquet footer for '{}' was never parsed",
                    files[index].display()
                )))
            })
        })
        .collect()
}

/// Returns the Arrow schema and shared Parquet footer for `path`, plus whether
/// the catalog's store served its footer metadata.
///
/// The parsed footer is published under [`PARQUET_FOOTER`] so the scan reading
/// the same file reuses it instead of parsing the footer a second time.
fn parquet_schema(
    path: &Path,
    metadata_store: &MetadataStore,
) -> Result<(Arc<arrow::datatypes::Schema>, Arc<ParquetMetaData>, bool)> {
    let key = MetadataKey::for_file(PARQUET_FOOTER, path);
    if let Some(key) = &key {
        if let Some(metadata) = metadata_store.get::<ParquetMetaData>(key) {
            let reader = ArrowReaderMetadata::try_new(Arc::clone(&metadata), ArrowReaderOptions::new())
                .map_err(|error| {
                    VeinError::internal(format!(
                        "derive Arrow schema from '{}': {error}",
                        path.display()
                    ))
                })?;
            return Ok((reader.schema().clone(), metadata, true));
        }
    }

    let file = File::open(path)
        .map_err(|error| VeinError::io(format!("open Parquet file '{}'", path.display()), error))?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file).map_err(|error| {
        VeinError::internal(format!(
            "read Parquet metadata from '{}': {error}",
            path.display()
        ))
    })?;
    let schema = reader.schema().clone();
    let metadata = Arc::clone(reader.metadata());
    if let Some(key) = key {
        metadata_store.insert(key, Arc::clone(&metadata));
        tracing::debug!(
            target: "vein.audit",
            event = "parquet_footer_parsed",
            path = %path.display(),
            "parquet_footer_parsed"
        );
    }
    Ok((schema, metadata, false))
}

fn collect_parquet_files(path: &Path, files: &mut Vec<PathBuf>) -> Result<()> {
    if path.is_file() {
        if is_parquet_file(path) {
            files.push(path.to_path_buf());
        }
        return Ok(());
    }

    let entries = fs::read_dir(path)
        .map_err(|error| VeinError::io(format!("read directory '{}'", path.display()), error))?;
    for entry in entries {
        let entry = entry.map_err(|error| {
            VeinError::io(
                format!("read directory entry in '{}'", path.display()),
                error,
            )
        })?;
        let file_type = entry
            .file_type()
            .map_err(|error| VeinError::io("read Parquet entry type", error))?;
        let file_name = entry.file_name();
        let file_name = file_name.to_string_lossy();
        if file_name.starts_with('.') || file_name.starts_with('_') || file_type.is_symlink() {
            continue;
        }

        let child = entry.path();
        if file_type.is_dir() {
            collect_parquet_files(&child, files)?;
        } else if file_type.is_file() && is_parquet_file(&child) {
            files.push(child);
        }
    }
    Ok(())
}

fn is_parquet_file(path: &Path) -> bool {
    path.extension()
        .and_then(|extension| extension.to_str())
        .is_some_and(|extension| extension.eq_ignore_ascii_case("parquet"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;

    fn write_parquet(path: &Path, field: &str) {
        let schema = Arc::new(Schema::new(vec![Field::new(field, DataType::Int64, false)]));
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![1, 2]))])
                .expect("test batch is valid");
        let file = File::create(path).expect("create test parquet file");
        let mut writer = ArrowWriter::try_new(file, schema, None).expect("create writer");
        writer.write(&batch).expect("write test batch");
        writer.close().expect("finish test parquet file");
    }

    fn write_parquet_with_row_groups(path: &Path, values: Vec<Option<i64>>) {
        let schema = Arc::new(Schema::new(vec![Field::new("value", DataType::Int64, true)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(values))],
        )
            .expect("test batch is valid");
        let file = File::create(path).expect("create test parquet file");
        let mut writer = ArrowWriter::try_new(file, schema, None).expect("create writer");
        const ROW_GROUP_ROWS: usize = 2;
        for offset in (0..batch.num_rows()).step_by(ROW_GROUP_ROWS) {
            let rows = ROW_GROUP_ROWS.min(batch.num_rows() - offset);
            writer
                .write(&batch.slice(offset, rows))
                .expect("write test row group");
            writer.flush().expect("flush test row group");
        }
        writer.close().expect("finish test parquet file");
    }

    #[tokio::test]
    async fn resolves_sorted_parquet_directory_and_ignores_hidden_entries() {
        let root = tempfile::tempdir().expect("temporary root");
        let table = root.path().join("flights");
        fs::create_dir_all(&table).expect("create table directory");
        write_parquet(&table.join("b.parquet"), "flight_id");
        write_parquet(&table.join("a.parquet"), "flight_id");
        write_parquet(&table.join("_metadata.parquet"), "ignored");

        let catalog = ParquetCatalog::new(root.path());
        let metadata = catalog.get_table("flights").await.expect("table resolves");

        assert_eq!(metadata.name, "flights");
        assert_eq!(metadata.files.len(), 2);
        assert!(metadata.files[0].ends_with("a.parquet"));
        assert!(metadata.files[1].ends_with("b.parquet"));
        assert_eq!(metadata.schema.fields[0].name, "flight_id");
        assert_eq!(
            metadata
                .statistics
                .as_ref()
                .and_then(|stats| stats.num_rows),
            Some(4)
        );
    }

    #[tokio::test]
    async fn rejects_mixed_schemas_in_a_table_directory() {
        let root = tempfile::tempdir().expect("temporary root");
        let table = root.path().join("flights");
        fs::create_dir_all(&table).expect("create table directory");
        write_parquet(&table.join("a.parquet"), "flight_id");
        write_parquet(&table.join("b.parquet"), "other_id");

        let error = ParquetCatalog::new(root.path())
            .get_table("flights")
            .await
            .expect_err("mixed schemas must fail");
        assert!(error.to_string().contains("schema mismatch"));
    }

    #[tokio::test]
    async fn collects_footer_statistics_at_file_row_group_and_table_scopes() {
        let root = tempfile::tempdir().expect("temporary root");
        let table = root.path().join("measurements");
        fs::create_dir_all(&table).expect("create table directory");
        write_parquet_with_row_groups(
            &table.join("a.parquet"),
            vec![Some(1), None, Some(4), Some(5)],
        );
        write_parquet_with_row_groups(&table.join("b.parquet"), vec![Some(8), Some(9)]);

        let catalog = ParquetCatalog::new(root.path());
        let metadata = catalog
            .get_table("measurements")
            .await
            .expect("table resolves");
        let statistics = metadata.statistics.as_ref().expect("footer statistics");

        assert_eq!(statistics.num_rows, Some(6));
        assert_eq!(statistics.file_count, Some(2));
        assert_eq!(statistics.row_group_count, Some(3));
        assert!(statistics.total_bytes.is_some_and(|bytes| bytes > 0));
        assert!(statistics.compressed_bytes.is_some_and(|bytes| bytes > 0));
        assert!(statistics.uncompressed_bytes.is_some_and(|bytes| bytes > 0));
        assert_eq!(statistics.data_units.len(), 5);
        assert_eq!(statistics.data_units[0].kind, catalog::StatisticsUnitKind::File);
        assert_eq!(statistics.data_units[1].parent, Some(0));
        assert_eq!(statistics.data_units[3].kind, catalog::StatisticsUnitKind::File);
        assert_eq!(statistics.data_units[4].parent, Some(3));

        let column = &statistics.column_statistics["value"];
        assert_eq!(column.null_count, Some(1));
        assert_eq!(column.min_value, Some(plan::LogicalLiteral::Int64(1)));
        assert_eq!(column.max_value, Some(plan::LogicalLiteral::Int64(9)));
        assert_eq!(column.distinct_count, None);

        let row_group = &statistics.data_units[1];
        let column_storage = &row_group.column_storage["value"];
        assert_eq!(column_storage.num_values, Some(2));
        assert!(column_storage.compressed_bytes.is_some_and(|bytes| bytes > 0));
        assert!(column_storage.uncompressed_bytes.is_some_and(|bytes| bytes > 0));
        assert!(column_storage.compression.is_some());
        assert!(!column_storage.encodings.is_empty());
        assert_eq!(column_storage.has_statistics, Some(true));
    }

    #[test]
    fn analyzed_column_statistics_reject_contradictory_synopses() {
        let valid = catalog::ColumnStatistics {
            null_count: Some(1),
            distinct_count: Some(4),
            nan_count: Some(0),
            min_value: Some(plan::LogicalLiteral::Int64(1)),
            max_value: Some(plan::LogicalLiteral::Int64(9)),
            min_max_complete: Some(true),
            numeric: Some(catalog::NumericStatistics {
                finite_count: 9,
                non_finite_count: 0,
                sum: Some(45.0),
                mean: Some(5.0),
                m2: Some(60.0),
            }),
            ..catalog::ColumnStatistics::default()
        };
        assert!(valid.is_consistent_with_rows(10));

        let mut invalid = valid.clone();
        invalid.numeric.as_mut().unwrap().finite_count = 8;
        assert!(!invalid.is_consistent_with_rows(10));

        let mut invalid = valid.clone();
        invalid.numeric.as_mut().unwrap().m2 = Some(-1.0);
        assert!(!invalid.is_consistent_with_rows(10));

        let mut invalid = valid.clone();
        invalid.min_value = Some(plan::LogicalLiteral::Int64(10));
        assert!(!invalid.is_consistent_with_rows(10));

        let mut invalid = valid.clone();
        invalid.min_value = None;
        invalid.max_value = None;
        assert!(
            !invalid.is_consistent_with_rows(10),
            "complete bounds cannot be absent when ordered values exist"
        );

        let mut invalid = valid.clone();
        invalid.min_max_complete = Some(false);
        assert!(
            !invalid.is_consistent_with_rows(10),
            "incomplete bounds cannot retain a misleading min/max pair"
        );

        let all_nan = catalog::ColumnStatistics {
            null_count: Some(0),
            distinct_count: Some(1),
            nan_count: Some(2),
            min_max_complete: Some(true),
            numeric: Some(catalog::NumericStatistics {
                finite_count: 0,
                non_finite_count: 2,
                sum: Some(0.0),
                mean: None,
                m2: None,
            }),
            ..catalog::ColumnStatistics::default()
        };
        assert!(all_nan.is_consistent_with_rows(2));

        let empty = catalog::ColumnStatistics {
            null_count: Some(0),
            nan_count: Some(0),
            min_max_complete: Some(true),
            ..catalog::ColumnStatistics::default()
        };
        assert!(empty.is_consistent_with_rows(0));

        let mut invalid = valid.clone();
        invalid.distinct_count = None;
        invalid.distinct_count_relative_error = Some(0.1);
        assert!(
            !invalid.is_consistent_with_rows(10),
            "an estimate error requires a corresponding distinct estimate"
        );

        let invalid_lengths = catalog::ColumnStatistics {
            null_count: Some(1),
            byte_length: Some(catalog::ByteLengthStatistics {
                value_count: 9,
                total_bytes: Some(8),
                min_bytes: Some(1),
                max_bytes: Some(2),
                contains_non_ascii: None,
            }),
            ..catalog::ColumnStatistics::default()
        };
        assert!(!invalid_lengths.is_consistent_with_rows(10));
    }
}
