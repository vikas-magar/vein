//! Read-only Parquet catalog with deterministic file discovery and schema checks.

use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use crate::parquet_storage::{ParquetStorage, resolve_manifest_file};
use async_trait::async_trait;
use catalog::{CatalogSnapshotToken, MetadataKey, MetadataStore};
use common::statistics::analysis::revision_from_identities;
use common::statistics::{
    RuntimeStatisticsStore, runtime::table_statistics_key, schema_fingerprint,
    stable_identifier_hash,
};
use error::{Result, VeinError};
use exec::operators::source::scan::PARQUET_FOOTER;
use exec::parquet_statistics::{file_units, table_statistics};
use parquet::arrow::arrow_reader::{
    ArrowReaderMetadata, ArrowReaderOptions,
};
use parquet::file::metadata::{PageIndexPolicy, ParquetMetaData, ParquetMetaDataReader};
use plan::{CatalogProvider, TableFormat, TableMetadata, VeinSchema};

type ParquetSchema = Arc<arrow::datatypes::Schema>;
type ParsedParquetFooter = (ParquetSchema, Arc<ParquetMetaData>, bool);
type ParsedParquetFooterResult = Result<ParsedParquetFooter>;
type ParsedParquetFooterSlots = Mutex<Vec<Option<ParsedParquetFooterResult>>>;

mod analysis;
mod statistics;

/// Catalog which resolves table names beneath a local data root.
#[derive(Debug, Clone)]
pub struct ParquetCatalog {
    root: Arc<PathBuf>,
    storage: ParquetStorage,
    metadata_store: MetadataStore,
    runtime_statistics: RuntimeStatisticsStore,
}

impl ParquetCatalog {
    /// Creates a catalog rooted at `root`.
    pub fn new(root: impl Into<PathBuf>) -> Self {
        let root = root.into();
        Self {
            storage: ParquetStorage::new(root.clone()),
            root: Arc::new(root),
            metadata_store: MetadataStore::new(),
            runtime_statistics: RuntimeStatisticsStore::default(),
        }
    }

    /// Shares lazily collected runtime statistics with future catalog lookups.
    pub fn with_runtime_statistics_store(mut self, store: RuntimeStatisticsStore) -> Self {
        self.runtime_statistics = store;
        self
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
        let runtime_statistics = self.runtime_statistics.clone();
        tokio::task::spawn_blocking(move || {
            let mut metadata = if let Some(manifest) = storage.read_manifest(&name)? {
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
                    statistics::apply_analyzed_statistics(
                        &mut metadata,
                        manifest.revision,
                        analyzed,
                        &identities,
                    );
                }
                metadata
            } else {
                let mut metadata = inspect_table(&name, &path, &metadata_store)?;
                if let Some(analyzed) = storage.read_external_statistics(&name)? {
                    let identities = storage.source_identities_from_paths(&metadata.files)?;
                    let revision = revision_from_identities(&identities);
                    statistics::apply_analyzed_statistics(&mut metadata, revision, &analyzed, &identities);
                }
                metadata
            };
            // Pin runtime synopses to the source object versions and logical schema
            // observed by this catalog lookup. Paths alone are not a revision:
            // an external file can be replaced in place without changing its path.
            let identities = storage.source_identities_from_paths(&metadata.files)?;
            let source_revision = revision_from_identities(&identities);
            let schema_version = stable_identifier_hash(&schema_fingerprint(&metadata.schema));
            metadata.catalog_snapshot = Some(CatalogSnapshotToken::new(
                metadata.name.clone(),
                format!("source:{source_revision:016x}"),
                format!("{source_revision:016x}"),
                schema_version,
            ));
            let runtime_key = metadata
                .catalog_snapshot
                .as_ref()
                .map(table_statistics_key)
                .ok_or_else(|| VeinError::internal("catalog source snapshot was not assigned"))?;
            if let Some(runtime) = runtime_statistics.complete_table_statistics(&runtime_key) {
                let statistics = metadata.statistics.get_or_insert_with(Default::default);
                statistics.num_rows = runtime.num_rows;
                for (column, synopsis) in &runtime.column_statistics {
                    statistics.column_statistics.insert(column.clone(), synopsis.clone());
                }
                if !runtime.column_groups.is_empty() {
                    statistics.column_groups = runtime.column_groups.clone();
                }
                tracing::info!(
                    target: "vein.audit",
                    event = "catalog_runtime_statistics_applied",
                    table = %metadata.name,
                    column_count = runtime.column_statistics.len(),
                    column_group_count = runtime.column_groups.len(),
                    rows = ?runtime.num_rows,
                    source_revision = %source_revision,
                    "catalog_runtime_statistics_applied"
                );
            } else {
                tracing::debug!(
                    target: "vein.audit",
                    event = "catalog_runtime_statistics_unavailable",
                    table = %metadata.name,
                    source_revision = %source_revision,
                    reason = "no_complete_snapshot_for_current_source_revision",
                    "catalog runtime statistics unavailable"
                );
            }
            Ok(metadata)
        })
            .await
            .map_err(|error| VeinError::internal(format!("Parquet metadata worker failed: {error}")))?
            .map(Arc::new)
    }

    fn metadata_store(&self) -> MetadataStore {
        self.metadata_store.clone()
    }
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
        let mut file_units = file_units(file_ordinal, *file_bytes, schema, footer);
        for unit in &mut file_units {
            if let Some(parent) = &mut unit.parent {
                *parent += first_unit;
            }
        }
        data_units.extend(file_units);
    }
    let table_statistics = table_statistics(data_units, expected_schema.as_ref());
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
) -> Result<Vec<ParsedParquetFooter>> {
    let slots: ParsedParquetFooterSlots =
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
    if let Some(key) = &key
        && let Some(metadata) = metadata_store.get::<ParquetMetaData>(key) {
        let reader = ArrowReaderMetadata::try_new(Arc::clone(&metadata), ArrowReaderOptions::new())
            .map_err(|error| {
                VeinError::internal(format!(
                    "derive Arrow schema from '{}': {error}",
                    path.display()
                ))
            })?;
        return Ok((reader.schema().clone(), metadata, true));
    }

    let file = File::open(path)
        .map_err(|error| VeinError::io(format!("open Parquet file '{}'", path.display()), error))?;
    let metadata = ParquetMetaDataReader::new()
        .with_page_index_policy(PageIndexPolicy::Optional)
        .parse_and_finish(&file)
        .map_err(|error| {
            VeinError::internal(format!(
                "read Parquet metadata from '{}': {error}",
                path.display()
            ))
        })?;
    let metadata = Arc::new(metadata);
    let reader = ArrowReaderMetadata::try_new(Arc::clone(&metadata), ArrowReaderOptions::new())
        .map_err(|error| {
            VeinError::internal(format!(
                "derive Arrow schema from '{}': {error}",
                path.display()
            ))
        })?;
    let schema = reader.schema().clone();
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
