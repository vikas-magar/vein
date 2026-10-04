//! Read-only Parquet catalog with deterministic file discovery and schema checks.

use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use crate::parquet_storage::{ParquetStorage, resolve_manifest_file};
use async_trait::async_trait;
use catalog::{MetadataKey, MetadataStore};
use error::{Result, VeinError};
use exec::operators::source::scan::PARQUET_FOOTER;
use parquet::arrow::arrow_reader::{
    ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReaderBuilder,
};
use parquet::file::metadata::ParquetMetaData;
use plan::{CatalogProvider, TableFormat, TableMetadata, VeinSchema};

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
                inspect_table_files(&name, &path, files, &metadata_store)
            } else {
                inspect_table(&name, &path, &metadata_store)
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
    for file in &files {
        if !file.is_file() {
            return Err(VeinError::not_found(format!(
                "committed Parquet artifact '{}' is missing",
                file.display()
            )));
        }
    }
    let stat_us = stat_started.elapsed().as_micros() as u64;

    let footer_started = Instant::now();
    let schemas = parquet_schemas(&files, metadata_store)?;
    let footer_us = footer_started.elapsed().as_micros() as u64;

    let footer_hits = schemas
        .iter()
        .filter(|(_, served_from_store)| *served_from_store)
        .count() as u64;
    let footer_misses = schemas.len() as u64 - footer_hits;

    let expected_schema = Arc::clone(&schemas[0].0);
    for (file, (schema, _)) in files.iter().zip(schemas.iter()).skip(1) {
        if schema.as_ref() != expected_schema.as_ref() {
            return Err(VeinError::internal(format!(
                "Parquet schema mismatch in '{}'; all files in one table must have identical schemas",
                file.display()
            )));
        }
    }
    let file_count = files.len();

    let table_schema = Arc::new(VeinSchema::try_from(expected_schema.as_ref())?);
    let files = files
        .into_iter()
        .map(|file| file.to_string_lossy().into_owned())
        .collect();
    let metadata = TableMetadata::new(name, table_schema)
        .with_location(path.to_string_lossy())
        .with_format(TableFormat::Parquet)
        .with_files(files);

    tracing::info!(
        target: "vein.audit",
        event = "catalog_schema",
        table = %name,
        file_count,
        stat_us,
        footer_us,
        footer_hits,
        footer_misses,
        total_us = started.elapsed().as_micros() as u64,
        "catalog_schema"
    );
    Ok(metadata)
}

/// Parses every file's footer concurrently, returning each file's schema and
/// whether the catalog's store already held the parsed metadata.
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
) -> Result<Vec<(Arc<arrow::datatypes::Schema>, bool)>> {
    let slots: Mutex<Vec<Option<Result<(Arc<arrow::datatypes::Schema>, bool)>>>> =
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

/// Returns the Arrow schema of `path` and whether the catalog's store served it.
///
/// The parsed footer is published under [`PARQUET_FOOTER`] so the scan reading
/// the same file reuses it instead of parsing the footer a second time.
fn parquet_schema(
    path: &Path,
    metadata_store: &MetadataStore,
) -> Result<(Arc<arrow::datatypes::Schema>, bool)> {
    let key = MetadataKey::for_file(PARQUET_FOOTER, path);
    if let Some(key) = &key {
        if let Some(metadata) = metadata_store.get::<ParquetMetaData>(key) {
            let reader = ArrowReaderMetadata::try_new(metadata, ArrowReaderOptions::new())
                .map_err(|error| {
                    VeinError::internal(format!(
                        "derive Arrow schema from '{}': {error}",
                        path.display()
                    ))
                })?;
            return Ok((reader.schema().clone(), true));
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
    if let Some(key) = key {
        metadata_store.insert(key, Arc::clone(reader.metadata()));
        tracing::debug!(
            target: "vein.audit",
            event = "parquet_footer_parsed",
            path = %path.display(),
            "parquet_footer_parsed"
        );
    }
    Ok((schema, false))
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
}
