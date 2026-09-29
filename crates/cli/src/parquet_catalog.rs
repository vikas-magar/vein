//! Read-only Parquet catalog with deterministic file discovery and schema checks.

use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use error::{Result, VeinError};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use plan::{CatalogProvider, TableFormat, TableMetadata, VeinSchema};
use crate::parquet_storage::{ParquetStorage, resolve_manifest_file};

/// Catalog which resolves table names beneath a local data root.
#[derive(Debug, Clone)]
pub struct ParquetCatalog {
    root: Arc<PathBuf>,
    storage: ParquetStorage,
}

impl ParquetCatalog {
    /// Creates a catalog rooted at `root`.
    pub fn new(root: impl Into<PathBuf>) -> Self {
        let root = root.into();
        Self { storage: ParquetStorage::new(root.clone()), root: Arc::new(root) }
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
        tokio::task::spawn_blocking(move || {
            if let Some(manifest) = storage.read_manifest(&name)? {
                let files = manifest.files.iter()
                    .map(|file| resolve_manifest_file(&root, file))
                    .collect::<Result<Vec<_>>>()?;
                inspect_table_files(&name, &path, files)
            } else {
                inspect_table(&name, &path)
            }
        })
            .await
            .map_err(|error| {
                VeinError::internal(format!("Parquet metadata worker failed: {error}"))
            })?
            .map(Arc::new)
    }
}

fn inspect_table(name: &str, path: &Path) -> Result<TableMetadata> {
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

    inspect_table_files(name, path, files)
}

fn inspect_table_files(name: &str, path: &Path, mut files: Vec<PathBuf>) -> Result<TableMetadata> {
    files.sort();
    if files.is_empty() {
        return Err(VeinError::not_found(format!("no committed Parquet files found for table '{name}'")));
    }
    for file in &files {
        if !file.is_file() {
            return Err(VeinError::not_found(format!("committed Parquet artifact '{}' is missing", file.display())));
        }
    }
    let expected_schema = parquet_schema(&files[0])?;
    for file in files.iter().skip(1) {
        let schema = parquet_schema(file)?;
        if schema.as_ref() != expected_schema.as_ref() {
            return Err(VeinError::internal(format!(
                "Parquet schema mismatch in '{}'; all files in one table must have identical schemas",
                file.display()
            )));
        }
    }

    let table_schema = Arc::new(VeinSchema::from(expected_schema.as_ref()));
    let files = files
        .into_iter()
        .map(|file| file.to_string_lossy().into_owned())
        .collect();
    Ok(TableMetadata::new(name, table_schema)
        .with_location(path.to_string_lossy())
        .with_format(TableFormat::Parquet)
        .with_files(files))
}

fn parquet_schema(path: &Path) -> Result<Arc<arrow::datatypes::Schema>> {
    let file = File::open(path)
        .map_err(|error| VeinError::io(format!("open Parquet file '{}'", path.display()), error))?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file).map_err(|error| {
        VeinError::internal(format!(
            "read Parquet metadata from '{}': {error}",
            path.display()
        ))
    })?;
    Ok(reader.schema().clone())
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
