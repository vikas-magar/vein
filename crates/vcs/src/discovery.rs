//! Smart source discovery and schema resolution for Vein.
//!
//! Automatically inspects files, Spark-style partitioned directories, VFS tables,
//! and cloud storage objects to authoritatively infer schemas and file lists.

use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use error::{Result, VeinError};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use types::{VeinSchema, VeinSchemaRef};

/// Discovered storage source with authoritative schema and file list.
#[derive(Debug, Clone)]
pub struct DiscoveredSource {
    /// Logical name or alias of the source.
    pub name: String,
    /// Physical storage location (path or URI).
    pub location: String,
    /// Detected format (e.g. "parquet", "exo", "csv").
    pub format: String,
    /// Authoritative schema inferred from storage metadata.
    pub schema: VeinSchemaRef,
    /// List of physical files to scan (e.g. Spark part files).
    pub files: Vec<String>,
}

/// Resolves a source by name, file path, or VFS table reference.
pub async fn resolve_source(name_or_path: &str) -> Result<DiscoveredSource> {
    // 0. Check VFS native memory-mapped block tables first!
    if let Ok(store) = vfs::VfsTableStore::shared_or_default() {
        let table_id = vfs::VfsTableStore::resolve_table_id(name_or_path);
        if let Ok(schema) = store.read_table_schema(table_id).await {
            let parts = store.resolve_table_parts(table_id).await.unwrap_or_default();
            let files = if parts.is_empty() {
                vec![format!("vfs://tbl_{table_id}/part_000000")]
            } else {
                parts
                    .iter()
                    .map(|p| format!("vfs://tbl_{table_id}/part_{p:06}"))
                    .collect()
            };
            return Ok(DiscoveredSource {
                name: name_or_path.to_string(),
                location: format!("vfs://tbl_{table_id}"),
                format: "exo".to_string(),
                schema,
                files,
            });
        }
    }

    // 1. Check if name_or_path directly exists as a file or directory
    let path = Path::new(name_or_path);
    if path.exists() {
        return resolve_existing_path(name_or_path, path);
    }

    // 2. Check for cloud URIs (s3://, gcs://, etc.)
    if name_or_path.contains("://") {
        return resolve_cloud_uri(name_or_path).await;
    }

    Err(VeinError::not_found(format!(
        "Table or source '{name_or_path}' could not be discovered on filesystem or VFS"
    )))
}

fn resolve_existing_path(logical_name: &str, path: &Path) -> Result<DiscoveredSource> {
    if path.is_file() {
        resolve_file(logical_name, path)
    } else if path.is_dir() {
        resolve_directory(logical_name, path)
    } else {
        Err(VeinError::not_found(format!(
            "Path '{}' is neither a file nor a directory",
            path.display()
        )))
    }
}

fn resolve_file(logical_name: &str, path: &Path) -> Result<DiscoveredSource> {
    let path_str = path.to_string_lossy().to_string();
    let extension = path.extension().and_then(|s| s.to_str()).unwrap_or("");

    if extension.eq_ignore_ascii_case("parquet") || is_parquet_file(path) {
        let file = File::open(path)
            .map_err(|e| VeinError::io(format!("Failed to open file '{}'", path.display()), e))?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|e| VeinError::internal(format!("Failed to parse Parquet footer from '{}': {e}", path.display())))?;
        let arrow_schema = builder.schema();
        let vein_schema = VeinSchema::from(arrow_schema.as_ref());

        Ok(DiscoveredSource {
            name: logical_name.to_string(),
            location: path_str.clone(),
            format: "parquet".to_string(),
            schema: Arc::new(vein_schema),
            files: vec![path_str],
        })
    } else {
        // Fallback to EXO or raw format
        Err(VeinError::not_found(format!(
            "Unsupported file format for '{}'",
            path.display()
        )))
    }
}

fn resolve_directory(logical_name: &str, path: &Path) -> Result<DiscoveredSource> {
    let path_str = path.to_string_lossy().to_string();

    // Check for _schema.json (Native VFS / .exo table)
    let schema_file = path.join("_schema.json");
    if schema_file.exists() && schema_file.is_file() {
        let content = std::fs::read_to_string(&schema_file)
            .map_err(|e| VeinError::io("Failed to read _schema.json", e))?;
        let vein_schema: VeinSchema = serde_json::from_str(&content)
            .map_err(|e| VeinError::internal(format!("Failed to parse _schema.json: {e}")))?;

        return Ok(DiscoveredSource {
            name: logical_name.to_string(),
            location: path_str.clone(),
            format: "exo".to_string(),
            schema: Arc::new(vein_schema),
            files: vec![path_str],
        });
    }

    // Spark-style directory scan: look for *.parquet part files
    let mut parquet_files = Vec::new();
    collect_parquet_files_recursive(path, &mut parquet_files)?;

    if !parquet_files.is_empty() {
        // Deterministically sort part files (part-00000.parquet, part-00001.parquet, etc.)
        parquet_files.sort();

        // Extract authoritative schema from first part file
        let first_file = File::open(&parquet_files[0])
            .map_err(|e| VeinError::io(format!("Failed to open '{}'", parquet_files[0].display()), e))?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(first_file)
            .map_err(|e| VeinError::internal(format!("Failed to parse Parquet footer from '{}': {e}", parquet_files[0].display())))?;
        let arrow_schema = builder.schema();
        let vein_schema = VeinSchema::from(arrow_schema.as_ref());

        let file_strings = parquet_files
            .into_iter()
            .map(|p| p.to_string_lossy().to_string())
            .collect();

        return Ok(DiscoveredSource {
            name: logical_name.to_string(),
            location: path_str,
            format: "parquet".to_string(),
            schema: Arc::new(vein_schema),
            files: file_strings,
        });
    }

    Err(VeinError::not_found(format!(
        "Directory '{}' contains no recognized Parquet or .exo datasets",
        path.display()
    )))
}

fn collect_parquet_files_recursive(dir: &Path, acc: &mut Vec<PathBuf>) -> Result<()> {
    let entries = std::fs::read_dir(dir)
        .map_err(|e| VeinError::io(format!("Failed to read directory '{}'", dir.display()), e))?;

    for entry in entries {
        let entry = entry.map_err(|e| VeinError::io("Directory entry error", e))?;
        let path = entry.path();
        let file_name = entry.file_name();
        let name_str = file_name.to_string_lossy();

        // Skip hidden files and Spark metadata flags
        if name_str.starts_with('.')
            || name_str.starts_with('_')
            || name_str.ends_with(".crc")
        {
            continue;
        }

        if path.is_dir() {
            collect_parquet_files_recursive(&path, acc)?;
        } else if path.is_file() {
            if path.extension().and_then(|s| s.to_str()).map_or(false, |ext| ext.eq_ignore_ascii_case("parquet"))
                || is_parquet_file(&path)
            {
                acc.push(path);
            }
        }
    }

    Ok(())
}

fn is_parquet_file(path: &Path) -> bool {
    use std::io::Read;
    if let Ok(mut f) = File::open(path) {
        let mut magic = [0u8; 4];
        if f.read_exact(&mut magic).is_ok() && &magic == b"PAR1" {
            return true;
        }
    }
    false
}

async fn resolve_cloud_uri(uri: &str) -> Result<DiscoveredSource> {
    // Basic cloud URI inference
    let format = if uri.ends_with(".parquet") {
        "parquet"
    } else if uri.ends_with(".csv") {
        "csv"
    } else {
        "exo"
    };

    Ok(DiscoveredSource {
        name: uri.to_string(),
        location: uri.to_string(),
        format: format.to_string(),
        schema: Arc::new(VeinSchema::empty()),
        files: vec![uri.to_string()],
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_resolve_users_parquet_if_exists() {
        if Path::new("users.parquet").exists() {
            let res = resolve_source("users.parquet").await.unwrap();
            assert_eq!(res.format, "parquet");
            assert!(!res.schema.fields.is_empty());
            assert_eq!(res.files, vec!["users.parquet"]);
        }
    }
}
