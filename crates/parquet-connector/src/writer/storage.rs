use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use common::protocol::StorageTarget;
use common::statistics::{StatisticsCollector, StatisticsConfig, schema_fingerprint};
use common::{Result, VeinError};
use exec::operators::sink::{WriteStorage, WriteTransaction};
use plan::VeinSchema;

use super::filesystem;
use super::options;
use super::schema::decode_schema;
use super::transaction::{ParquetWriteTransaction, ParquetWriteTransactionConfig};

/// Local filesystem-backed Parquet writer and managed manifest publisher.
#[derive(Debug, Clone)]
pub struct ParquetWriteStorage {
    pub(super) root: Arc<PathBuf>,
}

impl ParquetWriteStorage {
    /// Creates a writer rooted at a local data directory.
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: Arc::new(root.into()) }
    }
}

impl WriteStorage for ParquetWriteStorage {
    fn validate_target(&self, destination_uri: &str, options: &[(String, String)]) -> Result<()> {
        options::validate_target_parts(
            destination_uri,
            options.iter().map(|(key, value)| (key.as_str(), value.as_str())),
            false,
        )
            .map(|_| ())
    }

    fn begin_write(&self, target: &StorageTarget) -> Result<Box<dyn WriteTransaction>> {
        // Dispatch metadata is added after planning and is not a writer option.
        let (table, mode) = options::validate_target_parts(
            &target.destination_uri,
            target.options.iter().map(|option| (option.key.as_str(), option.value.as_str())),
            true,
        )?;
        let base_manifest = self.read_manifest(&table)?;
        validate_mode(&self.root, &table, &mode, base_manifest.is_some())?;

        let schema = decode_schema(&target.schema_ipc)?;
        let logical_schema = VeinSchema::try_from(schema.as_ref())?;
        let fingerprint = schema_fingerprint(&logical_schema);
        let statistics = self.statistics_for_write(&mode, base_manifest.as_ref(), &schema, &fingerprint)?;
        let generation = filesystem::next_generation()?;
        let staging_dir = self.root.join(".vein").join("staging").join(&table).join(&generation);
        fs::create_dir_all(&staging_dir).map_err(|error| {
            VeinError::io(format!("create staging directory '{}'", staging_dir.display()), error)
        })?;

        Ok(Box::new(ParquetWriteTransaction::new(
            ParquetWriteTransactionConfig {
                storage: self.clone(),
                table,
                mode,
                base_manifest,
                staging_dir,
                generation,
                schema,
                schema_fingerprint: fingerprint,
                statistics,
            },
        )))
    }
}

impl ParquetWriteStorage {
    fn statistics_for_write(
        &self,
        mode: &str,
        manifest: Option<&crate::catalog::manifest::TableManifest>,
        schema: &arrow::datatypes::SchemaRef,
        fingerprint: &str,
    ) -> Result<Option<StatisticsCollector>> {
        if mode == "append" {
            let prior = manifest.and_then(|manifest| {
                let analyzed = manifest.statistics.as_ref()?;
                let identities = self.source_identities(&manifest.files).ok()?;
                (analyzed.source_revision == manifest.revision
                    && analyzed.schema_fingerprint == fingerprint
                    && analyzed.source_identities == identities)
                    .then_some(analyzed)
            });
            Ok(prior.and_then(|analyzed| {
                StatisticsCollector::restore(
                    Arc::clone(schema), StatisticsConfig::default(), &analyzed.statistics,
                ).ok()
            }))
        } else {
            StatisticsCollector::new(Arc::clone(schema), StatisticsConfig::default()).map(Some)
        }
    }
}

fn validate_mode(root: &Path, table: &str, mode: &str, has_manifest: bool) -> Result<()> {
    match (mode, has_manifest) {
        ("create", true) => Err(VeinError::planning(
            format!("table '{table}' already exists; use mode='overwrite' or mode='append'"), None,
        )),
        ("append" | "overwrite", false) => Err(VeinError::not_found(
            format!("managed table '{table}' does not exist for {mode}"),
        )),
        ("create", false) if root.join(table).exists() => Err(VeinError::planning(
            format!("target '{}' already exists outside Vein's managed catalog", root.join(table).display()), None,
        )),
        _ => Ok(()),
    }
}
