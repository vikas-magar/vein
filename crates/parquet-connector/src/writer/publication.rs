use std::fs;
use std::path::PathBuf;

use crate::catalog::manifest::{ExternalStatisticsSidecar, TableManifest, external_statistics_path, table_manifest_path};
use crate::catalog::statistics_source_identity;
use catalog::{AnalyzedTableStatistics, StatisticsSourceIdentity};
use common::{Result, VeinError};
use storage::{StorageObjectVersion, StorageTimestamp};

use super::ParquetWriteStorage;
use super::filesystem;

impl ParquetWriteStorage {
    pub(super) fn manifest_path(&self, table: &str) -> Result<PathBuf> {
        Ok(self.root.join(table_manifest_path(table)?.as_str()))
    }

    pub(super) fn read_manifest(&self, table: &str) -> Result<Option<TableManifest>> {
        let path = self.manifest_path(table)?;
        match fs::read(&path) {
            Ok(bytes) => serde_json::from_slice(&bytes).map(Some).map_err(|error| {
                VeinError::internal(format!("invalid table manifest '{}': {error}", path.display()))
            }),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(VeinError::io(format!("read table manifest '{}'", path.display()), error)),
        }
    }

    pub(super) fn source_identities(&self, files: &[String]) -> Result<Vec<StatisticsSourceIdentity>> {
        let mut identities = files.iter().map(|file| self.source_identity(file)).collect::<Result<Vec<_>>>()?;
        identities.sort_by(|left, right| left.identifier.cmp(&right.identifier));
        Ok(identities)
    }

    pub(super) fn source_identity(&self, relative_path: &str) -> Result<StatisticsSourceIdentity> {
        let path = filesystem::resolve_manifest_file(&self.root, relative_path)?;
        let metadata = fs::metadata(&path)
            .map_err(|error| VeinError::io(format!("read source identity for '{}'", path.display()), error))?;
        let version = StorageObjectVersion {
            last_modified: metadata.modified().ok().and_then(StorageTimestamp::from_system_time),
            ..StorageObjectVersion::default()
        };
        Ok(statistics_source_identity(relative_path.to_owned(), metadata.len(), &version))
    }

    pub(super) fn publish_external_statistics(
        &self,
        table_name: &str,
        statistics: &AnalyzedTableStatistics,
    ) -> Result<()> {
        let sidecar = ExternalStatisticsSidecar {
            table_name: table_name.to_owned(),
            statistics: statistics.clone(),
        };
        let path = self.root.join(external_statistics_path(table_name).as_str());
        filesystem::publish_json_atomic(&path, &sidecar, &format!(".statistics-{}.tmp", filesystem::next_generation()?))
    }

    pub(super) fn publish_managed_statistics(
        &self,
        table_name: &str,
        statistics: &AnalyzedTableStatistics,
    ) -> Result<()> {
        let _guard = filesystem::table_commit_lock(&self.root, table_name)?;
        let mut manifest = self.read_manifest(table_name)?.ok_or_else(|| {
            VeinError::not_found(format!("managed table '{table_name}' no longer exists"))
        })?;
        if manifest.revision != statistics.source_revision
            || self.source_identities(&manifest.files)? != statistics.source_identities
        {
            return Err(VeinError::planning(
                format!("table '{table_name}' changed while ANALYZE was running; retry against the new snapshot"),
                None,
            ));
        }
        manifest.statistics = Some(statistics.clone());
        filesystem::publish_manifest(&self.manifest_path(table_name)?, &manifest, &filesystem::next_generation()?)
    }
}
