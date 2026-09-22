//! Durable, revisioned table metadata state owned by VCS.

use std::collections::BTreeMap;
use std::sync::Arc;

use bytes::Bytes;
use iceberg::io::FileIO;
use plan::{SortKey, TableFormat, TableMetadata, TableStatistics, TableType, VeinSchema};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::model::CatalogRevision;
use error::{Result, VeinError};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedTable {
    name: String,
    schema: VeinSchema,
    table_type: String,
    location: Option<String>,
    format: Option<String>,
    files: Vec<String>,
    partition_columns: Vec<String>,
    sort_order: Vec<SortKey>,
    statistics: Option<TableStatistics>,
    properties: BTreeMap<String, String>,
    comment: Option<String>,
}

impl From<&TableMetadata> for PersistedTable {
    fn from(metadata: &TableMetadata) -> Self {
        Self {
            name: metadata.name.clone(),
            schema: (*metadata.schema).clone(),
            table_type: match metadata.table_type {
                TableType::Base => "base",
                TableType::View => "view",
                TableType::Temporary => "temporary",
                TableType::External => "external",
            }
            .to_string(),
            location: metadata.location.clone(),
            format: metadata.format.as_ref().map(ToString::to_string),
            files: metadata.files.clone(),
            partition_columns: metadata.partition_columns.clone(),
            sort_order: metadata.sort_order.clone(),
            statistics: metadata.statistics.clone(),
            properties: metadata
                .properties
                .iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect(),
            comment: metadata.comment.clone(),
        }
    }
}

impl PersistedTable {
    fn into_metadata(self) -> TableMetadata {
        let table_type = match self.table_type.as_str() {
            "view" => TableType::View,
            "temporary" => TableType::Temporary,
            "external" => TableType::External,
            _ => TableType::Base,
        };
        let mut metadata = TableMetadata::new(self.name, Arc::new(self.schema))
            .with_table_type(table_type)
            .with_files(self.files)
            .with_partition_columns(self.partition_columns)
            .with_sort_order(self.sort_order);
        if let Some(statistics) = self.statistics {
            metadata = metadata.with_statistics(statistics);
        }
        if let Some(location) = self.location {
            metadata = metadata.with_location(location);
        }
        if let Some(format) = self.format {
            metadata = metadata.with_format(TableFormat::from(format));
        }
        for (key, value) in self.properties {
            metadata = metadata.with_property(key, value);
        }
        if let Some(comment) = self.comment {
            metadata = metadata.with_comment(comment);
        }
        metadata
    }
}

const TABLE_METADATA_PAYLOAD_VERSION: u16 = 1;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TableMetadataPayload {
    version: u16,
    metadata: PersistedTable,
}

/// Encodes the complete authoritative table metadata payload for replication.
pub fn encode_table_metadata(metadata: &TableMetadata) -> Result<Vec<u8>> {
    serde_json::to_vec(&TableMetadataPayload {
        version: TABLE_METADATA_PAYLOAD_VERSION,
        metadata: PersistedTable::from(metadata),
    })
    .map_err(|error| {
        VeinError::serialization(format!("failed to encode table metadata payload: {error}"))
    })
}

/// Decodes a complete authoritative table metadata payload from replication.
pub fn decode_table_metadata(payload: &[u8]) -> Result<TableMetadata> {
    let envelope: TableMetadataPayload = serde_json::from_slice(payload).map_err(|error| {
        VeinError::serialization(format!("invalid table metadata payload: {error}"))
    })?;
    if envelope.version != TABLE_METADATA_PAYLOAD_VERSION {
        return Err(VeinError::serialization(format!(
            "unsupported table metadata payload version {}",
            envelope.version
        )));
    }
    Ok(envelope.metadata.into_metadata())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CatalogStateSnapshot {
    revision: u64,
    tables: BTreeMap<String, PersistedTable>,
}

/// Durable table metadata state with an in-memory read snapshot.
pub struct PersistentCatalogState {
    file_io: FileIO,
    path: String,
    snapshot: RwLock<CatalogStateSnapshot>,
}

impl PersistentCatalogState {
    /// Opens and restores the catalog state at a storage path.
    pub async fn open(file_io: FileIO, path: impl Into<String>) -> Result<Arc<Self>> {
        let path = path.into();
        ensure_local_parent(&path)?;
        let input = file_io.new_input(&path).map_err(VeinError::Iceberg)?;
        let snapshot = if input.exists().await.map_err(VeinError::Iceberg)? {
            let bytes = input.read().await.map_err(VeinError::Iceberg)?;
            serde_json::from_slice(&bytes).map_err(|error| {
                VeinError::serialization(format!("invalid VCS catalog state: {error}"))
            })?
        } else {
            CatalogStateSnapshot {
                revision: 1,
                tables: BTreeMap::new(),
            }
        };

        Ok(Arc::new(Self {
            file_io,
            path,
            snapshot: RwLock::new(snapshot),
        }))
    }

    /// Returns the current committed revision.
    pub async fn revision(&self) -> CatalogRevision {
        CatalogRevision::new(self.snapshot.read().await.revision.to_string())
    }

    /// Resolves a table by name, base name, or registered location.
    pub async fn get(&self, name: &str) -> Option<Arc<TableMetadata>> {
        let snapshot = self.snapshot.read().await;
        let base_name = std::path::Path::new(name)
            .file_name()
            .and_then(|value| value.to_str())
            .unwrap_or(name);
        snapshot
            .tables
            .get(name)
            .or_else(|| snapshot.tables.get(base_name))
            .or_else(|| {
                snapshot.tables.values().find(|table| {
                    table.location.as_deref() == Some(name)
                        || table.location.as_deref() == Some(base_name)
                })
            })
            .cloned()
            .map(|table| Arc::new(table.into_metadata()))
    }

    /// Publishes a complete next-generation table state if the revision is current.
    pub async fn commit_table(
        &self,
        expected_revision: &CatalogRevision,
        metadata: &TableMetadata,
    ) -> Result<CatalogRevision> {
        let mut current = self.snapshot.write().await;
        let actual_revision = current.revision.to_string();
        if expected_revision.as_str() != actual_revision {
            return Err(VeinError::internal(format!(
                "catalog revision conflict: expected '{}', actual '{}'",
                expected_revision.as_str(),
                actual_revision
            )));
        }
        let mut next = current.clone();
        next.revision = next.revision.saturating_add(1);
        next.tables
            .insert(metadata.name.clone(), PersistedTable::from(metadata));
        let bytes = serde_json::to_vec(&next).map_err(|error| {
            VeinError::serialization(format!("failed to encode VCS catalog state: {error}"))
        })?;

        let output = self
            .file_io
            .new_output(&self.path)
            .map_err(VeinError::Iceberg)?;
        output
            .write(Bytes::from(bytes))
            .await
            .map_err(VeinError::Iceberg)?;

        *current = next;
        Ok(CatalogRevision::new(current.revision.to_string()))
    }
}

fn ensure_local_parent(path: &str) -> Result<()> {
    let Some(local_path) = path.strip_prefix("file://") else {
        return Ok(());
    };
    let Some(parent) = std::path::Path::new(local_path).parent() else {
        return Ok(());
    };
    std::fs::create_dir_all(parent).map_err(|error| {
        VeinError::io(
            format!(
                "failed to create VCS catalog directory '{}'",
                parent.display()
            ),
            error,
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use plan::{ColumnId, ResolvedColumn, VeinDataType, VeinSchema};

    #[tokio::test]
    async fn state_reopens_with_committed_table_and_revision() {
        let directory = tempfile::tempdir().expect("temporary catalog directory");
        let path = format!(
            "file://{}/_vein/catalog/tables.json",
            directory.path().display()
        );
        let metadata = TableMetadata::new(
            "orders",
            Arc::new(VeinSchema::new(vec![ResolvedColumn::new(
                ColumnId(0),
                "id",
                None,
                VeinDataType::Int64,
            )])),
        )
        .with_location("vfs://orders")
        .with_format(TableFormat::Exo);

        let state = PersistentCatalogState::open(FileIO::new_with_fs(), path.clone())
            .await
            .expect("state should open");
        let revision = state
            .commit_table(&CatalogRevision::new("1"), &metadata)
            .await
            .expect("commit should work");
        assert_eq!(revision.as_str(), "2");
        drop(state);

        let reopened = PersistentCatalogState::open(FileIO::new_with_fs(), path)
            .await
            .expect("state should reopen");
        assert_eq!(reopened.revision().await.as_str(), "2");
        let restored = reopened.get("orders").await.expect("table should restore");
        assert_eq!(restored.location.as_deref(), Some("vfs://orders"));
        assert_eq!(restored.format, Some(TableFormat::Exo));
        assert_eq!(restored.schema.fields[0].data_type, VeinDataType::Int64);
    }

    #[test]
    fn metadata_payload_round_trips_complete_planner_metadata() {
        use plan::{ColumnStatistics, LogicalExpr, LogicalLiteral, SortKey, TableStatistics};
        use std::collections::HashMap;

        let schema = Arc::new(VeinSchema::new(vec![ResolvedColumn::new(
            ColumnId(0),
            "id",
            Some("orders".to_owned()),
            VeinDataType::Int64,
        )]));
        let mut column_statistics = HashMap::new();
        column_statistics.insert(
            "id".to_owned(),
            ColumnStatistics {
                null_count: Some(0),
                distinct_count: Some(100),
                min_value: Some(LogicalLiteral::Int64(1)),
                max_value: Some(LogicalLiteral::Int64(100)),
            },
        );
        let metadata = TableMetadata::new("orders", schema)
            .with_table_type(TableType::External)
            .with_location("s3://warehouse/orders")
            .with_format(TableFormat::Parquet)
            .with_files(vec!["s3://warehouse/orders/part-0.parquet".to_owned()])
            .with_partition_columns(vec!["id".to_owned()])
            .with_sort_order(vec![SortKey {
                expr: LogicalExpr::Column(ResolvedColumn::new(
                    ColumnId(0),
                    "id",
                    Some("orders".to_owned()),
                    VeinDataType::Int64,
                )),
                asc: true,
                nulls_first: false,
            }])
            .with_statistics(TableStatistics {
                num_rows: Some(100),
                total_bytes: Some(4096),
                column_statistics,
            })
            .with_property("compression", "zstd")
            .with_comment("authoritative metadata");

        let payload = encode_table_metadata(&metadata).expect("metadata should encode");
        let restored = decode_table_metadata(&payload).expect("metadata should decode");
        assert_eq!(restored, metadata);
    }

    #[tokio::test]
    async fn stale_revision_cannot_replace_visible_state() {
        let directory = tempfile::tempdir().expect("temporary catalog directory");
        let path = format!("file://{}/catalog.json", directory.path().display());
        let state = PersistentCatalogState::open(FileIO::new_with_fs(), path)
            .await
            .expect("state should open");
        let first = TableMetadata::new("first", Arc::new(VeinSchema::empty()));
        let second = TableMetadata::new("second", Arc::new(VeinSchema::empty()));

        state
            .commit_table(&CatalogRevision::new("1"), &first)
            .await
            .expect("first commit should work");
        let error = state
            .commit_table(&CatalogRevision::new("1"), &second)
            .await
            .expect_err("stale commit must fail");
        assert!(error.to_string().contains("catalog revision conflict"));
        assert!(state.get("first").await.is_some());
        assert!(state.get("second").await.is_none());
        assert_eq!(state.revision().await.as_str(), "2");
    }
}
