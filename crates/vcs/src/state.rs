//! Complete table metadata payload codec owned by VCS.

use std::collections::BTreeMap;
use std::sync::Arc;

use plan::{SortKey, TableFormat, TableMetadata, TableStatistics, TableType, VeinSchema};
use serde::{Deserialize, Serialize};

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
