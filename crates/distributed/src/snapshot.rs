use std::collections::BTreeMap;

use common::protocol::{ExchangeKey, ExchangeManifest, SpillRun};
use serde::{Deserialize, Serialize};

use crate::{
    ArtifactId, CatalogRevision, SnapshotId, TableId, TableWriteIntent, TransactionId, WriteId,
};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ArtifactRef {
    pub id: ArtifactId,
    pub uri: String,
    pub checksum: String,
    pub size_bytes: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TableSnapshot {
    pub id: SnapshotId,
    pub table_name: String,
    pub schema_version: u64,
    pub manifest: ArtifactRef,
    pub schema_fingerprint: String,
    /// Canonical serialized complete table metadata owned by VCS.
    pub metadata_payload: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TableRecord {
    pub table_id: TableId,
    pub latest: TableSnapshot,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CommitReceipt {
    pub transaction_id: TransactionId,
    pub revision: CatalogRevision,
    pub snapshot_id: SnapshotId,
    pub command_fingerprint: String,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct CatalogView {
    pub revision: CatalogRevision,
    pub tables: BTreeMap<TableId, TableRecord>,
    pub table_names: BTreeMap<String, TableId>,
    pub exchanges: BTreeMap<String, ExchangeManifest>,
    pub spills: BTreeMap<String, SpillRun>,
    pub writes: BTreeMap<WriteId, TableWriteIntent>,
    pub commits: BTreeMap<TransactionId, CommitReceipt>,
}

pub fn exchange_key_id(key: &ExchangeKey) -> String {
    serde_json::to_string(key).expect("exchange keys must be serializable")
}

impl CatalogView {
    pub fn table(&self, table_id: &TableId) -> Option<&TableRecord> {
        self.tables.get(table_id)
    }

    pub fn exchange(&self, key: &ExchangeKey) -> Option<&ExchangeManifest> {
        self.exchanges.get(&exchange_key_id(key))
    }

    pub fn spill(&self, storage_path: &str) -> Option<&SpillRun> {
        self.spills.get(storage_path)
    }

    pub fn write(&self, write_id: &WriteId) -> Option<&TableWriteIntent> {
        self.writes.get(write_id)
    }
}
