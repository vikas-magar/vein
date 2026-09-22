use std::{
    fs::{self, File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use serde::{Deserialize, Serialize};

use crate::{
    CatalogView, DistributedError, MetadataCommand, MetadataStateMachine, Result,
};

const SNAPSHOT_FORMAT_VERSION: u32 = 1;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct SnapshotEnvelope {
    format_version: u32,
    state: CatalogView,
}

#[derive(Debug)]
pub struct PersistentMetadataStore {
    path: PathBuf,
    state_machine: Arc<Mutex<MetadataStateMachine>>,
}

impl PersistentMetadataStore {
    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }

        let state_machine = if path.exists() {
            let bytes = fs::read(&path)?;
            let envelope: SnapshotEnvelope = serde_json::from_slice(&bytes)?;

            if envelope.format_version != SNAPSHOT_FORMAT_VERSION {
                return Err(DistributedError::UnsupportedSnapshotFormat {
                    expected: SNAPSHOT_FORMAT_VERSION,
                    actual: envelope.format_version,
                });
            }

            MetadataStateMachine::from_state(envelope.state)?
        } else {
            MetadataStateMachine::new()
        };

        Ok(Self {
            path,
            state_machine: Arc::new(Mutex::new(state_machine)),
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    fn persist(&self, state: &CatalogView) -> Result<()> {
        let envelope = SnapshotEnvelope {
            format_version: SNAPSHOT_FORMAT_VERSION,
            state: state.clone(),
        };
        let bytes = serde_json::to_vec_pretty(&envelope)?;
        let temporary_path = self.temporary_path();

        let write_result = (|| -> Result<()> {
            let mut file = OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .open(&temporary_path)?;
            file.write_all(&bytes)?;
            file.sync_all()?;
            fs::rename(&temporary_path, &self.path)?;

            if let Some(parent) = self.path.parent() {
                File::open(parent)?.sync_all()?;
            }

            Ok(())
        })();

        if write_result.is_err() {
            let _ = fs::remove_file(&temporary_path);
        }

        write_result
    }

    fn temporary_path(&self) -> PathBuf {
        let file_name = self
            .path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("metadata");
        self.path
            .with_file_name(format!(".{file_name}.{}.tmp", std::process::id()))
    }
}

impl crate::MetadataStore for PersistentMetadataStore {
    fn read(&self) -> Result<CatalogView> {
        self.state_machine
            .lock()
            .map(|state| state.state().clone())
            .map_err(|_| DistributedError::StorePoisoned)
    }

    fn apply(&self, command: MetadataCommand) -> Result<crate::CommitReceipt> {
        let mut state = self
            .state_machine
            .lock()
            .map_err(|_| DistributedError::StorePoisoned)?;

        let mut candidate = state.clone();
        let receipt = candidate.apply(command)?;
        self.persist(candidate.state())?;
        *state = candidate;

        Ok(receipt)
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use tempfile::tempdir;

    use super::*;
    use crate::{
        ArtifactId, ArtifactRef, CatalogRevision, MetadataStore, SnapshotId, TableId,
        TableSnapshot, TransactionId,
    };

    fn command(transaction: &str, snapshot: &str, expected_revision: CatalogRevision) -> MetadataCommand {
        MetadataCommand::PublishTableSnapshot {
            transaction_id: TransactionId::from(transaction),
            table_id: TableId::from("orders"),
            expected_revision,
            snapshot: TableSnapshot {
                id: SnapshotId::from(snapshot),
                manifest: ArtifactRef {
                    id: ArtifactId::from(format!("manifest-{snapshot}")),
                    uri: format!("vfs://manifests/{snapshot}"),
                    checksum: format!("checksum-{snapshot}"),
                    size_bytes: 128,
                },
                schema_fingerprint: "schema-v1".to_owned(),
                metadata_payload: vec![1],
            },
        }
    }

    #[test]
    fn reopens_with_committed_snapshot() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("catalog.json");

        {
            let store = PersistentMetadataStore::open(&path).unwrap();
            store
                .apply(command("txn-1", "snapshot-1", CatalogRevision::INITIAL))
                .unwrap();
        }

        let reopened = PersistentMetadataStore::open(&path).unwrap();
        let view = reopened.read().unwrap();

        assert_eq!(view.revision, CatalogRevision::new(1));
        assert_eq!(
            view.table(&TableId::from("orders")).unwrap().latest.id,
            SnapshotId::from("snapshot-1")
        );
    }

    #[test]
    fn replays_idempotently_after_restart() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("catalog.json");

        let first = {
            let store = PersistentMetadataStore::open(&path).unwrap();
            store
                .apply(command("txn-1", "snapshot-1", CatalogRevision::INITIAL))
                .unwrap()
        };

        let reopened = PersistentMetadataStore::open(&path).unwrap();
        let replay = reopened
            .apply(command("txn-1", "snapshot-1", CatalogRevision::INITIAL))
            .unwrap();

        assert_eq!(replay, first);
        assert_eq!(reopened.read().unwrap().revision, CatalogRevision::new(1));
    }

    #[test]
    fn rejects_stale_revision_after_restart() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("catalog.json");

        {
            let store = PersistentMetadataStore::open(&path).unwrap();
            store
                .apply(command("txn-1", "snapshot-1", CatalogRevision::INITIAL))
                .unwrap();
        }

        let reopened = PersistentMetadataStore::open(&path).unwrap();
        let error = reopened
            .apply(command("txn-2", "snapshot-2", CatalogRevision::INITIAL))
            .unwrap_err();

        assert!(matches!(error, DistributedError::RevisionConflict { .. }));
    }

    #[test]
    fn failed_persistence_does_not_change_visible_state() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("catalog.json");
        let store = PersistentMetadataStore::open(&path).unwrap();
        fs::create_dir(&path).unwrap();

        let error = store
            .apply(command("txn-1", "snapshot-1", CatalogRevision::INITIAL))
            .unwrap_err();

        assert!(matches!(error, DistributedError::Io(_)));
        assert_eq!(store.read().unwrap().revision, CatalogRevision::INITIAL);
    }

    #[test]
    fn rejects_corrupt_snapshot() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("catalog.json");
        fs::write(&path, b"not-json").unwrap();

        let error = PersistentMetadataStore::open(&path).unwrap_err();

        assert!(matches!(error, DistributedError::Serialization(_)));
    }

    #[test]
    fn rejects_unknown_snapshot_format() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("catalog.json");
        let envelope = SnapshotEnvelope {
            format_version: SNAPSHOT_FORMAT_VERSION + 1,
            state: CatalogView::default(),
        };
        fs::write(&path, serde_json::to_vec(&envelope).unwrap()).unwrap();

        let error = PersistentMetadataStore::open(&path).unwrap_err();

        assert!(matches!(
            error,
            DistributedError::UnsupportedSnapshotFormat { .. }
        ));
    }

    #[test]
    fn rejects_inconsistent_snapshot_state() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("catalog.json");
        let mut state = CatalogView::default();
        state.tables.insert(
            TableId::from("orders"),
            crate::TableRecord {
                table_id: TableId::from("customers"),
                latest: TableSnapshot {
                    id: SnapshotId::from("snapshot-1"),
                    manifest: ArtifactRef {
                        id: ArtifactId::from("manifest-1"),
                        uri: "vfs://manifests/snapshot-1".to_owned(),
                        checksum: "checksum-1".to_owned(),
                        size_bytes: 128,
                    },
                    schema_fingerprint: "schema-v1".to_owned(),
                metadata_payload: vec![1],
                },
            },
        );
        let envelope = SnapshotEnvelope {
            format_version: SNAPSHOT_FORMAT_VERSION,
            state,
        };
        fs::write(&path, serde_json::to_vec(&envelope).unwrap()).unwrap();

        let error = PersistentMetadataStore::open(&path).unwrap_err();

        assert!(matches!(error, DistributedError::InvalidSnapshot(_)));
    }
}
