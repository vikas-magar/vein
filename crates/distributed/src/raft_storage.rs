use std::{
    fs::{self, File, OpenOptions},
    io::{Cursor, Write},
    ops::{Bound, RangeBounds},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use openraft::{
    BasicNode, Entry, ErrorSubject, ErrorVerb, LogId, LogState, OptionalSend, StorageError,
    StoredMembership, Vote,
    entry::EntryPayload,
    storage::{RaftLogReader, RaftSnapshotBuilder, RaftStorage, Snapshot, SnapshotMeta},
};
use serde::{Deserialize, Serialize};

use crate::{CommitReceipt, MetadataStateMachine, Result};

use super::VeinRaftConfig;

const FORMAT_VERSION: u32 = 1;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct StoredSnapshot {
    meta: SnapshotMeta<u64, BasicNode>,
    data: Vec<u8>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct PersistedState {
    format_version: u32,
    vote: Option<Vote<u64>>,
    committed: Option<LogId<u64>>,
    purged: Option<LogId<u64>>,
    entries: Vec<Entry<VeinRaftConfig>>,
    applied: Option<LogId<u64>>,
    membership: StoredMembership<u64, BasicNode>,
    catalog: crate::CatalogView,
    snapshot: Option<StoredSnapshot>,
}

impl Default for PersistedState {
    fn default() -> Self {
        Self {
            format_version: FORMAT_VERSION,
            vote: None,
            committed: None,
            purged: None,
            entries: Vec::new(),
            applied: None,
            membership: StoredMembership::default(),
            catalog: crate::CatalogView::default(),
            snapshot: None,
        }
    }
}

#[derive(Clone)]
pub struct DurableRaftStorage {
    path: PathBuf,
    state: Arc<Mutex<PersistedState>>,
}

#[derive(Clone)]
pub struct DurableLogReader {
    state: Arc<Mutex<PersistedState>>,
}

#[derive(Clone)]
pub struct DurableSnapshotBuilder {
    path: PathBuf,
    state: Arc<Mutex<PersistedState>>,
}

fn persist_state(path: &Path, state: &PersistedState) -> Result<()> {
    let bytes = serde_json::to_vec(state)?;
    let temporary_path = path.with_extension("tmp");
    let result = (|| -> Result<()> {
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&temporary_path)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        fs::rename(&temporary_path, path)?;
        if let Some(parent) = path.parent() {
            File::open(parent)?.sync_all()?;
        }
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary_path);
    }
    result
}

impl DurableRaftStorage {
    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }

        let state = if path.exists() {
            let bytes = fs::read(&path)?;
            let state: PersistedState = serde_json::from_slice(&bytes)?;
            if state.format_version != FORMAT_VERSION {
                return Err(crate::DistributedError::UnsupportedSnapshotFormat {
                    expected: FORMAT_VERSION,
                    actual: state.format_version,
                });
            }
            validate_state(&state)?;
            state
        } else {
            PersistedState::default()
        };

        Ok(Self {
            path,
            state: Arc::new(Mutex::new(state)),
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn read_catalog(&self) -> Result<crate::CatalogView> {
        self.state
            .lock()
            .map(|state| state.catalog.clone())
            .map_err(|_| crate::DistributedError::StorePoisoned)
    }

    fn persist(&self, state: &PersistedState) -> Result<()> {
        persist_state(&self.path, state)
    }

    fn update(&self, update: impl FnOnce(&mut PersistedState) -> Result<()>) -> Result<()> {
        let mut guard = self
            .state
            .lock()
            .map_err(|_| crate::DistributedError::StorePoisoned)?;
        let mut candidate = guard.clone();
        update(&mut candidate)?;
        validate_state(&candidate)?;
        self.persist(&candidate)?;
        *guard = candidate;
        Ok(())
    }

    fn snapshot_from_state(state: &PersistedState) -> Option<Snapshot<VeinRaftConfig>> {
        state.snapshot.as_ref().map(|stored| Snapshot {
            meta: stored.meta.clone(),
            snapshot: Box::new(Cursor::new(stored.data.clone())),
        })
    }
}

fn validate_state(state: &PersistedState) -> Result<()> {
    let mut expected = state.purged.map(|id| id.index + 1).unwrap_or(0);
    for entry in &state.entries {
        if entry.log_id.index != expected {
            return Err(crate::DistributedError::InvalidSnapshot(format!(
                "raft log is not contiguous at index {}, expected {}",
                entry.log_id.index, expected
            )));
        }
        expected += 1;
    }
    if let (Some(applied), Some(committed)) = (&state.applied, &state.committed) {
        if applied > committed {
            return Err(crate::DistributedError::InvalidSnapshot(
                "applied log is ahead of committed log".to_owned(),
            ));
        }
    }
    Ok(())
}

fn storage_error(
    subject: ErrorSubject<u64>,
    verb: ErrorVerb,
    error: impl std::error::Error + Send + Sync + 'static,
) -> StorageError<u64> {
    StorageError::from_io_error(subject, verb, std::io::Error::other(error.to_string()))
}

fn storage_io(
    subject: ErrorSubject<u64>,
    verb: ErrorVerb,
    error: std::io::Error,
) -> StorageError<u64> {
    StorageError::from_io_error(subject, verb, error)
}

impl RaftLogReader<VeinRaftConfig> for DurableLogReader {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + std::fmt::Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> std::result::Result<Vec<Entry<VeinRaftConfig>>, StorageError<u64>> {
        let state = self.state.lock().map_err(|_| {
            storage_io(
                ErrorSubject::Logs,
                ErrorVerb::Read,
                std::io::Error::other("storage lock poisoned"),
            )
        })?;
        let start = match range.start_bound() {
            Bound::Included(value) => *value,
            Bound::Excluded(value) => value.saturating_add(1),
            Bound::Unbounded => 0,
        };
        let end = match range.end_bound() {
            Bound::Included(value) => value.saturating_add(1),
            Bound::Excluded(value) => *value,
            Bound::Unbounded => u64::MAX,
        };
        Ok(state
            .entries
            .iter()
            .filter(|entry| entry.log_id.index >= start && entry.log_id.index < end)
            .cloned()
            .collect())
    }
}

impl RaftLogReader<VeinRaftConfig> for DurableRaftStorage {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + std::fmt::Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> std::result::Result<Vec<Entry<VeinRaftConfig>>, StorageError<u64>> {
        let mut reader = DurableLogReader {
            state: Arc::clone(&self.state),
        };
        reader.try_get_log_entries(range).await
    }
}

impl RaftStorage<VeinRaftConfig> for DurableRaftStorage {
    type LogReader = DurableLogReader;
    type SnapshotBuilder = DurableSnapshotBuilder;

    async fn save_vote(&mut self, vote: &Vote<u64>) -> std::result::Result<(), StorageError<u64>> {
        self.update(|state| {
            state.vote = Some(vote.clone());
            Ok(())
        })
        .map_err(|error| {
            storage_error(
                ErrorSubject::Vote,
                ErrorVerb::Write,
                std::io::Error::other(error.to_string()),
            )
        })
    }

    async fn read_vote(&mut self) -> std::result::Result<Option<Vote<u64>>, StorageError<u64>> {
        self.state
            .lock()
            .map(|state| state.vote.clone())
            .map_err(|_| {
                storage_io(
                    ErrorSubject::Vote,
                    ErrorVerb::Read,
                    std::io::Error::other("storage lock poisoned"),
                )
            })
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<u64>>,
    ) -> std::result::Result<(), StorageError<u64>> {
        self.update(|state| {
            state.committed = committed;
            Ok(())
        })
        .map_err(|error| {
            storage_error(
                ErrorSubject::Store,
                ErrorVerb::Write,
                std::io::Error::other(error.to_string()),
            )
        })
    }

    async fn read_committed(
        &mut self,
    ) -> std::result::Result<Option<LogId<u64>>, StorageError<u64>> {
        self.state
            .lock()
            .map(|state| state.committed.clone())
            .map_err(|_| {
                storage_io(
                    ErrorSubject::Store,
                    ErrorVerb::Read,
                    std::io::Error::other("storage lock poisoned"),
                )
            })
    }

    async fn get_log_state(
        &mut self,
    ) -> std::result::Result<LogState<VeinRaftConfig>, StorageError<u64>> {
        self.state
            .lock()
            .map(|state| LogState {
                last_purged_log_id: state.purged.clone(),
                last_log_id: state
                    .entries
                    .last()
                    .map(|entry| entry.log_id.clone())
                    .or_else(|| state.purged.clone()),
            })
            .map_err(|_| {
                storage_io(
                    ErrorSubject::Logs,
                    ErrorVerb::Read,
                    std::io::Error::other("storage lock poisoned"),
                )
            })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        DurableLogReader {
            state: Arc::clone(&self.state),
        }
    }

    async fn append_to_log<I>(&mut self, entries: I) -> std::result::Result<(), StorageError<u64>>
    where
        I: IntoIterator<Item = Entry<VeinRaftConfig>> + OptionalSend,
    {
        let entries = entries.into_iter().collect::<Vec<_>>();
        self.update(|state| {
            if entries.is_empty() {
                return Ok(());
            }
            let first = entries[0].log_id.index;
            let retained_start = state.purged.map(|id| id.index + 1).unwrap_or(0);
            if first < retained_start {
                return Err(crate::DistributedError::InvalidSnapshot(
                    "append precedes purged log".to_owned(),
                ));
            }
            state.entries.retain(|entry| entry.log_id.index < first);
            let mut expected = state
                .entries
                .last()
                .map(|entry| entry.log_id.index + 1)
                .or(state.purged.map(|id| id.index + 1))
                .unwrap_or(0);
            for entry in entries {
                if entry.log_id.index != expected {
                    return Err(crate::DistributedError::InvalidSnapshot(format!(
                        "append has a hole at {}",
                        entry.log_id.index
                    )));
                }
                state.entries.push(entry);
                expected += 1;
            }
            Ok(())
        })
        .map_err(|error| {
            storage_error(
                ErrorSubject::Logs,
                ErrorVerb::Write,
                std::io::Error::other(error.to_string()),
            )
        })
    }

    async fn delete_conflict_logs_since(
        &mut self,
        log_id: LogId<u64>,
    ) -> std::result::Result<(), StorageError<u64>> {
        self.update(|state| {
            if state
                .applied
                .as_ref()
                .is_some_and(|applied| *applied >= log_id)
            {
                return Err(crate::DistributedError::InvalidSnapshot(
                    "cannot delete an applied log".to_owned(),
                ));
            }
            state.entries.retain(|entry| entry.log_id < log_id);
            Ok(())
        })
        .map_err(|error| {
            storage_error(
                ErrorSubject::Logs,
                ErrorVerb::Delete,
                std::io::Error::other(error.to_string()),
            )
        })
    }

    async fn purge_logs_upto(
        &mut self,
        log_id: LogId<u64>,
    ) -> std::result::Result<(), StorageError<u64>> {
        self.update(|state| {
            if state
                .applied
                .as_ref()
                .is_none_or(|applied| *applied < log_id)
            {
                return Err(crate::DistributedError::InvalidSnapshot(
                    "cannot purge an unapplied log".to_owned(),
                ));
            }
            state.entries.retain(|entry| entry.log_id > log_id);
            state.purged = Some(log_id);
            Ok(())
        })
        .map_err(|error| {
            storage_error(
                ErrorSubject::Logs,
                ErrorVerb::Delete,
                std::io::Error::other(error.to_string()),
            )
        })
    }

    async fn last_applied_state(
        &mut self,
    ) -> std::result::Result<
        (Option<LogId<u64>>, StoredMembership<u64, BasicNode>),
        StorageError<u64>,
    > {
        self.state
            .lock()
            .map(|state| (state.applied.clone(), state.membership.clone()))
            .map_err(|_| {
                storage_io(
                    ErrorSubject::StateMachine,
                    ErrorVerb::Read,
                    std::io::Error::other("storage lock poisoned"),
                )
            })
    }

    async fn apply_to_state_machine(
        &mut self,
        entries: &[Entry<VeinRaftConfig>],
    ) -> std::result::Result<Vec<CommitReceipt>, StorageError<u64>> {
        let mut results = Vec::new();
        self.update(|state| {
            let mut machine = MetadataStateMachine::from_state(state.catalog.clone())
                .map_err(|error| crate::DistributedError::InvalidSnapshot(error.to_string()))?;
            for entry in entries {
                match &entry.payload {
                    EntryPayload::Normal(command) => {
                        results.push(machine.apply(command.clone()).map_err(|error| {
                            crate::DistributedError::InvalidSnapshot(error.to_string())
                        })?)
                    }
                    EntryPayload::Membership(membership) => {
                        state.membership =
                            StoredMembership::new(Some(entry.log_id.clone()), membership.clone())
                    }
                    EntryPayload::Blank => {}
                }
                state.applied = Some(entry.log_id.clone());
            }
            state.catalog = machine.state().clone();
            Ok(())
        })
        .map_err(|error| {
            storage_error(
                ErrorSubject::StateMachine,
                ErrorVerb::Write,
                std::io::Error::other(error.to_string()),
            )
        })?;
        Ok(results)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        DurableSnapshotBuilder {
            path: self.path.clone(),
            state: Arc::clone(&self.state),
        }
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> std::result::Result<Box<Cursor<Vec<u8>>>, StorageError<u64>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<u64, BasicNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> std::result::Result<(), StorageError<u64>> {
        let data = snapshot.into_inner();
        let decoded: crate::CatalogView = serde_json::from_slice(&data).map_err(|error| {
            storage_error(
                ErrorSubject::Snapshot(None),
                ErrorVerb::Read,
                std::io::Error::other(error.to_string()),
            )
        })?;
        MetadataStateMachine::from_state(decoded.clone()).map_err(|error| {
            storage_error(
                ErrorSubject::Snapshot(None),
                ErrorVerb::Read,
                std::io::Error::other(error.to_string()),
            )
        })?;
        self.update(|state| {
            state.catalog = decoded;
            state.snapshot = Some(StoredSnapshot {
                meta: meta.clone(),
                data,
            });
            state.applied = meta.last_log_id.clone();
            state.purged = meta.last_log_id.clone();
            state.entries.retain(|entry| {
                meta.last_log_id
                    .as_ref()
                    .is_none_or(|last| entry.log_id > *last)
            });
            Ok(())
        })
        .map_err(|error| {
            storage_error(
                ErrorSubject::Snapshot(None),
                ErrorVerb::Write,
                std::io::Error::other(error.to_string()),
            )
        })
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> std::result::Result<Option<Snapshot<VeinRaftConfig>>, StorageError<u64>> {
        self.state
            .lock()
            .map(|state| Self::snapshot_from_state(&state))
            .map_err(|_| {
                storage_io(
                    ErrorSubject::Snapshot(None),
                    ErrorVerb::Read,
                    std::io::Error::other("storage lock poisoned"),
                )
            })
    }
}

impl RaftSnapshotBuilder<VeinRaftConfig> for DurableSnapshotBuilder {
    async fn build_snapshot(
        &mut self,
    ) -> std::result::Result<Snapshot<VeinRaftConfig>, StorageError<u64>> {
        let mut guard = self.state.lock().map_err(|_| {
            storage_io(
                ErrorSubject::Snapshot(None),
                ErrorVerb::Read,
                std::io::Error::other("storage lock poisoned"),
            )
        })?;
        let data = serde_json::to_vec(&guard.catalog).map_err(|error| {
            storage_error(
                ErrorSubject::Snapshot(None),
                ErrorVerb::Write,
                std::io::Error::other(error.to_string()),
            )
        })?;
        let last_log_id = guard.applied.clone();
        let meta = SnapshotMeta {
            last_log_id: last_log_id.clone(),
            last_membership: guard.membership.clone(),
            snapshot_id: format!(
                "vein-{}",
                last_log_id.as_ref().map(|id| id.index).unwrap_or(0)
            ),
        };
        let mut candidate = guard.clone();
        candidate.snapshot = Some(StoredSnapshot {
            meta: meta.clone(),
            data: data.clone(),
        });
        persist_state(&self.path, &candidate).map_err(|error| {
            storage_error(
                ErrorSubject::Snapshot(None),
                ErrorVerb::Write,
                std::io::Error::other(error.to_string()),
            )
        })?;
        *guard = candidate;
        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;
    use crate::MetadataCommand;
    use openraft::{
        LeaderId,
        entry::{FromAppData, RaftEntry},
    };

    #[tokio::test]
    async fn persists_vote_and_log_across_restart() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("raft.json");
        let mut store = DurableRaftStorage::open(&path).unwrap();
        let vote = Vote::new(1, 7);
        store.save_vote(&vote).await.unwrap();
        let entry = Entry::<VeinRaftConfig>::new_blank(LogId::new(LeaderId::new(1, 7), 0));
        store.append_to_log(vec![entry]).await.unwrap();
        drop(store);

        let mut reopened = DurableRaftStorage::open(&path).unwrap();
        assert_eq!(reopened.read_vote().await.unwrap(), Some(vote));
        assert_eq!(
            reopened
                .get_log_state()
                .await
                .unwrap()
                .last_log_id
                .unwrap()
                .index,
            0
        );
    }

    #[tokio::test]
    async fn snapshot_round_trip_is_validated() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("raft.json");
        let mut store = DurableRaftStorage::open(&path).unwrap();
        let command = MetadataCommand::PublishTableSnapshot {
            transaction_id: crate::TransactionId::from("txn"),
            table_id: crate::TableId::from("table"),
            expected_revision: crate::CatalogRevision::INITIAL,
            snapshot: crate::TableSnapshot {
                id: crate::SnapshotId::from("snapshot"),
                manifest: crate::ArtifactRef {
                    id: crate::ArtifactId::from("manifest"),
                    uri: "vfs://manifest".to_owned(),
                    checksum: "checksum".to_owned(),
                    size_bytes: 1,
                },
                schema_fingerprint: "schema".to_owned(),
                metadata_payload: Vec::new(),
            },
        };
        let entry = Entry::<VeinRaftConfig>::from_app_data(command);
        let mut entry = entry;
        entry.log_id = LogId::new(LeaderId::new(1, 7), 0);
        store.append_to_log(vec![entry.clone()]).await.unwrap();
        store.apply_to_state_machine(&[entry]).await.unwrap();
        let mut builder = store.get_snapshot_builder().await;
        let snapshot = builder.build_snapshot().await.unwrap();
        let bytes = snapshot.snapshot.into_inner();
        assert!(serde_json::from_slice::<crate::CatalogView>(&bytes).is_ok());
    }
}
