use common::protocol::{ExchangeState, SpillRunState};

use crate::{
    CatalogView, CommitReceipt, DistributedError, MetadataCommand, Result, SnapshotId, TableRecord,
    TableWriteState, exchange_key_id,
};

#[derive(Clone, Debug, Default)]
pub struct MetadataStateMachine {
    state: CatalogView,
}

impl MetadataStateMachine {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn state(&self) -> &CatalogView {
        &self.state
    }

    pub fn from_state(state: CatalogView) -> Result<Self> {
        for (table_id, record) in &state.tables {
            if table_id != &record.table_id {
                return Err(DistributedError::InvalidSnapshot(format!(
                    "table index '{}' does not match record '{}'",
                    table_id, record.table_id
                )));
            }
        }

        for (write_id, intent) in &state.writes {
            if write_id != &intent.write_id {
                return Err(DistributedError::InvalidSnapshot(format!(
                    "write index '{}' does not match intent '{}'",
                    write_id, intent.write_id
                )));
            }
            if intent.parts.iter().any(|(partition_id, part)| {
                *partition_id != part.partition_id || part.partition_id >= intent.expected_parts
            }) {
                return Err(DistributedError::InvalidSnapshot(format!(
                    "write '{}' contains an invalid partition descriptor",
                    write_id
                )));
            }
        }

        if state
            .commits
            .values()
            .any(|receipt| receipt.revision > state.revision)
        {
            return Err(DistributedError::InvalidSnapshot(
                "commit revision exceeds catalog revision".to_owned(),
            ));
        }

        Ok(Self { state })
    }

    pub fn apply(&mut self, command: MetadataCommand) -> Result<CommitReceipt> {
        let transaction_id = command.transaction_id().clone();
        let fingerprint = command.fingerprint();

        if let Some(previous) = self.state.commits.get(&transaction_id) {
            if previous.command_fingerprint == fingerprint {
                return Ok(previous.clone());
            }

            return Err(DistributedError::TransactionConflict { transaction_id });
        }

        if let Some(expected_revision) = command.expected_revision() {
            if expected_revision != self.state.revision {
                return Err(DistributedError::RevisionConflict {
                    expected: expected_revision,
                    actual: self.state.revision,
                });
            }
        }

        let revision = self.state.revision.next();
        let snapshot_id = SnapshotId::from(transaction_id.as_str());
        match command {
            MetadataCommand::PublishTableSnapshot {
                table_id, snapshot, ..
            } => {
                if snapshot.metadata_payload.is_empty() {
                    return Err(DistributedError::InvalidCommand(
                        "table snapshot is missing complete metadata".to_owned(),
                    ));
                }
                self.state.tables.insert(
                    table_id.clone(),
                    TableRecord {
                        table_id,
                        latest: snapshot,
                    },
                );
            }
            MetadataCommand::PrepareTableWrite {
                intent,
                expected_revision,
                ..
            } => {
                if intent.expected_revision != expected_revision
                    || intent.state != TableWriteState::Prepared
                    || intent.parts.len() != 0
                    || self.state.writes.contains_key(&intent.write_id)
                {
                    return Err(DistributedError::InvalidCommand(
                        "invalid or duplicate table write intent".to_owned(),
                    ));
                }
                if intent.expected_parts == 0 && intent.mode == crate::TableWriteMode::Append {
                    return Err(DistributedError::InvalidCommand(
                        "append write must publish at least one part".to_owned(),
                    ));
                }
                self.state.writes.insert(intent.write_id.clone(), intent);
            }
            MetadataCommand::PublishTableWritePart {
                write_id, part, ..
            } => {
                let intent = self.state.writes.get_mut(&write_id).ok_or_else(|| {
                    DistributedError::InvalidCommand("table write intent not found".to_owned())
                })?;
                if intent.state != TableWriteState::Prepared
                    || part.partition_id >= intent.expected_parts
                {
                    return Err(DistributedError::InvalidCommand(
                        "table write is not accepting this part".to_owned(),
                    ));
                }
                if let Some(existing) = intent.parts.get(&part.partition_id) {
                    if existing != &part {
                        return Err(DistributedError::InvalidCommand(
                            "table write partition was published with different metadata"
                                .to_owned(),
                        ));
                    }
                } else {
                    intent.parts.insert(part.partition_id, part);
                }
            }
            MetadataCommand::CommitTableWrite {
                write_id, snapshot, ..
            } => {
                let intent = self.state.writes.get_mut(&write_id).ok_or_else(|| {
                    DistributedError::InvalidCommand("table write intent not found".to_owned())
                })?;
                if intent.state != TableWriteState::Prepared || !intent.is_complete() {
                    return Err(DistributedError::InvalidCommand(
                        "table write is incomplete or not prepared".to_owned(),
                    ));
                }
                if snapshot.metadata_payload.is_empty() {
                    return Err(DistributedError::InvalidCommand(
                        "table write snapshot is missing complete metadata".to_owned(),
                    ));
                }
                let table_id = intent.table_id.clone();
                intent.state = TableWriteState::Committed;
                self.state.tables.insert(
                    table_id.clone(),
                    TableRecord {
                        table_id,
                        latest: snapshot,
                    },
                );
            }
            MetadataCommand::AbortTableWrite { write_id, .. } => {
                let intent = self.state.writes.get_mut(&write_id).ok_or_else(|| {
                    DistributedError::InvalidCommand("table write intent not found".to_owned())
                })?;
                if intent.state != TableWriteState::Prepared {
                    return Err(DistributedError::InvalidCommand(
                        "only prepared table writes can be aborted".to_owned(),
                    ));
                }
                intent.state = TableWriteState::Aborted;
            }
            MetadataCommand::CreateExchange { manifest, .. } => {
                let key = exchange_key_id(&manifest.key);
                if let Some(existing) = self.state.exchanges.get(&key) {
                    if existing.partition_count != manifest.partition_count
                        || existing.expected_tasks != manifest.expected_tasks
                        || existing.schema_fingerprint != manifest.schema_fingerprint
                    {
                        return Err(DistributedError::InvalidCommand(
                            "exchange already exists with incompatible metadata".to_owned(),
                        ));
                    }
                } else {
                    self.state.exchanges.insert(key, manifest);
                }
            }
            MetadataCommand::PublishExchangeFragment { key, fragment, .. } => {
                let manifest = self
                    .state
                    .exchanges
                    .get_mut(&exchange_key_id(&key))
                    .ok_or_else(|| {
                        DistributedError::InvalidCommand("exchange not found".to_owned())
                    })?;
                if manifest.state != ExchangeState::Open
                    || fragment.partition_id >= manifest.partition_count as u32
                    || !manifest.expected_tasks.contains(&fragment.task_id)
                {
                    return Err(DistributedError::InvalidCommand(
                        "invalid exchange fragment".to_owned(),
                    ));
                }
                if let Some(existing) = manifest.fragments.iter_mut().find(|existing| {
                    existing.task_id == fragment.task_id
                        && existing.partition_id == fragment.partition_id
                }) {
                    if fragment.attempt >= existing.attempt {
                        *existing = fragment;
                    }
                } else {
                    manifest.fragments.push(fragment);
                }
                if manifest.all_fragments_published() {
                    manifest.state = ExchangeState::Sealed;
                }
            }
            MetadataCommand::SealExchange { key, .. } => {
                let manifest = self
                    .state
                    .exchanges
                    .get_mut(&exchange_key_id(&key))
                    .ok_or_else(|| {
                        DistributedError::InvalidCommand("exchange not found".to_owned())
                    })?;
                if manifest.state == ExchangeState::Aborted {
                    return Err(DistributedError::InvalidCommand(
                        "exchange is aborted".to_owned(),
                    ));
                }
                if !manifest.all_tasks_published() {
                    return Err(DistributedError::InvalidCommand(
                        "exchange is missing producer fragments".to_owned(),
                    ));
                }
                manifest.state = ExchangeState::Sealed;
            }
            MetadataCommand::DeleteExchange { key, .. } => {
                self.state.exchanges.remove(&exchange_key_id(&key));
            }
            MetadataCommand::RegisterSpill { spill, .. } => {
                if spill.storage_path.is_empty() || spill.state != SpillRunState::Open {
                    return Err(DistributedError::InvalidCommand(
                        "spill must be registered open with a path".to_owned(),
                    ));
                }
                self.state.spills.insert(spill.storage_path.clone(), spill);
            }
            MetadataCommand::SetSpillState {
                storage_path,
                state,
                ..
            } => {
                let spill = self.state.spills.get_mut(&storage_path).ok_or_else(|| {
                    DistributedError::InvalidCommand("spill not found".to_owned())
                })?;
                if spill.state != SpillRunState::Open
                    || !matches!(state, SpillRunState::Sealed | SpillRunState::Aborted)
                {
                    return Err(DistributedError::InvalidCommand(
                        "invalid spill state transition".to_owned(),
                    ));
                }
                spill.state = state;
            }
            MetadataCommand::DeleteSpill { storage_path, .. } => {
                self.state.spills.remove(&storage_path);
            }
        }

        let receipt = CommitReceipt {
            transaction_id: transaction_id.clone(),
            revision,
            snapshot_id,
            command_fingerprint: fingerprint,
        };
        self.state.commits.insert(transaction_id, receipt.clone());
        self.state.revision = revision;
        Ok(receipt)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        ArtifactId, ArtifactRef, CatalogRevision, SnapshotId, TableId, TableSnapshot, TableWriteIntent,
        TableWriteMode, TableWritePart, TableWriteState, TransactionId, WriteId,
    };

    fn command(
        transaction: &str,
        snapshot: &str,
        expected_revision: CatalogRevision,
    ) -> MetadataCommand {
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

    fn write_intent(write_id: &str, expected_revision: CatalogRevision) -> TableWriteIntent {
        TableWriteIntent {
            write_id: WriteId::from(write_id),
            table_id: TableId::from("orders"),
            mode: TableWriteMode::Overwrite,
            expected_revision,
            generation: 1,
            expected_parts: 1,
            state: TableWriteState::Prepared,
            parts: Default::default(),
            base_snapshot_id: None,
        }
    }

    fn write_part() -> TableWritePart {
        TableWritePart {
            partition_id: 0,
            artifact: ArtifactRef {
                id: ArtifactId::from("part-0"),
                uri: "vfs://staged/part-0".to_owned(),
                checksum: "checksum-part-0".to_owned(),
                size_bytes: 64,
            },
            row_count: 10,
        }
    }

    fn write_snapshot() -> TableSnapshot {
        TableSnapshot {
            id: SnapshotId::from("snapshot-write-1"),
            manifest: ArtifactRef {
                id: ArtifactId::from("manifest-write-1"),
                uri: "vfs://manifests/write-1".to_owned(),
                checksum: "checksum-write-1".to_owned(),
                size_bytes: 128,
            },
            schema_fingerprint: "schema-v1".to_owned(),
            metadata_payload: vec![1, 2, 3],
        }
    }

    #[test]
    fn publishes_snapshot_at_next_revision() {
        let mut state = MetadataStateMachine::new();

        let receipt = state
            .apply(command("txn-1", "snapshot-1", CatalogRevision::INITIAL))
            .unwrap();

        assert_eq!(receipt.revision, CatalogRevision::new(1));
        assert_eq!(state.state().revision, CatalogRevision::new(1));
        assert_eq!(
            state
                .state()
                .table(&TableId::from("orders"))
                .unwrap()
                .latest
                .id,
            SnapshotId::from("snapshot-1")
        );
    }

    #[test]
    fn replays_identical_transaction_without_advancing_revision() {
        let mut state = MetadataStateMachine::new();
        let first = state
            .apply(command("txn-1", "snapshot-1", CatalogRevision::INITIAL))
            .unwrap();
        let replay = state
            .apply(command("txn-1", "snapshot-1", CatalogRevision::INITIAL))
            .unwrap();

        assert_eq!(replay, first);
        assert_eq!(state.state().revision, CatalogRevision::new(1));
    }

    #[test]
    fn rejects_transaction_reuse_with_different_payload() {
        let mut state = MetadataStateMachine::new();
        state
            .apply(command("txn-1", "snapshot-1", CatalogRevision::INITIAL))
            .unwrap();

        let error = state
            .apply(command("txn-1", "snapshot-2", CatalogRevision::INITIAL))
            .unwrap_err();

        assert!(matches!(
            error,
            DistributedError::TransactionConflict { .. }
        ));
    }

    #[test]
    fn rejects_stale_revision() {
        let mut state = MetadataStateMachine::new();
        state
            .apply(command("txn-1", "snapshot-1", CatalogRevision::INITIAL))
            .unwrap();

        let error = state
            .apply(command("txn-2", "snapshot-2", CatalogRevision::INITIAL))
            .unwrap_err();

        assert!(matches!(error, DistributedError::RevisionConflict { .. }));
    }

    #[test]
    fn commits_only_a_complete_write_generation() {
        let mut state = MetadataStateMachine::new();
        let intent = write_intent("write-1", CatalogRevision::INITIAL);
        state
            .apply(MetadataCommand::PrepareTableWrite {
                transaction_id: TransactionId::from("prepare-1"),
                expected_revision: CatalogRevision::INITIAL,
                intent,
            })
            .unwrap();

        let incomplete = state
            .apply(MetadataCommand::CommitTableWrite {
                transaction_id: TransactionId::from("commit-incomplete"),
                write_id: WriteId::from("write-1"),
                expected_revision: CatalogRevision::new(1),
                snapshot: write_snapshot(),
            })
            .unwrap_err();
        assert!(matches!(incomplete, DistributedError::InvalidCommand(_)));
        assert!(state.state().table(&TableId::from("orders")).is_none());

        state
            .apply(MetadataCommand::PublishTableWritePart {
                transaction_id: TransactionId::from("part-1"),
                write_id: WriteId::from("write-1"),
                part: write_part(),
            })
            .unwrap();
        state
            .apply(MetadataCommand::CommitTableWrite {
                transaction_id: TransactionId::from("commit-1"),
                write_id: WriteId::from("write-1"),
                expected_revision: CatalogRevision::new(2),
                snapshot: write_snapshot(),
            })
            .unwrap();

        assert_eq!(
            state
                .state()
                .table(&TableId::from("orders"))
                .unwrap()
                .latest
                .id,
            SnapshotId::from("snapshot-write-1")
        );
        assert_eq!(
            state.state().write(&WriteId::from("write-1")).unwrap().state,
            TableWriteState::Committed
        );
    }

    #[test]
    fn abort_keeps_staged_write_invisible() {
        let mut state = MetadataStateMachine::new();
        state
            .apply(MetadataCommand::PrepareTableWrite {
                transaction_id: TransactionId::from("prepare-2"),
                expected_revision: CatalogRevision::INITIAL,
                intent: write_intent("write-2", CatalogRevision::INITIAL),
            })
            .unwrap();
        state
            .apply(MetadataCommand::AbortTableWrite {
                transaction_id: TransactionId::from("abort-2"),
                write_id: WriteId::from("write-2"),
            })
            .unwrap();

        assert!(state.state().table(&TableId::from("orders")).is_none());
        assert_eq!(
            state.state().write(&WriteId::from("write-2")).unwrap().state,
            TableWriteState::Aborted
        );
    }
}
