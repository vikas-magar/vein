use common::protocol::{ExchangeFragment, ExchangeKey, ExchangeManifest, SpillRun, SpillRunState};
use serde::{Deserialize, Serialize};

use crate::{
    CatalogRevision, TableId, TableSnapshot, TableWriteIntent, TableWritePart, TransactionId,
    WriteId,
};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum MetadataCommand {
    PublishTableSnapshot {
        transaction_id: TransactionId,
        table_id: TableId,
        expected_revision: CatalogRevision,
        snapshot: TableSnapshot,
    },
    PrepareTableWrite {
        transaction_id: TransactionId,
        expected_revision: CatalogRevision,
        intent: TableWriteIntent,
    },
    PublishTableWritePart {
        transaction_id: TransactionId,
        write_id: WriteId,
        part: TableWritePart,
    },
    CommitTableWrite {
        transaction_id: TransactionId,
        write_id: WriteId,
        expected_revision: CatalogRevision,
        snapshot: TableSnapshot,
    },
    AbortTableWrite {
        transaction_id: TransactionId,
        write_id: WriteId,
    },
    CreateExchange {
        transaction_id: TransactionId,
        manifest: ExchangeManifest,
    },
    PublishExchangeFragment {
        transaction_id: TransactionId,
        key: ExchangeKey,
        fragment: ExchangeFragment,
    },
    SealExchange {
        transaction_id: TransactionId,
        key: ExchangeKey,
    },
    DeleteExchange {
        transaction_id: TransactionId,
        key: ExchangeKey,
    },
    RegisterSpill {
        transaction_id: TransactionId,
        spill: SpillRun,
    },
    SetSpillState {
        transaction_id: TransactionId,
        storage_path: String,
        state: SpillRunState,
    },
    DeleteSpill {
        transaction_id: TransactionId,
        storage_path: String,
    },
}

impl MetadataCommand {
    pub fn transaction_id(&self) -> &TransactionId {
        match self {
            Self::PublishTableSnapshot { transaction_id, .. }
            | Self::PrepareTableWrite { transaction_id, .. }
            | Self::PublishTableWritePart { transaction_id, .. }
            | Self::CommitTableWrite { transaction_id, .. }
            | Self::AbortTableWrite { transaction_id, .. }
            | Self::CreateExchange { transaction_id, .. }
            | Self::PublishExchangeFragment { transaction_id, .. }
            | Self::SealExchange { transaction_id, .. }
            | Self::DeleteExchange { transaction_id, .. }
            | Self::RegisterSpill { transaction_id, .. }
            | Self::SetSpillState { transaction_id, .. }
            | Self::DeleteSpill { transaction_id, .. } => transaction_id,
        }
    }

    pub fn expected_revision(&self) -> Option<CatalogRevision> {
        match self {
            Self::PublishTableSnapshot {
                expected_revision, ..
            }
            | Self::PrepareTableWrite {
                expected_revision, ..
            }
            | Self::CommitTableWrite {
                expected_revision, ..
            } => Some(*expected_revision),
            _ => None,
        }
    }

    pub fn fingerprint(&self) -> String {
        serde_json::to_string(self).expect("metadata commands must be serializable")
    }
}
