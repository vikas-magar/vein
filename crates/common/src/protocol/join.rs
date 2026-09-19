//! Physical join and exchange contracts shared by planners and executors.

use serde::{Deserialize, Serialize};

use crate::identifiers::StageId;
use super::exchange::ExchangeKey;
use super::task::FieldSchema;

/// Physical join semantics independent of the parser's AST types.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize,
)]
pub enum PhysicalJoinType {
    /// Return rows that match on both sides.
    Inner,
    /// Preserve unmatched rows from the left input.
    Left,
    /// Preserve unmatched rows from the right input.
    Right,
    /// Preserve unmatched rows from both inputs.
    Full,
    /// Cartesian product without join keys.
    Cross,
}

/// Key extraction policy selected by the physical planner.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize,
)]
pub enum JoinKeyMode {
    /// Let the source adapter choose its most efficient exact representation.
    Auto,
    /// Prefer an EXO canonical address when the source can provide one.
    AddressNative,
    /// Extract and compare values from the columnar batch.
    ValueNative,
}

/// A resolved join key column.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize,
)]
pub struct JoinKeySpec {
    /// Stable schema field identifier.
    pub field_id: i32,
    /// Resolved column name for source adapters that address columns by name.
    pub column_name: String,
    /// Preferred key extraction mode.
    pub mode: JoinKeyMode,
}

/// One partition of an upstream stage consumed by a downstream operator.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize,
)]
pub struct ExchangeInput {
    /// Authoritative VCS exchange identity.
    pub exchange_key: ExchangeKey,
    /// Producing stage.
    pub upstream_stage_id: StageId,
    /// Canonical VFS prefix written by the producing shuffle sink.
    pub path_prefix: String,
    /// Partition assigned to this task.
    pub partition_id: u32,
    /// Total number of partitions in the exchange.
    pub partition_count: u16,
}

/// Two-input physical join source.
///
/// Both inputs must use the same normalized key encoding and partition hash
/// contract before reaching a partition-local join task.
#[derive(
    Debug, Clone, Serialize, Deserialize, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize,
)]
pub struct JoinSource {
    /// Left-side exchange input.
    pub left: ExchangeInput,
    /// Right-side exchange input.
    pub right: ExchangeInput,
    /// Join semantics.
    pub join_type: PhysicalJoinType,
    /// Left join keys, in comparison order.
    pub left_keys: Vec<JoinKeySpec>,
    /// Right join keys, in comparison order.
    pub right_keys: Vec<JoinKeySpec>,
    /// Whether exact verification is required after candidate-key matching.
    pub requires_exact_verification: bool,
    /// Schema of the left exchange stream.
    pub left_schema: Vec<FieldSchema>,
    /// Schema of the right exchange stream.
    pub right_schema: Vec<FieldSchema>,
}

impl JoinSource {
    /// Returns whether this is an equi-join with aligned key arity.
    pub fn is_equi_join(&self) -> bool {
        !self.left_keys.is_empty() && self.left_keys.len() == self.right_keys.len()
    }
}
