//! Cluster-visible exchange metadata shared by planners, VCS, and executors.
use serde::{Deserialize, Serialize};

use crate::identifiers::{NodeId, QueryId, StageId, TaskId};

/// One optional canonical-address column transported with an exchange batch.
///
/// The bitmap uses the same packed row order as the Arrow payload. Keeping this
/// wire type independent from EXO lets value-native sources participate in the
/// same exchange protocol without taking a dependency on the storage crate.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
)]
pub struct ExchangeAddressColumn {
    /// Canonical address for every physical row in the Arrow payload.
    pub addresses: Vec<u64>,
    /// Packed validity bitmap aligned with the addresses.
    pub validity: Vec<u8>,
}

/// One self-contained exchange payload.
///
/// The VFS layer frames this encoded value. The Arrow bytes remain opaque to the
/// metadata/control plane, while address sidecars remain available to reducers.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
)]
pub struct ExchangeBatch {
    /// One independently decodable Arrow IPC stream.
    pub arrow_ipc: Vec<u8>,
    /// Address sidecars aligned with Arrow columns.
    pub address_columns: Vec<Option<ExchangeAddressColumn>>,
}

impl ExchangeBatch {
    const MAGIC: &'static [u8; 4] = b"VEXB";
    const VERSION: u32 = 1;

    /// Encodes this batch for storage in one VFS exchange frame.
    pub fn encode(self) -> Result<Vec<u8>, String> {
        let arrow_len = u64::try_from(self.arrow_ipc.len())
            .map_err(|_| "exchange Arrow payload is too large".to_string())?;
        let column_count = u32::try_from(self.address_columns.len())
            .map_err(|_| "exchange has too many columns".to_string())?;

        let sidecar_len = self
            .address_columns
            .iter()
            .try_fold(4usize, |length, column| {
                let column_len = match column {
                    None => 1,
                    Some(column) => 1usize
                        .checked_add(16)?
                        .checked_add(column.addresses.len().checked_mul(8)?)?
                        .checked_add(column.validity.len())?,
                };
                length.checked_add(column_len)
            })
            .ok_or_else(|| "exchange sidecar length overflows usize".to_string())?;

        let mut output = self.arrow_ipc;
        let payload_len = output.len();
        let framed_len = payload_len
            .checked_add(16)
            .ok_or_else(|| "exchange frame length overflows usize".to_string())?;
        output
            .try_reserve(
                16usize
                    .checked_add(sidecar_len)
                    .ok_or_else(|| "exchange frame capacity overflows usize".to_string())?,
            )
            .map_err(|error| format!("unable to reserve exchange frame: {error}"))?;
        output.resize(framed_len, 0);
        output.copy_within(0..payload_len, 16);
        output[0..4].copy_from_slice(Self::MAGIC);
        output[4..8].copy_from_slice(&Self::VERSION.to_le_bytes());
        output[8..16].copy_from_slice(&arrow_len.to_le_bytes());
        output.extend_from_slice(&column_count.to_le_bytes());

        for column in self.address_columns {
            match column {
                None => output.push(0),
                Some(column) => {
                    output.push(1);
                    let row_count = u64::try_from(column.addresses.len())
                        .map_err(|_| "exchange address column is too large".to_string())?;
                    let validity_len = u64::try_from(column.validity.len())
                        .map_err(|_| "exchange validity bitmap is too large".to_string())?;
                    output.extend_from_slice(&row_count.to_le_bytes());
                    output.extend_from_slice(&validity_len.to_le_bytes());
                    for address in column.addresses {
                        output.extend_from_slice(&address.to_le_bytes());
                    }
                    output.extend_from_slice(&column.validity);
                }
            }
        }
        Ok(output)
    }

    /// Decodes and validates one stored exchange payload.
    pub fn decode(bytes: &[u8]) -> Result<Self, String> {
        let (arrow_range, address_columns) = Self::decode_parts(bytes)?;
        Ok(Self {
            arrow_ipc: bytes[arrow_range].to_vec(),
            address_columns,
        })
    }

    /// Decodes an owned frame by reusing its allocation for the Arrow IPC payload.
    pub fn decode_owned(mut bytes: Vec<u8>) -> Result<Self, String> {
        let (arrow_range, address_columns) = Self::decode_parts(&bytes)?;
        let arrow_len = arrow_range.len();
        bytes.copy_within(arrow_range, 0);
        bytes.truncate(arrow_len);
        Ok(Self {
            arrow_ipc: bytes,
            address_columns,
        })
    }

    fn decode_parts(
        bytes: &[u8],
    ) -> Result<(std::ops::Range<usize>, Vec<Option<ExchangeAddressColumn>>), String> {
        let mut cursor = 0usize;
        let magic = read_bytes(bytes, &mut cursor, 4)?;
        if magic != Self::MAGIC {
            return Err("invalid exchange batch magic".to_string());
        }
        if read_u32(bytes, &mut cursor)? != Self::VERSION {
            return Err("unsupported exchange batch version".to_string());
        }

        let arrow_len = usize::try_from(read_u64(bytes, &mut cursor)?)
            .map_err(|_| "exchange Arrow payload length overflows usize".to_string())?;
        let arrow_start = cursor;
        read_bytes(bytes, &mut cursor, arrow_len)?;
        let arrow_range = arrow_start..cursor;
        let column_count = usize::try_from(read_u32(bytes, &mut cursor)?)
            .map_err(|_| "exchange column count overflows usize".to_string())?;
        let mut address_columns = Vec::with_capacity(column_count);

        for _ in 0..column_count {
            match read_u8(bytes, &mut cursor)? {
                0 => address_columns.push(None),
                1 => {
                    let row_count = usize::try_from(read_u64(bytes, &mut cursor)?)
                        .map_err(|_| "exchange row count overflows usize".to_string())?;
                    let validity_len = usize::try_from(read_u64(bytes, &mut cursor)?)
                        .map_err(|_| "exchange validity length overflows usize".to_string())?;
                    let address_bytes = row_count.checked_mul(8).ok_or_else(|| {
                        "exchange address byte length overflows usize".to_string()
                    })?;
                    let address_bytes = read_bytes(bytes, &mut cursor, address_bytes)?;
                    let addresses = address_bytes
                        .chunks_exact(8)
                        .map(|value| u64::from_le_bytes(value.try_into().expect("8-byte chunk")))
                        .collect();
                    let validity = read_bytes(bytes, &mut cursor, validity_len)?.to_vec();
                    address_columns.push(Some(ExchangeAddressColumn {
                        addresses,
                        validity,
                    }));
                }
                marker => {
                    return Err(format!("invalid exchange address column marker {marker}"));
                }
            }
        }

        if cursor != bytes.len() {
            return Err("trailing bytes in exchange batch payload".to_string());
        }
        Ok((arrow_range, address_columns))
    }
}

fn read_bytes<'a>(bytes: &'a [u8], cursor: &mut usize, length: usize) -> Result<&'a [u8], String> {
    let end = cursor
        .checked_add(length)
        .ok_or_else(|| "exchange payload offset overflows usize".to_string())?;
    if end > bytes.len() {
        return Err("truncated exchange batch payload".to_string());
    }
    let value = &bytes[*cursor..end];
    *cursor = end;
    Ok(value)
}

fn read_u8(bytes: &[u8], cursor: &mut usize) -> Result<u8, String> {
    read_bytes(bytes, cursor, 1).map(|value| value[0])
}

fn read_u32(bytes: &[u8], cursor: &mut usize) -> Result<u32, String> {
    read_bytes(bytes, cursor, 4)
        .map(|value| u32::from_le_bytes(value.try_into().expect("4-byte slice")))
}

fn read_u64(bytes: &[u8], cursor: &mut usize) -> Result<u64, String> {
    read_bytes(bytes, cursor, 8)
        .map(|value| u64::from_le_bytes(value.try_into().expect("8-byte slice")))
}

/// Identity of one shuffle exchange.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
)]
pub struct ExchangeKey {
    /// Query owning the exchange.
    pub query_id: QueryId,
    /// Stage producing the exchange.
    pub stage_id: StageId,
}

/// Lifecycle state of a shuffle exchange.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
)]
pub enum ExchangeState {
    /// Producers may publish fragments.
    Open,
    /// All producer output is visible to reducers.
    Sealed,
    /// The exchange is invalid and should be removed.
    Aborted,
}

/// One immutable persisted shuffle fragment.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
)]
pub struct ExchangeFragment {
    /// Exchange partition containing this fragment.
    pub partition_id: u32,
    /// Producer task that created the fragment.
    pub task_id: TaskId,
    /// Retry attempt for the producer task.
    pub attempt: u32,
    /// VFS or object-store path for the immutable fragment.
    pub storage_path: String,
    /// VFS node that owns or cached the fragment, when known.
    pub node_id: Option<NodeId>,
    /// Fragment byte length.
    pub byte_len: u64,
    /// Rows contained in the fragment.
    pub row_count: u64,
    /// Content checksum used for validation.
    pub checksum: u64,
}

/// Lifecycle state of one join spill run.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
)]
pub enum SpillRunState {
    /// The producer may still append frames.
    Open,
    /// The run is complete and may be consumed.
    Sealed,
    /// The run is no longer valid.
    Aborted,
}

/// Durable descriptor for one join spill run.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
)]
pub struct SpillRun {
    /// Exchange that owns the join.
    pub exchange_key: ExchangeKey,
    /// Task attempt that produced the run.
    pub task_id: TaskId,
    /// Retry attempt number.
    pub attempt: u32,
    /// Reducer partition containing the run.
    pub partition_id: u32,
    /// Grace-hash subpartition of the run.
    pub subpartition_id: u32,
    /// VFS or object-store path containing the frames.
    pub storage_path: String,
    /// Bytes written to the run.
    pub byte_len: u64,
    /// Rows written to the run.
    pub row_count: u64,
    /// Rolling content checksum.
    pub checksum: u64,
    /// Current lifecycle state.
    pub state: SpillRunState,
    /// Lease expiration in Unix milliseconds.
    pub lease_until_ms: u64,
}

/// Authoritative metadata for one exchange stage.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
)]
pub struct ExchangeManifest {
    /// Exchange identity.
    pub key: ExchangeKey,
    /// Number of reducer partitions.
    pub partition_count: u16,
    /// Producer tasks expected to publish output.
    pub expected_tasks: Vec<TaskId>,
    /// Fingerprint of the exchanged schema and key contract.
    pub schema_fingerprint: String,
    /// Published immutable fragments.
    pub fragments: Vec<ExchangeFragment>,
    /// Current exchange lifecycle state.
    pub state: ExchangeState,
}

impl ExchangeManifest {
    /// Creates an open exchange manifest.
    pub fn new(
        key: ExchangeKey,
        partition_count: u16,
        expected_tasks: Vec<TaskId>,
        schema_fingerprint: impl Into<String>,
    ) -> Self {
        Self {
            key,
            partition_count,
            expected_tasks,
            schema_fingerprint: schema_fingerprint.into(),
            fragments: Vec::new(),
            state: ExchangeState::Open,
        }
    }

    /// Returns fragments for one reducer partition.
    pub fn fragments_for_partition(
        &self,
        partition_id: u32,
    ) -> impl Iterator<Item = &ExchangeFragment> {
        self.fragments
            .iter()
            .filter(move |fragment| fragment.partition_id == partition_id)
    }

    /// Returns whether every expected producer has a published attempt.
    pub fn all_tasks_published(&self) -> bool {
        self.expected_tasks.iter().all(|task_id| {
            self.fragments
                .iter()
                .any(|fragment| fragment.task_id == *task_id)
        })
    }

    /// Returns whether every expected producer published every partition.
    pub fn all_fragments_published(&self) -> bool {
        self.expected_tasks.iter().all(|task_id| {
            (0..self.partition_count as u32).all(|partition_id| {
                self.fragments.iter().any(|fragment| {
                    fragment.task_id == *task_id && fragment.partition_id == partition_id
                })
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{ExchangeAddressColumn, ExchangeBatch};

    #[test]
    fn owned_decode_preserves_exchange_payload() {
        let exchange = ExchangeBatch {
            arrow_ipc: (0..64).collect(),
            address_columns: vec![
                Some(ExchangeAddressColumn {
                    addresses: vec![3, 5, 8],
                    validity: vec![0b0000_0101],
                }),
                None,
            ],
        };
        let encoded = exchange.encode().expect("encode exchange frame");
        let borrowed = ExchangeBatch::decode(&encoded).expect("decode borrowed frame");
        let owned = ExchangeBatch::decode_owned(encoded).expect("decode owned frame");

        assert_eq!(borrowed, owned);
        assert_eq!(owned.arrow_ipc, (0..64).collect::<Vec<u8>>());
    }
}
