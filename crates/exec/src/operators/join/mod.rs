//! Join execution building blocks shared by local and distributed joins.

mod hash;
mod key;
mod memory;
mod partition;
mod source;
mod spill;

pub use hash::{HashJoinTable, JoinMatch, JoinRowRef};
pub use key::{
    ArrowJoinKeyExtractor, JoinKey, JoinKeyBatch, JoinKeyExtractor, JoinKeyPart, JoinKeyRow,
    try_extract_address_keys,
};
pub use memory::{DEFAULT_JOIN_MEMORY_LIMIT, JoinMemoryBudget, JoinMemoryReservation};
pub use partition::JoinPartitioner;
pub use source::HashJoinSource;
pub use spill::{
    JoinSpillIdentity, JoinSpillWriter, VfsJoinSpillWriter, new_attempt_spill_path, new_spill_path,
};
