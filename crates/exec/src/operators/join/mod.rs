//! Join execution building blocks shared by local and distributed joins.

mod hash;
mod key;
mod partition;
mod source;

pub use hash::{HashJoinTable, JoinMatch, JoinRowRef};
pub use partition::JoinPartitioner;
pub use source::HashJoinSource;
pub use key::{
    ArrowJoinKeyExtractor, JoinKey, JoinKeyBatch, JoinKeyExtractor, JoinKeyPart, JoinKeyRow,
    try_extract_address_keys,
};
