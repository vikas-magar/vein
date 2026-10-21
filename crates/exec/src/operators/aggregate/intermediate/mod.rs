//! Spill records retain group keys and opaque, aggregate-owned state payloads.

mod group_key;
mod state_payload;
#[cfg(test)]
pub(super) mod test_support;

use std::sync::Arc;

use common::Result;

pub(super) use group_key::AggregateGroupKeyRef;
use state_payload::{decode, encode};

#[derive(Clone)]
pub(super) struct AggregateIntermediateGroup {
    /// Query-local hash retained for consistent recursive spill partitioning.
    pub(super) hash: u64,
    pub(super) grouping_set_id: usize,
    pub(super) keys: Vec<Option<AggregateGroupKeyRef>>,
    /// The owning registered aggregate encodes and decodes each state.
    pub(super) aggregate_states: Vec<Arc<Vec<u8>>>,
}

impl AggregateIntermediateGroup {
    pub(super) fn encode_aggregate_states(&self) -> Result<Vec<u8>> {
        encode(&self.aggregate_states)
    }

    pub(super) fn decode_aggregate_states(bytes: &[u8]) -> Result<Vec<Arc<Vec<u8>>>> {
        decode(bytes)
    }
}

#[cfg(test)]
mod tests;
