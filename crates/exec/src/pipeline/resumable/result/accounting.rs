use super::{ResultBatch, core::{ArrayCharge, State}};
use crate::memory::{MemoryReservation, QueryMemoryTracker};
use crate::pipeline::{DataChunkView, view::ViewMemoryLease};
use common::{Result, VeinError};
use std::mem::size_of;
use std::sync::Arc;

pub(super) struct Footprint {
    pub arrays: Vec<(usize, usize)>,
    pub metadata: usize,
    pub total: usize,
}
impl Footprint {
    pub fn measure(view: &DataChunkView) -> Result<Self> {
        let arrays = view.source_array_memory_sizes()?;
        let view_metadata = view.retained_metadata_bytes()?;
        let metadata = arrays.capacity().checked_mul(size_of::<(usize, usize)>())
            .and_then(|extra| extra.checked_add(size_of::<super::core::Entry>()))
            .and_then(|extra| view_metadata.checked_add(extra))
            .ok_or_else(|| VeinError::internal("result view footprint overflow"))?;
        let total = arrays.iter().try_fold(metadata, |total, (_, bytes)| total.checked_add(*bytes))
            .ok_or_else(|| VeinError::internal("result retained-array footprint overflow"))?;
        Ok(Self { arrays, metadata, total })
    }
    pub fn acquire(&self, mut view: DataChunkView, tracker: &QueryMemoryTracker) -> Result<ResultBatch> {
        let metadata = tracker.reserve(self.metadata, "result view metadata")?;
        view.account_source_arrays(tracker.identity(), |identity, bytes| {
            let lease: Arc<MemoryReservation> = tracker.lease_retained_array(identity, bytes, "result retained array")?;
            Ok(lease as Arc<dyn ViewMemoryLease>)
        })?;
        Ok(ResultBatch { view, _metadata: metadata })
    }
    pub fn retain(&self, state: &mut State) {
        state.retained += self.metadata;
        for &(id, bytes) in &self.arrays {
            let charge = state.arrays.entry(id).or_insert_with(|| {
                state.retained += bytes;
                ArrayCharge { bytes, references: 0 }
            });
            charge.references += 1;
        }
    }
    pub fn release(&self, state: &mut State) {
        state.retained -= self.metadata;
        for &(id, _) in &self.arrays {
            let charge = state.arrays.get_mut(&id).expect("retained result array is registered");
            charge.references -= 1;
            if charge.references == 0 {
                state.retained -= charge.bytes;
                state.arrays.remove(&id);
            }
        }
    }
}
