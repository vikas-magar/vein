//! Client-neutral output capacity, ownership and readiness.
//!
//! Producers acquire capacity before constructing output, prepare a retained
//! view, then publish it. Capacity covers pending and queued output; fetched
//! handles retain query memory leases until released. Estimates are explicit
//! engine reservations, not allocator/RSS measurements.
//! Repeated ArrayRef identities share one payload charge; distinct Arrow arrays
//! sharing internal buffers can be conservatively charged more than once.

mod accounting;
mod core;
mod producer;
mod permit;
mod pending;
mod consumer;
mod wait;
mod terminal;
mod metrics;
mod sink;
mod query;

use crate::memory::{MemoryReservation, QueryMemoryTracker};
use crate::pipeline::DataChunkView;
use common::{Result, VeinError};
use core::Core;
use std::num::NonZeroUsize;
use std::sync::Arc;

pub use consumer::{FetchResult, ResultConsumer};
pub use permit::{Admission, ResultPermit};
pub use pending::PendingResult;
pub use producer::ResultProducer;
pub use wait::{ResultWait, ResultWaitGuard, ResultWaitRegistrar};
pub use metrics::{ResultBufferMetrics, result_batch_footprint};
pub use sink::ResultSink;

/// Explicit output arrival policy; this does not sort rows or compare SQL keys.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OutputOrder {
    /// Deliver batches in publication order across all producers.
    Unordered,
    /// Drain producers in their assigned ordinal order, preserving each one's batches.
    ProducerOrder,
}

/// Hard capacity and per-batch retained-footprint limits for output admission.
#[derive(Clone, Copy, Debug)]
pub struct ResultBufferConfig {
    /// Capacity covering both reserved producer credit and retained queued/pending views.
    pub capacity_bytes: NonZeroUsize,
    /// Maximum admissible standalone footprint; ordered output reserves this headroom.
    /// Capacity equal to this limit permits only the current ordered producer.
    pub max_batch_bytes: NonZeroUsize,
    /// Required producer ordering semantics.
    pub order: OutputOrder,
}

/// A fetched view and its owned metadata reservation; Arrow data stays in DataChunkView.
pub struct ResultBatch {
    view: DataChunkView,
    _metadata: MemoryReservation,
}

impl ResultBatch {
    /// Accounts a directly fetched view using the same retained-array and
    /// metadata lease logic as buffered output. No column values are copied.
    pub fn account(view: DataChunkView, tracker: &QueryMemoryTracker) -> Result<Self> {
        accounting::Footprint::measure(&view)?.acquire(view, tracker)
    }
    /// Borrow the canonical view without gathering or copying its columns.
    pub fn view(&self) -> &DataChunkView { &self.view }
}

/// Creates one consumer and uniquely owned producer handles with stable ordinals.
pub fn result_buffer(config: ResultBufferConfig, producers: NonZeroUsize, tracker: QueryMemoryTracker)
                     -> Result<(ResultConsumer, Vec<ResultProducer>)>
{
    if config.max_batch_bytes > config.capacity_bytes {
        return Err(VeinError::invalid_input("result batch limit exceeds buffer capacity"));
    }
    let core = Arc::new(Core::new(config, producers.get(), tracker)?);
    let handles = (0..producers.get()).map(|id| ResultProducer { core: Arc::clone(&core), id, finished: false }).collect();
    Ok((ResultConsumer { core }, handles))
}

#[cfg(test)]
mod tests;
