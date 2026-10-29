use super::accounting::Footprint;
use super::wait::WaitKind;
use super::{OutputOrder, ResultBatch, ResultBufferConfig};
use crate::memory::{MemoryReservation, QueryMemoryTracker};
use common::{Result, VeinError};
use std::collections::{HashMap, VecDeque};
use std::mem::size_of;
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};
use common::cancellation::CancellationRegistration;
use crate::runtime::QueryExecutionResources;
use std::task::Waker;

pub(super) struct Entry {
    pub batch: ResultBatch,
    pub footprint: Footprint,
}
pub(super) struct ArrayCharge {
    pub bytes: usize,
    pub references: usize,
}
pub(super) struct ProducerState {
    pub busy: bool,
    pub finished: bool,
    pub queue: VecDeque<Entry>,
}
#[derive(Clone, Copy)]
pub(super) enum Terminal { Closed, Cancelled, Failed }
pub(super) struct State {
    pub producers: Vec<ProducerState>,
    pub arrivals: VecDeque<usize>,
    pub frontier: usize,
    pub reserved: usize,
    pub retained: usize,
    pub pending: usize,
    pub queued: usize,
    pub arrays: HashMap<usize, ArrayCharge>,
    pub terminal: Option<Terminal>,
    pub error: Option<VeinError>,
    pub waiters: HashMap<u64, (WaitKind, Waker)>,
    pub next_wait: u64,
}
pub(super) struct Core {
    pub config: ResultBufferConfig,
    pub tracker: QueryMemoryTracker,
    pub state: Mutex<State>,
    pub _metadata: MemoryReservation,
    pub query: OnceLock<Arc<QueryExecutionResources>>,
    pub cancellation: OnceLock<CancellationRegistration>,
}

impl Core {
    pub fn new(config: ResultBufferConfig, producers: usize, tracker: QueryMemoryTracker) -> Result<Self> {
        let bytes = producers.checked_mul(size_of::<ProducerState>())
            .and_then(|extra| extra.checked_add(size_of::<Self>()))
            .ok_or_else(|| VeinError::internal("result producer metadata overflow"))?;
        let metadata = tracker.reserve(bytes, "result buffer metadata")?;
        Ok(Self {
            config,
            tracker,
            _metadata: metadata,
            query: OnceLock::new(),
            cancellation: OnceLock::new(),
            state: Mutex::new(State {
                producers: (0..producers).map(|_| ProducerState { busy: false, finished: false, queue: VecDeque::new() }).collect(),
                arrivals: VecDeque::new(),
                frontier: 0,
                reserved: 0,
                retained: 0,
                pending: 0,
                queued: 0,
                arrays: HashMap::new(),
                terminal: None,
                error: None,
                waiters: HashMap::new(),
                next_wait: 0,
            }),
        })
    }
    pub fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|poison| poison.into_inner())
    }
    pub fn can_acquire(&self, state: &State, producer: usize) -> bool {
        let slot = &state.producers[producer];
        if slot.busy || slot.finished || state.terminal.is_some() { return false; }
        let limit = if self.config.order == OutputOrder::ProducerOrder && producer != state.frontier {
            self.config.capacity_bytes.get() - self.config.max_batch_bytes.get()
        } else { self.config.capacity_bytes.get() };
        state.retained.checked_add(state.reserved).and_then(|used| used.checked_add(self.config.max_batch_bytes.get()))
            .is_some_and(|used| used <= limit)
    }
    pub fn advance(&self, state: &mut State) {
        while state.frontier < state.producers.len()
            && state.producers[state.frontier].finished && state.producers[state.frontier].queue.is_empty()
        { state.frontier += 1; }
    }
    pub fn data_ready(&self, state: &State) -> bool {
        match self.config.order {
            OutputOrder::Unordered => !state.arrivals.is_empty(),
            OutputOrder::ProducerOrder => state.producers.get(state.frontier).is_some_and(|slot| !slot.queue.is_empty()),
        }
    }
    pub fn complete(&self, state: &State) -> bool {
        state.queued == 0 && state.producers.iter().all(|slot| slot.finished)
    }
}
