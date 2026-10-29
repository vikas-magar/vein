//! One execution outcome shared by cursor, controls and worker failures.
mod transitions;
mod snapshot;

use crate::operators::sink::WriteReceipt;
use common::time::Instant;
use common::VeinError;
use futures::task::AtomicWaker;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;
pub use snapshot::QueryProgress;

/// Stable cursor lifecycle; a computation can finish before its output is drained.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryStatus {
    /// Prepared, with no execution work requested.
    Prepared,
    /// Execution or result draining is in progress.
    Running,
    /// Output was drained or a write commit was confirmed.
    Finished,
    /// Execution failed; the original shared error remains available.
    Failed,
    /// Cancellation won before completion.
    Cancelled,
    /// The owning cursor closed before completion.
    Closed,
}
impl QueryStatus {
    /// Whether no further output may be delivered.
    pub fn is_terminal(self) -> bool { !matches!(self, Self::Prepared | Self::Running) }
}

pub(super) struct State {
    pub status: QueryStatus,
    pub failure: Option<Arc<VeinError>>,
    pub receipt: Option<Arc<WriteReceipt>>,
    pub rows: u64,
    pub batches: u64,
    pub started: Option<Instant>,
    pub elapsed: Duration,
    pub cursor_claimed: bool,
}

pub(super) struct Lifecycle {
    state: Mutex<State>,
    pub waker: AtomicWaker,
}
impl Lifecycle {
    pub fn new() -> Self {
        Self {
            state: Mutex::new(State {
                status: QueryStatus::Prepared,
                failure: None,
                receipt: None,
                rows: 0,
                batches: 0,
                started: None,
                elapsed: Duration::ZERO,
                cursor_claimed: false,
            }),
            waker: AtomicWaker::new(),
        }
    }
    pub fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|poison| poison.into_inner())
    }
    pub fn stop_clock(state: &mut State) {
        if let Some(started) = state.started.take() { state.elapsed = started.elapsed(); }
    }
}
