//! Owned batch processing shared by operator adapters.
mod stage;
mod state;

use crate::pipeline::DataChunkView;
use common::Result;
use common::protocol::extension::ParallelismCapability;

/// Synchronous, downstream-free processing over reference-backed input.
pub trait BatchProcessor: Send + Sync {
    /// Processes one batch; `None` means no output, not end-of-input.
    fn process(&mut self, input: DataChunkView) -> Result<Option<DataChunkView>>;

    /// Creates independent worker-local state from this immutable template.
    fn fork(&self) -> Result<Box<dyn BatchProcessor>>;

    /// Declares whether independent forks may run concurrently.
    fn parallelism(&self) -> ParallelismCapability {
        ParallelismCapability::Serial
    }
}

pub use state::BatchProcessorState;

#[cfg(test)]
#[path = "tests/mod.rs"]
mod tests;
