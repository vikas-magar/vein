//! Demand-driven, format-neutral scan output.
mod readiness;
mod registrar;
mod stage;
mod work;
mod binding;
mod provider;

pub use readiness::{ScanReady, ScanRegistration};
pub use registrar::{ScanWaitRegistrar, ScanWaitRegistration};
pub use stage::ScanStage;
pub use work::ScanWorkQueue;
pub use provider::ScanPipelineProvider;

#[cfg(test)]
mod tests;

use common::Result;
use futures::{StreamExt, stream::BoxStream};
use crate::pipeline::{DataChunkView, PipelineNode};

/// Retains provider position and pending I/O. Dropping it stops remaining reads.
/// `Pending` must register the supplied task waker; EOF is distinct from empty output.
pub type ScanCursor<'a> = BoxStream<'a, Result<DataChunkView>>;

/// Transitional push adapter over the canonical cursor, without read-ahead.
pub async fn drain(mut cursor: ScanCursor<'_>, next: &mut dyn PipelineNode) -> Result<()> {
    while !next.finished() {
        let Some(view) = cursor.next().await else { break };
        next.push(view?).await?;
    }
    Ok(())
}
