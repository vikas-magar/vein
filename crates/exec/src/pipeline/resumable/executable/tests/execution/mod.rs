//! Scripted protocol tests exercise the real canonical construction and driver.
pub(super) mod stage;
pub(super) mod fixture;
mod delivery;
mod lifecycle;
mod chain;

use crate::pipeline::resumable::executor::{PipelineExecutor, PipelineStep};
use std::num::NonZeroUsize;

fn budget(value: usize) -> NonZeroUsize { NonZeroUsize::new(value).unwrap() }

fn drain(executor: &mut PipelineExecutor, work: usize) -> usize {
    let mut suspensions = 0;
    for _ in 0..1000 {
        match executor.execute(budget(work)).expect("pipeline turn") {
            PipelineStep::Finished => return suspensions,
            PipelineStep::Runnable => {}
            PipelineStep::Blocked { token, reason } => {
                reason.downcast::<u8>().expect("typed wait reason");
                executor.resume(&token).unwrap();
                suspensions += 1;
            }
        }
    }
    panic!("pipeline did not finish within the test turn limit");
}
