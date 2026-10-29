//! Client-independent, cooperative execution over prepared canonical stages.
//!
//! Every turn is bounded by operator calls, not elapsed time. Operators must
//! implement bounded `step` work. The scheduler owns readiness and wakeups.

mod driver;
mod state;

use super::executable::PipelineInstance;
use super::{ErasedWaitReason, InputEvent};
use common::{Result, VeinError};
use state::StageState;
use std::num::NonZeroUsize;
use std::sync::Arc;

/// Identity of one suspension, valid only for its owning executor and generation.
#[derive(Clone)]
pub struct ResumeToken {
    owner: Arc<()>,
    generation: u64,
    stage: usize,
}

impl ResumeToken {
    /// Position of the stage waiting on a scheduler-owned resource.
    pub fn stage(&self) -> usize { self.stage }
}

/// Scheduling outcome of a bounded execution turn.
pub enum PipelineStep {
    /// The work budget was exhausted; another turn can run immediately.
    Runnable,
    /// No more operator work or local-state combination remains.
    Finished,
    /// Resume only after the resource is ready, using this exact token.
    Blocked {
        /// Identifies the owning stage and this suspension generation.
        token: ResumeToken,
        /// Resource-specific readiness information for the scheduler.
        reason: ErasedWaitReason,
    },
}

/// Owns local operators and undelivered views throughout suspension and draining.
pub struct PipelineExecutor {
    instance: PipelineInstance,
    states: Vec<StageState>,
    pending: Vec<Option<InputEvent>>,
    owner: Arc<()>,
    generation: u64,
    blocked: Option<ResumeToken>,
    failed: bool,
    active: usize,
    finished: usize,
}

impl PipelineExecutor {
    /// Takes exclusive ownership of a fresh worker; no input payload is cloned.
    pub fn new(instance: PipelineInstance) -> Self {
        let count = instance.definition().stages().len();
        Self {
            instance,
            states: vec![StageState::Ready; count],
            pending: (0..count).map(|_| None).collect(),
            owner: Arc::new(()),
            generation: 0,
            blocked: None,
            failed: false,
            active: count.saturating_sub(1),
            finished: 0,
        }
    }

    /// Performs at most `budget` stage steps or input offers.
    ///
    /// Errors from a stage are terminal. Repeated calls after completion are
    /// harmless; calling while blocked requires the matching readiness token.
    pub fn execute(&mut self, budget: NonZeroUsize) -> Result<PipelineStep> {
        if self.failed { return Err(VeinError::invalid_input("pipeline execution already failed")); }
        if self.blocked.is_some() {
            return Err(VeinError::invalid_input("pipeline must be resumed before execution"));
        }
        let result = self.run(budget.get());
        if result.is_err() {
            self.failed = true;
            self.pending.iter_mut().for_each(|event| *event = None);
        }
        result
    }

    /// Marks a suspended stage runnable without repeating its previous step.
    /// Stale tokens and tokens belonging to another worker are rejected.
    pub fn resume(&mut self, token: &ResumeToken) -> Result<()> {
        let valid = self.blocked.as_ref().is_some_and(|active|
            Arc::ptr_eq(&active.owner, &token.owner)
                && active.generation == token.generation && active.stage == token.stage);
        if !valid { return Err(VeinError::invalid_input("pipeline resume token is stale or belongs to another executor")); }
        self.states[token.stage] = StageState::Ready;
        self.blocked = None;
        Ok(())
    }
}
