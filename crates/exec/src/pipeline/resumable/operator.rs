use common::protocol::pipeline_graph::PipelinePhase;
use std::any::Any;

use crate::pipeline::resumable::{
    Handoff, InputEvent, OperatorInputPort, OperatorMetadata, OperatorRole, OperatorStep,
};
use common::{Result, VeinError};

/// Stateful operator contract for a cooperative pipeline driver.
///
/// Accepting a batch transfers its view handles to the stage. `Retry` returns
/// the original event unchanged. A stage must not be stepped again while the
/// scheduler owns an unaccepted output. After input EOF is accepted, the stage
/// drains and finalizes before returning `Finished`; finished stages reject
/// later input and execution calls.
///
/// Sources are step-driven and do not accept upstream batches. Transforms accept
/// batches and EOF, then emit zero or more views. Sinks accept the same input
/// events but finish only after their finalization work is complete.
pub trait ResumableOperator: Send {
    /// Scheduler-specific signal identifying what must wake a blocked stage.
    type WaitReason: Any + Send;

    /// Offers one owned input event; return it in `Retry` if not accepted.
    fn push_input(&mut self, port: OperatorInputPort, input: InputEvent) -> Result<Handoff<InputEvent>>;

    /// Performs one bounded unit of work and reports its next scheduling state.
    fn step(&mut self) -> Result<OperatorStep<Self::WaitReason>>;
}

/// Operator state that is returned after the stage finishes.
pub trait LocalStateOperator: ResumableOperator {
    /// State owned by one pipeline instance or worker.
    type LocalState: Any + Send;

    /// Transfers local state to the coordinator exactly once after completion.
    fn take_local_state(&mut self) -> Result<Self::LocalState>;
}

/// Object-safe stage interface used by a future scheduler and client adapters.
pub trait ErasedResumableOperator: Send {
    /// Transfers one input event, preserving it when the stage asks for retry.
    fn push_input(&mut self, port: OperatorInputPort, input: InputEvent) -> Result<Handoff<InputEvent>>;
    /// Advances one bounded unit and erases only the wait-reason type.
    fn step(&mut self) -> Result<OperatorStep<super::ErasedWaitReason>>;
    /// Acknowledges that downstream accepted the output returned by `step`.
    fn acknowledge_output(&mut self) -> Result<()>;
    /// Returns local state after finish; a second extraction is rejected.
    fn take_local_state(&mut self) -> Result<Box<dyn Any + Send>>;
}

/// Type-erased scheduler wake-up detail, recoverable by its owning client.
pub struct ErasedWaitReason(Box<dyn Any + Send>);

impl ErasedWaitReason {
    pub(super) fn new<T: Any + Send>(reason: T) -> Self {
        Self(Box::new(reason))
    }

    /// Checks the resource type before routing ownership to its registered adapter.
    pub fn is<T: Any + Send>(&self) -> bool { self.0.is::<T>() }

    /// Recovers the concrete wait reason without cloning its payload.
    pub fn downcast<T: Any + Send>(self) -> Result<T> {
        self.0.downcast::<T>().map(|reason| *reason)
            .map_err(|_| VeinError::invalid_input("wait reason type does not match requested type"))
    }
}

/// One-time type erasure for a typed operator stage.
pub(super) struct OperatorAdapter<O> {
    operator: O,
    role: OperatorRole,
    input_closed: Vec<Option<bool>>,
    pending_output: bool,
    finished: bool,
    local_state_taken: bool,
}

impl<O: LocalStateOperator> OperatorAdapter<O> {
    #[cfg(test)]
    pub(super) fn new(operator: O, metadata: &OperatorMetadata) -> Self {
        Self {
            operator,
            role: metadata.role,
            input_closed: vec![Some(false); metadata.children.len()],
            pending_output: false,
            finished: false,
            local_state_taken: false,
        }
    }
    pub(super) fn for_phase(operator: O, metadata: &OperatorMetadata, phase: PipelinePhase) -> Result<Self> {
        let mut input_closed = vec![None; metadata.children.len()];
        if let Some(port) = phase.input_port() {
            let slot = input_closed.get_mut(port.index()).ok_or_else(||
                VeinError::invalid_input("operator phase references a missing input port"))?;
            *slot = Some(false);
        }
        Ok(Self {
            operator,
            role: phase.role(),
            input_closed,
            pending_output: false,
            finished: false,
            local_state_taken: false,
        })
    }
}

impl<O: LocalStateOperator> ErasedResumableOperator for OperatorAdapter<O> {
    fn push_input(&mut self, port: OperatorInputPort, input: InputEvent) -> Result<Handoff<InputEvent>> {
        if self.finished || self.pending_output {
            return Err(VeinError::internal("stage cannot accept input in its current state"));
        }
        if self.role == OperatorRole::Source {
            return Err(VeinError::invalid_input("source stages do not accept upstream input"));
        }
        let closed = self.input_closed.get_mut(port.index()).and_then(Option::as_mut)
            .ok_or_else(|| VeinError::invalid_input("input port does not exist on this stage"))?;
        if *closed {
            return Err(VeinError::invalid_input("input port already reached end-of-input"));
        }
        let closes_input = matches!(input, InputEvent::EndOfInput);
        match self.operator.push_input(port, input)? {
            Handoff::Accepted => {
                *closed |= closes_input;
                Ok(Handoff::Accepted)
            }
            Handoff::Retry(input) => Ok(Handoff::Retry(input)),
        }
    }

    fn step(&mut self) -> Result<OperatorStep<super::ErasedWaitReason>> {
        if self.finished || self.pending_output {
            return Err(VeinError::internal("stage cannot be stepped before output acknowledgement or after finish"));
        }
        let step = self.operator.step()?;
        match step {
            OperatorStep::Output(_) if self.role == OperatorRole::Sink => {
                Err(VeinError::internal("sink stages cannot produce output batches"))
            }
            OperatorStep::Output(view) => {
                self.pending_output = true;
                Ok(OperatorStep::Output(view))
            }
            OperatorStep::NeedsInput if self.input_closed.iter().flatten().all(|closed| *closed) => {
                Err(VeinError::internal("stage cannot request input after all active ports are closed"))
            }
            OperatorStep::Blocked(reason) => Ok(OperatorStep::Blocked(super::ErasedWaitReason::new(reason))),
            OperatorStep::Finished if self.role != OperatorRole::Source && !self.input_closed.iter().flatten().all(|closed| *closed) => {
                Err(VeinError::internal("transform or sink finished before end-of-input"))
            }
            OperatorStep::Finished => {
                self.finished = true;
                Ok(OperatorStep::Finished)
            }
            OperatorStep::Runnable => Ok(OperatorStep::Runnable),
            OperatorStep::NeedsInput => Ok(OperatorStep::NeedsInput),
        }
    }

    fn acknowledge_output(&mut self) -> Result<()> {
        if !self.pending_output {
            return Err(VeinError::internal("stage has no output awaiting acknowledgement"));
        }
        self.pending_output = false;
        Ok(())
    }

    fn take_local_state(&mut self) -> Result<Box<dyn Any + Send>> {
        if !self.finished || self.local_state_taken {
            return Err(VeinError::internal("local state is available once, after stage completion"));
        }
        let local = self.operator.take_local_state()?;
        self.local_state_taken = true;
        Ok(Box::new(local))
    }
}
