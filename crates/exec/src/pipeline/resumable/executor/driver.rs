use super::{PipelineExecutor, PipelineStep, ResumeToken, StageState};
use crate::pipeline::resumable::{Handoff, InputEvent, OperatorStep};
use common::{Result, VeinError};
use std::sync::Arc;

impl PipelineExecutor {
    pub(super) fn run(&mut self, budget: usize) -> Result<PipelineStep> {
        for _ in 0..budget {
            if self.finished == self.states.len() {
                return Ok(PipelineStep::Finished);
            }
            // Drain downstream first. An unacknowledged output prevents its
            // producer from advancing, so every edge retains at most one view.
            let index = self.active;
            if self.states[index] == StageState::NeedsInput {
                if self.pending[index].is_some() { self.offer(index)?; } else {
                    self.active = index.checked_sub(1)
                        .ok_or_else(|| VeinError::internal("pipeline source requested upstream input"))?;
                }
            } else if self.states[index] == StageState::Ready {
                if let Some(blocked) = self.advance(index)? { return Ok(blocked); }
            } else {
                return Err(VeinError::internal("pipeline cursor reached an inactive stage"));
            }
        }
        Ok(if self.finished == self.states.len() {
            PipelineStep::Finished
        } else { PipelineStep::Runnable })
    }

    fn offer(&mut self, index: usize) -> Result<()> {
        let input = self.pending[index].take()
            .ok_or_else(|| VeinError::internal("pipeline input disappeared before delivery"))?;
        let batch = matches!(&input, InputEvent::Batch(_));
        let port = self.instance.definition().stages()[index].definition().phase().input_port()
            .ok_or_else(|| VeinError::internal("pipeline source cannot receive an input event"))?;
        match self.instance.stage_mut(index).unwrap().push_input(port, input)? {
            Handoff::Accepted => {
                if batch {
                    let producer = index.checked_sub(1)
                        .ok_or_else(|| VeinError::internal("pipeline batch has no producer"))?;
                    self.instance.stage_mut(producer).unwrap().acknowledge_output()?;
                    self.states[producer] = StageState::Ready;
                }
            }
            Handoff::Retry(input) => self.pending[index] = Some(input),
        }
        // A receiver that refused input must perform work (or register a wait)
        // before another offer. No input is replayed after acceptance.
        self.states[index] = StageState::Ready;
        Ok(())
    }

    fn advance(&mut self, index: usize) -> Result<Option<PipelineStep>> {
        match self.instance.stage_mut(index).unwrap().step()? {
            OperatorStep::Output(view) => {
                let next = index + 1;
                if next == self.states.len() || self.pending[next].is_some() {
                    return Err(VeinError::internal("pipeline output has no available downstream edge"));
                }
                self.pending[next] = Some(InputEvent::Batch(view));
                self.states[index] = StageState::AwaitingOutput;
                self.active = next;
            }
            OperatorStep::NeedsInput => self.states[index] = StageState::NeedsInput,
            OperatorStep::Runnable => {}
            OperatorStep::Blocked(reason) => {
                self.generation = self.generation.checked_add(1)
                    .ok_or_else(|| VeinError::internal("pipeline suspension generation overflow"))?;
                let token = ResumeToken { owner: Arc::clone(&self.owner), generation: self.generation, stage: index };
                self.states[index] = StageState::Blocked;
                self.blocked = Some(token.clone());
                return Ok(Some(PipelineStep::Blocked { token, reason }));
            }
            OperatorStep::Finished => {
                self.instance.finish_stage(index)?;
                self.states[index] = StageState::Finished;
                self.finished += 1;
                if index + 1 < self.states.len() {
                    if self.pending[index + 1].is_some() {
                        return Err(VeinError::internal("pipeline finished with undelivered output"));
                    }
                    self.pending[index + 1] = Some(InputEvent::EndOfInput);
                    self.active = index + 1;
                }
            }
        }
        Ok(None)
    }
}
