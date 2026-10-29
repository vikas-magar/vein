//! Executable pipeline construction over canonical plan-node references.

mod factory;
mod stage;
mod pipeline;
mod properties;
mod builder;
mod validation;

pub use factory::{PipelineOperatorProvider, PipelineOperatorState};
pub use stage::ExecutableStage;
pub use pipeline::{ExecutablePipeline, PipelineInstance};
pub use properties::ResolvedPipelineProperties;
pub use builder::ExecutablePipelinePlan;

#[cfg(test)]
mod tests;
