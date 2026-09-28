//! Physical stage builder abstractions and implementations.
//!
//! Submodules translate logical plan operators into concrete physical stages:
//! - [`pipeline`]: Linear unbroken pipelines (`Scan -> Filter -> Project`).
//!
pub mod pipeline;

pub use pipeline::{DefaultPipelineStageBuilder, PipelineStageBuilder};
