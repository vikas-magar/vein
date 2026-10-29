//! Physical stage builder abstractions and implementations.
//!
//! Submodules translate logical plan operators into concrete physical stages:
//! - [`pipeline`]: Linear unbroken pipelines (`Scan -> Filter -> Project`).
//!
pub mod pipeline;
/// Registered lowering hooks for physical extension nodes.
pub mod extension;

pub use pipeline::{DefaultPipelineStageBuilder, PipelineStageBuilder};
