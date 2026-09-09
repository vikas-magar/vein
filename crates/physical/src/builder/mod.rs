//! Physical stage builder abstractions and implementations.
//!
//! Submodules translate logical plan operators into concrete physical stages:
//! - [`pipeline`]: Linear unbroken pipelines (`Scan -> Filter -> Project`).
//! - [`aggregate`]: Distributed two-phase Map/Reduce aggregations.

pub mod aggregate;
pub mod pipeline;

pub use aggregate::{AggregateStageBuilder, DefaultAggregateStageBuilder};
pub use pipeline::{DefaultPipelineStageBuilder, PipelineStageBuilder};
