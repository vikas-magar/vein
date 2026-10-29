//! Resumable aggregate sink and output phases over the existing aggregate kernels.
mod sink;
mod source;
mod state;

pub use state::AggregatePipelineState;

#[cfg(test)]
#[path = "tests/mod.rs"]
mod tests;
