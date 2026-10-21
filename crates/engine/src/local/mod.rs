//! Single-node query orchestration with explicit provider composition.

mod builder;
mod components;
mod engine;
mod prepare;
mod query;
mod query_context;
mod stream;
mod tasks;
mod adaptive;
pub use adaptive::{AdaptiveBoundary, AdaptiveCheckpointPolicy, StatisticsCheckpointPolicy};
pub use builder::LocalEngineBuilder;
pub use components::LocalEngineComponents;
pub use engine::LocalQueryEngine;
pub use query::{LocalQuery, LocalResultStream};
pub use query_context::query_statistics_context;

#[cfg(test)]
mod tests;
