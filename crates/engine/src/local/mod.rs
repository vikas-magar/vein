//! Single-node query orchestration with explicit provider composition.

mod builder;
mod components;
mod engine;
mod prepare;
mod query;
mod stream;
mod tasks;
mod adaptive;
pub use adaptive::{AdaptiveBoundary, AdaptiveCheckpointPolicy, StatisticsCheckpointPolicy};
pub use builder::LocalEngineBuilder;
pub use components::LocalEngineComponents;
pub use engine::LocalQueryEngine;
pub use query::{query_statistics_context, LocalQuery, LocalResultStream};

#[cfg(test)]
mod tests;
