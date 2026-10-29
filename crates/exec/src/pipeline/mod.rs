//! Core streaming pipeline primitives and abstractions.
//!
//! Provides the reference-backed [`DataChunkView`] container and streaming
//! execution node contracts ([`PipelineNode`], [`Source`], [`Transform`], [`Sink`]).

pub mod node;
pub mod delivery;
pub mod arrow_adapter;
pub mod resumable;
pub mod view;

pub use node::{PipelineNode, Sink, Source, Transform};
pub use view::{DataChunkView, MaterializationReport, RowMapping, SourceRow, ViewColumn, ViewColumnRef, ViewInputRows};

#[cfg(test)]
pub(crate) mod test_support;
