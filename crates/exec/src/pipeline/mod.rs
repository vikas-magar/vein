//! Core streaming pipeline primitives and abstractions.
//!
//! Provides materialized [`DataChunk`] and reference-backed [`DataChunkView`]
//! containers, plus the streaming execution node contracts ([`PipelineNode`],
//! [`Source`], [`Transform`], [`Sink`]).

pub mod chunk;
pub mod batch;
pub mod node;
pub mod view;

pub use batch::PipelineBatch;
pub use chunk::DataChunk;
pub use node::{PipelineNode, Sink, Source, Transform};
pub use view::{DataChunkView, RowMapping, SourceRow, ViewColumn, ViewColumnRef, ViewInputRows};
