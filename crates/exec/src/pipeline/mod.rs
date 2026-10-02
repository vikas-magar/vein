//! Core streaming pipeline primitives and abstractions.
//!
//! Provides the fundamental columnar data container [`DataChunk`] and the
//! streaming execution node contracts ([`PipelineNode`], [`Source`], [`Transform`], [`Sink`]).

pub mod chunk;
pub mod node;

pub use chunk::DataChunk;
pub use node::{PipelineNode, Sink, Source, Transform};
