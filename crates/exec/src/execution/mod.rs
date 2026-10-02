//! Execution over the engine's own vector representation.
//!
//! Phase 1 of `vein_design/07-development-plan.md` exits when *"scan/filter/
//! project pipeline works; no unnecessary intermediate materialization"* — on
//! Vein's representation, not Arrow's. This module holds those operators.
//!
//! They sit beside the Arrow pipeline rather than replacing it, which is
//! possible because `PipelineNode<B>` is generic over the batch type: a chain of
//! `PipelineNode<vector::DataChunk>` coexists with the existing
//! `PipelineNode<DataChunk>` chain, so the two can be compared while the
//! migration runs (`09-testing-and-correctness.md` §4, Rule 33).
//!
//! Operators narrow the selection rather than rebuilding columns. A filter
//! writes no data, a projection copies handles, and a limit narrows a selection
//! — so the only materialisation in a filtered scan is the one the sink
//! performs on the rows that actually survive.

/// Narrows a chunk's selection to the rows a predicate accepts.
pub mod filter;
/// Narrows a chunk to a subset of its columns.
pub mod project;
/// Applies an offset and a row limit across the stream.
pub mod limit;
/// Virtual registers holding the engine's own values.
pub mod register;
/// The VIR interpreter over the engine's own representation.
pub mod vm;
/// A scan producing the engine's own representation.
pub mod source;

pub use filter::Filter;
pub use limit::Limit;
pub use project::Project;
pub use register::{BitMask, ColumnRef, RegisterFile, RegisterSlot};
pub use source::VectorScanSource;
pub use vm::{ProgramPredicate, execute_chunk};

use common::Result;
use vector::DataChunk;

/// Evaluates a predicate over a chunk, producing one bit per physical row.
///
/// This is the seam a VIR-compiled predicate plugs into. Until the VIR
/// compiler is ported, a predicate is any implementation of this trait, which
/// keeps the operators testable without one — and keeps the compiler out of the
/// operator's way, as `02-execution-and-vir-lld.md` §10 intends.
pub trait Predicate: Send + Sync {
    /// The condition for each physical row of `chunk`. An error means the
    /// predicate could not be evaluated at all, which is a failure rather than
    /// an empty result.
    fn evaluate(&self, chunk: &DataChunk) -> Result<BitMask>;
}

#[cfg(test)]
pub(crate) mod test_support {
    use std::sync::{Arc, Mutex};

    use common::Result;

    use crate::pipeline::PipelineNode;
    use vector::DataChunk;

    /// Terminal node that keeps every chunk it receives, for assertions.
    #[derive(Clone, Default)]
    pub struct CollectChunks(Arc<Mutex<Vec<DataChunk>>>);

    impl CollectChunks {
        /// The chunks received so far, in order.
        pub fn chunks(&self) -> Vec<DataChunk> {
            self.0.lock().expect("collect lock").clone()
        }

        /// Total active rows across the received chunks.
        pub fn active_rows(&self) -> u32 {
            self.chunks().iter().map(DataChunk::active_rows).sum()
        }

        /// Active rows of column `index`, in order, read as `i64`.
        pub fn i64_rows(&self, index: usize) -> Vec<i64> {
            let mut out = Vec::new();
            for chunk in self.chunks() {
                let view = chunk.view(index).expect("column exists");
                let values = view.resolve_i64().expect("i64 column");
                out.extend(view.rows().filter_map(|row| values.at(row)));
            }
            out
        }
    }

    impl PipelineNode<DataChunk> for CollectChunks {
        fn push(&mut self, chunk: DataChunk) -> Result<()> {
            self.0.lock().expect("collect lock").push(chunk);
            Ok(())
        }

        fn flush(&mut self) -> Result<()> {
            Ok(())
        }
    }

    /// A predicate reading one `i64` column and comparing it to a threshold.
    pub struct GreaterThan {
        /// Column to read.
        pub column: usize,
        /// Exclusive lower bound.
        pub threshold: i64,
    }

    impl super::Predicate for GreaterThan {
        fn evaluate(&self, chunk: &DataChunk) -> common::Result<super::register::BitMask> {
            let values = chunk
                .view(self.column)
                .and_then(|view| view.resolve_i64())
                .ok_or_else(|| {
                    error::VeinError::internal(format!(
                        "column {} is not readable as Int64",
                        self.column
                    ))
                })?;
            let words = (chunk.row_count() as usize).div_ceil(64);
            let mut values_bits = vec![0u64; words];
            let mut valid = vec![0u64; words];
            for row in 0..chunk.row_count() {
                // `at` reads whichever encoding the column has, so the predicate
                // never forces a flat representation (Rule 5).
                let Some(value) = values.at(row) else {
                    continue;
                };
                let bit = 1u64 << (row % 64);
                valid[row as usize / 64] |= bit;
                if value > self.threshold {
                    values_bits[row as usize / 64] |= bit;
                }
            }
            Ok(super::register::BitMask {
                values: values_bits.into(),
                valid: valid.into(),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use vector::{Selection, Vector, VectorRef};

    use super::test_support::{CollectChunks, GreaterThan};
    use super::*;
    use crate::pipeline::PipelineNode;

    /// Six rows in one chunk: `id` 10..60 and a matching label.
    fn source_chunk() -> DataChunk {
        let ids: VectorRef = Arc::new(Vector::from_i64(&[10, 20, 30, 40, 50, 60]));
        let labels: VectorRef =
            Arc::new(Vector::from_utf8(&["a", "b", "c", "d", "e", "f"]));
        DataChunk::new(6, vec![ids, labels], Selection::all()).expect("consistent columns")
    }

    /// The Phase 1 exit criterion, on the engine's own representation: a
    /// filter, a projection and a limit composed into one pipeline.
    #[test]
    fn filter_project_limit_pipeline() {
        let sink = CollectChunks::default();
        // limit -> project -> filter -> sink, built back to front.
        let limit = Limit::new(1, 2, Box::new(sink.clone()));
        let project = Project::new(vec![0], Box::new(limit));
        let filter = Filter::new(GreaterThan { column: 0, threshold: 20 }, Box::new(project));
        let mut pipeline = filter;

        pipeline.push(source_chunk()).expect("push");
        pipeline.flush().expect("flush");

        // id > 20 keeps 30, 40, 50, 60; the offset drops 30 and the limit takes
        // 40 and 50.
        assert_eq!(sink.active_rows(), 2);
        assert_eq!(sink.i64_rows(0), vec![40, 50]);
    }

    #[test]
    fn a_filter_that_rejects_every_row_emits_nothing() {
        let sink = CollectChunks::default();
        let mut pipeline = Filter::new(
            GreaterThan {
                column: 0,
                threshold: 1000,
            },
            Box::new(sink.clone()),
        );
        pipeline.push(source_chunk()).expect("push");
        pipeline.flush().expect("flush");
        assert!(sink.chunks().is_empty(), "no empty chunk may reach the sink");
    }

    #[test]
    fn filters_compose_as_intersections() {
        let sink = CollectChunks::default();
        let inner = Filter::new(GreaterThan { column: 0, threshold: 20 }, Box::new(sink.clone()));
        let mut outer = Filter::new(GreaterThan { column: 0, threshold: 40 }, Box::new(inner));
        outer.push(source_chunk()).expect("push");
        outer.flush().expect("flush");

        // The second filter only sees the rows the first kept.
        assert_eq!(sink.i64_rows(0), vec![50, 60]);
    }
}
