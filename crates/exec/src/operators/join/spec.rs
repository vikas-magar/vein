//! What the planner decides about a join.
//!
//! Everything the operator needs that is not data: which columns key each side,
//! which flavour of join, how the output columns interleave, and the type of
//! each side. Keeping it as one plain object matters for the wiring — the
//! planner builds a `JoinSpec`, the runtime turns it into the two halves of the
//! breaker, and neither has to know the other's internals.
//!
//! It is deliberately free of anything execution-specific, so it is also the
//! shape that can be serialised into a task once `ExecuteTask` grows a join
//! input.

use std::sync::Arc;

use arrow::datatypes::DataType;

use crate::pipeline::{DataChunkView, PipelineNode};

use super::hash_join::{BuildSink, HashJoin};
use super::hash_table::{HashTable, JoinKeys};
use super::rows::JoinType;

/// Maps an output column to its source side without materializing its values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputColumn {
    /// A column of the probe view.
    Probe(usize),
    /// A column present in every retained build view.
    Build(usize),
}

/// Everything a hash join needs that the planner decides.
#[derive(Debug, Clone)]
pub struct JoinSpec {
    /// Key columns of a probe chunk.
    pub probe_keys: JoinKeys,
    /// Key columns of a build chunk, positionally paired with `probe_keys`.
    pub build_keys: JoinKeys,
    /// Which flavour of join to perform.
    pub join_type: JoinType,
    /// Output columns, in output order.
    pub columns: Vec<OutputColumn>,
    /// Data type of every probe column the output may reference.
    pub probe_types: Vec<DataType>,
    /// Data type of every build column the output may reference.
    pub build_types: Vec<DataType>,
}

impl JoinSpec {
    /// The build side's sink — the first half of the breaker.
    pub fn build_sink(&self) -> BuildSink {
        BuildSink::new(self.probe_keys.clone(), self.build_keys.clone())
    }

    /// The probe side's node, given the table the build side produced — the
    /// second half.
    pub fn into_probe(
        self,
        table: Arc<HashTable>,
        next: Box<dyn PipelineNode<DataChunkView>>,
    ) -> HashJoin {
        HashJoin::new(
            table,
            self.join_type,
            self.columns,
            self.probe_types,
            self.build_types,
            next,
        )
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use arrow::array::{Array, ArrayRef, Int64Array};
    use crate::pipeline::DataChunkView;

    use super::*;

    #[derive(Clone, Default)]
    struct Collect(Arc<Mutex<Vec<DataChunkView>>>);

    impl PipelineNode<DataChunkView> for Collect {
        fn push(&mut self, chunk: DataChunkView) -> Result<(), error::VeinError> {
            self.0.lock().expect("collect lock").push(chunk);
            Ok(())
        }

        fn flush(&mut self) -> Result<(), error::VeinError> {
            Ok(())
        }
    }

    fn ints(values: &[i64]) -> ArrayRef {
        Arc::new(Int64Array::from(values.to_vec()))
    }

    fn cells(chunk: &DataChunkView, column: usize) -> Vec<Option<i64>> {
        let ints = crate::pipeline::test_support::materialized_column::<Int64Array>(chunk, column);
        (0..ints.len())
            .map(|row| ints.is_valid(row).then(|| ints.value(row)))
            .collect()
    }

    /// A spec is enough to build a working join: the planner's description, the
    /// two halves, and nothing else.
    ///
    /// The key sits at column 1 on the build side and column 0 on the probe, so
    /// this also proves the two sides' key positions travel through the spec
    /// rather than being assumed equal.
    #[test]
    fn a_spec_builds_a_working_join() {
        let spec = JoinSpec {
            probe_keys: JoinKeys::new(vec![0]),
            build_keys: JoinKeys::new(vec![1]),
            join_type: JoinType::Inner,
            columns: vec![OutputColumn::Probe(0), OutputColumn::Build(0)],
            probe_types: vec![DataType::Int64],
            build_types: vec![DataType::Int64],
        };

        let mut build = spec.build_sink();
        build
            .push(crate::pipeline::test_support::dense_view(vec![
                ints(&[100, 200]),
                ints(&[7, 8]),
            ]))
            .expect("build");
        let table = build.finish();

        let sink = Collect::default();
        let mut probe = spec.into_probe(Arc::new(table), Box::new(sink.clone()));
        probe
            .push(crate::pipeline::test_support::dense_view(vec![ints(&[8])]))
            .expect("probe");
        probe.flush().expect("flush");

        let chunks = sink.0.lock().expect("collect lock");
        let probe_side: Vec<Option<i64>> = chunks.iter().flat_map(|c| cells(c, 0)).collect();
        let build_side: Vec<Option<i64>> = chunks.iter().flat_map(|c| cells(c, 1)).collect();
        assert_eq!(probe_side, vec![Some(8)]);
        assert_eq!(build_side, vec![Some(200)], "key 8 is the build's row 1");
    }
}
