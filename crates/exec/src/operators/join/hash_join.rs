//! The join operator: a build sink and a probe node.
//!
//! A join is a pipeline breaker, so it is two nodes rather than one. [`BuildSink`]
//! absorbs the build side; the finished [`HashTable`] is handed to [`HashJoin`],
//! which probes. The runtime sequences them — drive the build side to
//! completion, then probe — which is precisely why a join cannot be a VM
//! instruction: the VM sees one chunk at a time and has no second input.

use arrow::datatypes::DataType;

use crate::pipeline::{DataChunk, PipelineNode};
use common::Result;

use super::gather::{OutputColumn, assemble};
use super::hash_table::{HashTable, JoinKeys};
use super::rows::{JoinType, output_rows};

/// Consumes the build side into a hash table.
pub struct BuildSink {
    table: HashTable,
}

impl BuildSink {
    /// A sink that keys each build chunk on `build_keys` and will be probed on
    /// `probe_keys`.
    pub fn new(probe_keys: JoinKeys, build_keys: JoinKeys) -> Self {
        Self {
            table: HashTable::new(build_keys, probe_keys),
        }
    }

    /// How many build rows have been absorbed.
    pub fn rows(&self) -> usize {
        self.table.len()
    }

    /// The finished table, ready to probe.
    pub fn finish(self) -> HashTable {
        self.table
    }
}

impl PipelineNode<DataChunk> for BuildSink {
    fn push(&mut self, chunk: DataChunk) -> Result<()> {
        self.table.insert(chunk)
    }

    fn flush(&mut self) -> Result<()> {
        Ok(())
    }
}

/// Probes a built table and emits the joined columns.
pub struct HashJoin {
    table: HashTable,
    join_type: JoinType,
    columns: Vec<OutputColumn>,
    probe_types: Vec<DataType>,
    build_types: Vec<DataType>,
    next: Box<dyn PipelineNode<DataChunk>>,
}

impl HashJoin {
    /// Probes `table`, emitting `columns` assembled from the probe and build
    /// sides. `probe_types` and `build_types` are the output types of each side,
    /// which a null side needs and no data can always supply.
    pub fn new(
        table: HashTable,
        join_type: JoinType,
        columns: Vec<OutputColumn>,
        probe_types: Vec<DataType>,
        build_types: Vec<DataType>,
        next: Box<dyn PipelineNode<DataChunk>>,
    ) -> Self {
        Self {
            table,
            join_type,
            columns,
            probe_types,
            build_types,
            next,
        }
    }

    /// Probes one chunk and forwards everything it produced.
    fn probe(&mut self, chunk: DataChunk) -> Result<()> {
        if chunk.is_empty() {
            return Ok(());
        }
        let probe_rows: Vec<u32> = (0..chunk.selection.size())
            .map(|position| chunk.selection.get_index(position) as u32)
            .collect();

        let mut pairs = Vec::new();
        self.table.probe(&chunk, &mut pairs)?;
        let matches: Vec<(u32, (u32, u32))> = pairs
            .iter()
            .map(|(probe_row, entry)| (*probe_row, self.table.build_row(*entry)))
            .collect();

        // Unmatched *build* rows belong to the flush, not to this chunk.
        let rows = output_rows(self.join_type, &probe_rows, &matches, &[]);
        let mut produced = assemble(
            Some(&chunk),
            &self.table,
            &rows,
            &self.columns,
            &self.probe_types,
            &self.build_types,
        )?;
        self.forward(&mut produced)
    }

    /// Forwards produced chunks, dropping any that carry no rows.
    fn forward(&mut self, chunks: &mut Vec<DataChunk>) -> Result<()> {
        for chunk in chunks.drain(..) {
            if !chunk.is_empty() {
                self.next.push(chunk)?;
            }
        }
        Ok(())
    }
}

impl PipelineNode<DataChunk> for HashJoin {
    fn push(&mut self, chunk: DataChunk) -> Result<()> {
        self.probe(chunk)
    }

    fn flush(&mut self) -> Result<()> {
        // An outer join owes the build rows nothing matched. They are emitted
        // here, once, with the probe side null.
        if matches!(self.join_type, JoinType::Right | JoinType::Full) {
            let unmatched = self.table.unmatched_build_rows();
            if !unmatched.is_empty() {
                let rows = output_rows(self.join_type, &[], &[], &unmatched);
                let mut produced = assemble(
                    None,
                    &self.table,
                    &rows,
                    &self.columns,
                    &self.probe_types,
                    &self.build_types,
                )?;
                self.forward(&mut produced)?;
            }
        }
        self.next.flush()
    }
}

impl crate::pipeline::Transform<DataChunk> for HashJoin {}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use arrow::array::{Array, ArrayRef, Int64Array};

    use super::*;

    #[derive(Clone, Default)]
    struct Collect(Arc<Mutex<Vec<DataChunk>>>);

    impl PipelineNode<DataChunk> for Collect {
        fn push(&mut self, chunk: DataChunk) -> Result<()> {
            self.0.lock().expect("collect lock").push(chunk);
            Ok(())
        }

        fn flush(&mut self) -> Result<()> {
            Ok(())
        }
    }

    fn ints(values: &[i64]) -> ArrayRef {
        Arc::new(Int64Array::from(values.to_vec()))
    }

    fn cells(chunk: &DataChunk, column: usize) -> Vec<Option<i64>> {
        let array = chunk.column(column);
        let ints = array
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("int64 output");
        (0..ints.len())
            .map(|row| ints.is_valid(row).then(|| ints.value(row)))
            .collect()
    }

    /// A built table over one chunk of `values`.
    fn build(values: &[i64]) -> HashTable {
        let mut sink = BuildSink::new(JoinKeys::new(vec![0]), JoinKeys::new(vec![0]));
        sink.push(DataChunk::from_vectors(vec![ints(values)]))
            .expect("build");
        sink.finish()
    }

    /// The whole operator: absorb a build side, probe it, emit joined columns.
    #[test]
    fn inner_join_end_to_end() {
        let sink = Collect::default();
        let mut join = HashJoin::new(
            build(&[20, 30]),
            JoinType::Inner,
            vec![OutputColumn::Probe(0), OutputColumn::Build(0)],
            vec![DataType::Int64],
            vec![DataType::Int64],
            Box::new(sink.clone()),
        );

        join.push(DataChunk::from_vectors(vec![ints(&[10, 20])]))
            .expect("probe");
        join.flush().expect("flush");

        let chunks = sink.0.lock().expect("collect lock");
        let all: Vec<Option<i64>> = chunks.iter().flat_map(|chunk| cells(chunk, 0)).collect();
        assert_eq!(all, vec![Some(20)], "only the matching probe row survives");
    }

    /// A right join owes its unmatched build rows, and emits them at flush with
    /// a null probe side.
    #[test]
    fn right_join_emits_unmatched_build_rows_at_flush() {
        let sink = Collect::default();
        let mut join = HashJoin::new(
            build(&[20, 40]),
            JoinType::Right,
            vec![OutputColumn::Probe(0), OutputColumn::Build(0)],
            vec![DataType::Int64],
            vec![DataType::Int64],
            Box::new(sink.clone()),
        );

        join.push(DataChunk::from_vectors(vec![ints(&[20])]))
            .expect("probe");
        join.flush().expect("flush");

        let chunks = sink.0.lock().expect("collect lock");
        let probe: Vec<Option<i64>> = chunks.iter().flat_map(|c| cells(c, 0)).collect();
        let build: Vec<Option<i64>> = chunks.iter().flat_map(|c| cells(c, 1)).collect();
        assert_eq!(probe, vec![Some(20), None], "the match, then a null probe row");
        assert_eq!(build, vec![Some(20), Some(40)], "build 40 matched nothing");
    }
}
