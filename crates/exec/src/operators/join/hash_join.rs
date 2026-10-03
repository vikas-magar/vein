//! The join operator: a build sink and a probe node.
//!
//! A join is a pipeline breaker, so it is two nodes rather than one. [`BuildSink`]
//! absorbs the build side; the finished [`HashTable`] is handed to [`HashJoin`],
//! which probes. The runtime sequences them — drive the build side to
//! completion, then probe — which is precisely why a join cannot be a VM
//! instruction: the VM sees one chunk at a time and has no second input.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Instant;

use arrow::datatypes::DataType;

use crate::pipeline::{DataChunk, PipelineNode};
use common::Result;

use super::gather::{OutputColumn, assemble};
use super::hash_table::{HashTable, JoinKeys};
use super::rows::{JoinType, output_rows};

/// Consumes the build side into a hash table.
pub struct BuildSink {
    table: HashTable,
    /// Rows absorbed, and the time spent hashing and inserting them.
    rows: u64,
    insert_us: u64,
}

impl BuildSink {
    /// A sink that keys each build chunk on `build_keys` and will be probed on
    /// `probe_keys`.
    pub fn new(probe_keys: JoinKeys, build_keys: JoinKeys) -> Self {
        Self {
            table: HashTable::new(build_keys, probe_keys),
            rows: 0,
            insert_us: 0,
        }
    }

    /// How many build rows have been absorbed.
    pub fn rows(&self) -> usize {
        self.rows as usize
    }

    /// The finished table, ready to probe.
    pub fn finish(self) -> HashTable {
        self.table
    }
}

impl PipelineNode<DataChunk> for BuildSink {
    fn push(&mut self, chunk: DataChunk) -> Result<()> {
        self.rows += chunk.num_rows() as u64;
        let started = Instant::now();
        self.table.insert(chunk)?;
        self.insert_us += started.elapsed().as_micros() as u64;
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        tracing::info!(
            target: "vein.audit",
            event = "join_build_finished",
            rows = self.rows,
            table_rows = self.table.len(),
            build_chunks = self.table.chunk_count(),
            insert_us = self.insert_us,
            "join_build_finished"
        );
        Ok(())
    }
}

/// Probes a built table and emits the joined columns.
pub struct HashJoin {
    /// Shared rather than owned, so a probe fan-out can give every task the same
    /// table. Read-only by construction: `probe` takes `&self`.
    table: Arc<HashTable>,
    join_type: JoinType,
    columns: Vec<OutputColumn>,
    probe_types: Vec<DataType>,
    build_types: Vec<DataType>,
    next: Box<dyn PipelineNode<DataChunk>>,
    /// The build rows this task consumed, for the flavours that owe unmatched
    /// ones. Kept here, not in the table, because the table is shared.
    matched_build: HashSet<(u32, u32)>,
    /// The join's own accounting: rows in, matches found, and the time split
    /// between finding them and assembling the output.
    probed_rows: u64,
    matched_rows: u64,
    probe_us: u64,
    gather_us: u64,
}

impl HashJoin {
    /// Probes `table`, emitting `columns` assembled from the probe and build
    /// sides. `probe_types` and `build_types` are the output types of each side,
    /// which a null side needs and no data can always supply.
    pub fn new(
        table: Arc<HashTable>,
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
            matched_build: HashSet::new(),
            probed_rows: 0,
            matched_rows: 0,
            probe_us: 0,
            gather_us: 0,
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
        let probe_started = Instant::now();
        self.table.probe(&chunk, &mut pairs)?;
        self.probe_us += probe_started.elapsed().as_micros() as u64;
        let matches = pairs;
        // Only the flavours that owe unmatched build rows need this.
        if matches!(self.join_type, JoinType::Right | JoinType::Full) {
            self.matched_build
                .extend(matches.iter().map(|(_, build)| *build));
        }
        self.probed_rows += probe_rows.len() as u64;
        self.matched_rows += matches.len() as u64;

        // Unmatched *build* rows belong to the flush, not to this chunk.
        let gather_started = Instant::now();
        let rows = output_rows(self.join_type, &probe_rows, &matches, &[]);
        let mut produced = assemble(
            Some(&chunk),
            &self.table,
            &rows,
            &self.columns,
            &self.probe_types,
            &self.build_types,
        )?;
        let forwarded = self.forward(&mut produced);
        self.gather_us += gather_started.elapsed().as_micros() as u64;
        forwarded
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
            let unmatched: Vec<(u32, u32)> = self
                .table
                .all_build_rows()
                .into_iter()
                .filter(|row| !self.matched_build.contains(row))
                .collect();
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
        tracing::info!(
            target: "vein.audit",
            event = "join_finished",
            probed_rows = self.probed_rows,
            matched_rows = self.matched_rows,
            probe_us = self.probe_us,
            gather_us = self.gather_us,
            "join_finished"
        );
        self.next.flush()
    }

    /// Whether the pipeline is done is decided below the join, so the source's
    /// question has to pass through it.
    fn finished(&self) -> bool {
        self.next.finished()
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
            Arc::new(build(&[20, 30])),
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
            Arc::new(build(&[20, 40])),
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
