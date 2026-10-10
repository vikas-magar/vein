//! The join operator: a build sink and a probe node.
//!
//! A join is a pipeline breaker, so it is two nodes rather than one. [`BuildSink`]
//! absorbs the build side; the finished [`HashTable`] is handed to [`HashJoin`],
//! which probes. The runtime sequences them — drive the build side to
//! completion, then probe — which is precisely why a join cannot be a VM
//! instruction: the VM sees one chunk at a time and has no second input.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use arrow::datatypes::DataType;

use crate::pipeline::{DataChunk, PipelineNode, Transform};
use common::arrow::SelectionVector;
use common::Result;
use error::VeinError;

use super::gather::{OutputColumn, assemble};
use super::hash_table::{HashTable, JoinKeys};
use super::rows::{JoinType, existence_probe_rows, output_rows};

/// Collects the selection left by a VM join-residual filter.
#[derive(Clone, Default)]
pub struct JoinFilterCollector {
    output: Arc<Mutex<Option<DataChunk>>>,
}

impl JoinFilterCollector {
    /// Clears the previous candidate batch before evaluating another one.
    pub fn clear(&self) -> Result<()> {
        *self.output.lock().map_err(|_| VeinError::internal("join filter collector poisoned"))? = None;
        Ok(())
    }

    /// Takes the filtered candidate batch, if the predicate retained any rows.
    pub fn take(&self) -> Result<Option<DataChunk>> {
        Ok(self.output.lock().map_err(|_| VeinError::internal("join filter collector poisoned"))?.take())
    }
}

impl PipelineNode<DataChunk> for JoinFilterCollector {
    fn push(&mut self, chunk: DataChunk) -> Result<()> {
        *self.output.lock().map_err(|_| VeinError::internal("join filter collector poisoned"))? = Some(chunk);
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        Ok(())
    }
}

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
    residual_columns: Vec<OutputColumn>,
    residual_vm: Option<Box<dyn Transform<DataChunk>>>,
    residual_output: Option<JoinFilterCollector>,
    /// The build rows this task consumed, for the flavours that owe unmatched
    /// ones. Kept here, not in the table, because the table is shared.
    matched_build: HashSet<(u32, u32)>,
    /// The join's own accounting: rows in, matches found, and the time split
    /// between finding them and assembling the output.
    probed_rows: u64,
    matched_rows: u64,
    probe_us: u64,
    gather_us: u64,
    residual_us: u64,
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
            residual_columns: Vec::new(),
            residual_vm: None,
            residual_output: None,
            matched_build: HashSet::new(),
            probed_rows: 0,
            matched_rows: 0,
            probe_us: 0,
            gather_us: 0,
            residual_us: 0,
        }
    }

    /// Installs a vectorized residual predicate that filters hash candidates
    /// before the join decides which rows are unmatched.
    pub fn with_residual_filter(
        mut self,
        columns: Vec<OutputColumn>,
        vm: Box<dyn Transform<DataChunk>>,
        output: JoinFilterCollector,
    ) -> Self {
        self.residual_columns = columns;
        self.residual_vm = Some(vm);
        self.residual_output = Some(output);
        self
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
        let mut matches = pairs;
        let residual_started = Instant::now();
        if let (Some(vm), Some(output)) = (&mut self.residual_vm, &self.residual_output) {
            let candidates = matches
                .iter()
                .map(|(probe, build)| super::rows::OutputRow {
                    probe: Some(*probe),
                    build: Some(*build),
                })
                .collect::<Vec<_>>();
            let mut candidate_chunks = assemble(
                Some(&chunk),
                &self.table,
                &candidates,
                &self.residual_columns,
                &self.probe_types,
                &self.build_types,
            )?;
            output.clear()?;
            if let Some(candidate) = candidate_chunks.pop() {
                vm.push(candidate)?;
                let filtered = output.take()?;
                let accepted = filtered
                    .map(|candidate| {
                        (0..candidate.selection.size())
                            .map(|position| candidate.selection.get_index(position))
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default();
                matches = accepted
                    .into_iter()
                    .filter_map(|index| matches.get(index).copied())
                    .collect();
            } else {
                matches.clear();
            }
        }
        let residual_us = residual_started.elapsed().as_micros() as u64;
        // Only the flavours that owe unmatched build rows need this.
        if matches!(self.join_type, JoinType::Right | JoinType::Full) {
            self.matched_build
                .extend(matches.iter().map(|(_, build)| *build));
        }
        self.probed_rows += probe_rows.len() as u64;
        self.matched_rows += matches.len() as u64;

        if matches!(self.join_type, JoinType::Semi | JoinType::Anti) {
            let gather_started = Instant::now();
            let selected_rows = existence_probe_rows(self.join_type, &probe_rows, &matches);
            let vectors = self
                .columns
                .iter()
                .map(|column| match column {
                    OutputColumn::Probe(index) => Ok(Arc::clone(chunk.column(*index))),
                    OutputColumn::Build(_) => Err(VeinError::internal(
                        "semi/anti join output cannot contain build-side columns",
                    )),
                })
                .collect::<Result<Vec<_>>>()?;
            let output = DataChunk::new(vectors, SelectionVector::new_sparse(selected_rows));
            let mut produced = if output.is_empty() { Vec::new() } else { vec![output] };
            let forwarded = self.forward(&mut produced);
            self.gather_us += gather_started.elapsed().as_micros() as u64;
            self.residual_us += residual_us;
            return forwarded;
        }

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
        self.residual_us += residual_us;
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
            residual_us = self.residual_us,
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

    fn selected_cells(chunk: &DataChunk, column: usize) -> Vec<Option<i64>> {
        let array = chunk
            .column(column)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("int64 output");
        (0..chunk.selection.size())
            .map(|logical_row| {
                let physical_row = chunk.selection.get_index(logical_row);
                array.is_valid(physical_row).then(|| array.value(physical_row))
            })
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

    #[test]
    fn semi_join_emits_a_probe_row_once_despite_duplicate_build_matches() {
        let sink = Collect::default();
        let mut join = HashJoin::new(
            Arc::new(build(&[20, 20, 30])),
            JoinType::Semi,
            vec![OutputColumn::Probe(0)],
            vec![DataType::Int64],
            vec![DataType::Int64],
            Box::new(sink.clone()),
        );

        join.push(DataChunk::from_vectors(vec![ints(&[20, 10, 30])]))
            .expect("probe");
        join.flush().expect("flush");

        let chunks = sink.0.lock().expect("collect lock");
        let rows: Vec<Option<i64>> = chunks.iter().flat_map(|chunk| selected_cells(chunk, 0)).collect();
        assert_eq!(rows, vec![Some(20), Some(30)]);
        assert!(chunks.iter().all(|chunk| chunk.num_columns() == 1));
    }

    #[test]
    fn semi_join_reuses_probe_arrays_and_keeps_sparse_selection_indices() {
        use common::arrow::SelectionVector;

        let sink = Collect::default();
        let probe_values = ints(&[10, 20, 30, 40]);
        let probe = DataChunk::new(
            vec![Arc::clone(&probe_values)],
            SelectionVector::new_sparse(vec![1, 3]),
        );
        let mut join = HashJoin::new(
            Arc::new(build(&[40, 40])),
            JoinType::Semi,
            vec![OutputColumn::Probe(0)],
            vec![DataType::Int64],
            vec![DataType::Int64],
            Box::new(sink.clone()),
        );

        join.push(probe).expect("probe");
        join.flush().expect("flush");

        let chunks = sink.0.lock().expect("collect lock");
        assert_eq!(chunks.len(), 1);
        assert!(Arc::ptr_eq(chunks[0].column(0), &probe_values));
        assert_eq!(chunks[0].selection.size(), 1);
        assert_eq!(chunks[0].selection.get_index(0), 3);
        assert_eq!(selected_cells(&chunks[0], 0), vec![Some(40)]);
    }

    #[test]
    fn anti_join_reuses_probe_arrays_and_emits_only_unmatched_selected_rows() {
        use common::arrow::SelectionVector;

        let sink = Collect::default();
        let probe_values = ints(&[10, 20, 30, 40]);
        let probe = DataChunk::new(
            vec![Arc::clone(&probe_values)],
            SelectionVector::new_sparse(vec![1, 3]),
        );
        let mut join = HashJoin::new(
            Arc::new(build(&[40])),
            JoinType::Anti,
            vec![OutputColumn::Probe(0)],
            vec![DataType::Int64],
            vec![DataType::Int64],
            Box::new(sink.clone()),
        );

        join.push(probe).expect("probe");
        join.flush().expect("flush");

        let chunks = sink.0.lock().expect("collect lock");
        assert_eq!(chunks.len(), 1);
        assert!(Arc::ptr_eq(chunks[0].column(0), &probe_values));
        assert_eq!(selected_cells(&chunks[0], 0), vec![Some(20)]);
        assert_eq!(chunks[0].selection.get_index(0), 1);
    }

    #[test]
    fn semi_join_emits_no_chunk_when_no_probe_key_matches() {
        let sink = Collect::default();
        let mut join = HashJoin::new(
            Arc::new(build(&[20])),
            JoinType::Semi,
            vec![OutputColumn::Probe(0)],
            vec![DataType::Int64],
            vec![DataType::Int64],
            Box::new(sink.clone()),
        );

        join.push(DataChunk::from_vectors(vec![ints(&[10, 30])]))
            .expect("probe");
        join.flush().expect("flush");

        assert!(sink.0.lock().expect("collect lock").is_empty());
    }

    #[test]
    fn anti_join_keeps_nonmatching_and_null_probe_keys() {
        let sink = Collect::default();
        let mut join = HashJoin::new(
            Arc::new(build(&[20, 20])),
            JoinType::Anti,
            vec![OutputColumn::Probe(0)],
            vec![DataType::Int64],
            vec![DataType::Int64],
            Box::new(sink.clone()),
        );

        join.push(DataChunk::from_vectors(vec![Arc::new(Int64Array::from(vec![
            Some(20), Some(10), None,
        ]))]))
            .expect("probe");
        join.flush().expect("flush");

        let chunks = sink.0.lock().expect("collect lock");
        let rows: Vec<Option<i64>> = chunks.iter().flat_map(|chunk| selected_cells(chunk, 0)).collect();
        assert_eq!(rows, vec![Some(10), None]);
        assert!(chunks.iter().all(|chunk| chunk.num_columns() == 1));
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
