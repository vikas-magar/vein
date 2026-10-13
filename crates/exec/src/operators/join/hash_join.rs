//! The join operator: a build sink and a probe node.
//!
//! A join is a pipeline breaker, so it is two nodes rather than one. [`BuildSink`]
//! absorbs the build side; the finished [`HashTable`] is handed to [`HashJoin`],
//! which probes. The runtime sequences them — drive the build side to
//! completion, then probe — which is precisely why a join cannot be a VM
//! instruction: the VM sees one chunk at a time and has no second input.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use arrow::datatypes::DataType;

use crate::memory::QueryMemoryTracker;
use crate::pipeline::{DataChunkView, MaterializationReport, PipelineNode, Transform, ViewColumnRef, ViewInputRows};
use common::Result;
use error::VeinError;

use super::spec::OutputColumn;
use super::hash_table::{HashTable, JoinKeys};
use super::spill::{JoinSpillRun, JoinSpillSchema, JoinSpillWriter};
use super::rows::{JoinType, output_rows};

/// Collects the selection left by a VM join-residual filter.
#[derive(Clone, Default)]
pub struct JoinFilterCollector {
    output: Arc<Mutex<Option<DataChunkView>>>,
}

impl JoinFilterCollector {
    /// Clears the previous candidate batch before evaluating another one.
    pub fn clear(&self) -> Result<()> {
        *self.output.lock().map_err(|_| VeinError::internal("join filter collector poisoned"))? = None;
        Ok(())
    }

    /// Takes the filtered candidate batch, if the predicate retained any rows.
    pub fn take(&self) -> Result<Option<DataChunkView>> {
        Ok(self.output.lock().map_err(|_| VeinError::internal("join filter collector poisoned"))?.take())
    }
}

impl PipelineNode<DataChunkView> for JoinFilterCollector {
    fn push(&mut self, view: DataChunkView) -> Result<()> {
        *self.output.lock().map_err(|_| VeinError::internal("join filter collector poisoned"))? = Some(view);
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        Ok(())
    }
}

/// Consumes the build side into a hash table.
pub struct BuildSink {
    table: Option<HashTable>,
    spill_writer: Option<JoinSpillWriter>,
    spill_schema: Option<JoinSpillSchema>,
    spill_backup: Vec<DataChunkView>,
    build_keys: JoinKeys,
    tracker: QueryMemoryTracker,
    /// Rows absorbed, and the time spent hashing and inserting them.
    rows: u64,
    insert_us: u64,
}

impl BuildSink {
    /// A sink that keys each build chunk on `build_keys` and will be probed on
    /// `probe_keys`.
    pub fn new(probe_keys: JoinKeys, build_keys: JoinKeys) -> Self {
        Self::with_memory_tracker(
            probe_keys,
            build_keys,
            QueryMemoryTracker::unlimited(common::cancellation::CancellationToken::new()),
        )
            .expect("unlimited join memory budget must create a build sink")
    }

    /// Create a build sink that charges its index to `tracker`.
    pub fn with_memory_tracker(
        probe_keys: JoinKeys,
        build_keys: JoinKeys,
        tracker: QueryMemoryTracker,
    ) -> Result<Self> {
        let table = HashTable::with_memory_tracker(build_keys.clone(), probe_keys, tracker.clone())?;
        Ok(Self {
            table: Some(table),
            spill_writer: None,
            spill_schema: None,
            spill_backup: Vec::new(),
            build_keys,
            tracker,
            rows: 0,
            insert_us: 0,
        })
    }

    /// Create a build sink that falls back to an IPC run when retained build
    /// state exceeds the query's memory budget.
    pub(crate) fn with_spill_support(
        probe_keys: JoinKeys,
        build_keys: JoinKeys,
        build_types: &[DataType],
        tracker: QueryMemoryTracker,
    ) -> Result<Self> {
        let schema = JoinSpillSchema::new(build_types);
        let (table, spill_writer) = match HashTable::with_memory_tracker(
            build_keys.clone(),
            probe_keys,
            tracker.clone(),
        ) {
            Ok(table) => (Some(table), None),
            Err(VeinError::ResourceExhausted { .. }) => {
                (None, Some(JoinSpillWriter::new(tracker.clone(), schema.clone())?))
            }
            Err(error) => return Err(error),
        };
        Ok(Self {
            table,
            spill_writer,
            spill_schema: Some(schema),
            spill_backup: Vec::new(),
            build_keys,
            tracker,
            rows: 0,
            insert_us: 0,
        })
    }

    /// How many build rows have been absorbed.
    pub fn rows(&self) -> usize {
        self.rows as usize
    }

    /// The finished table, ready to probe.
    pub fn finish(self) -> HashTable {
        self.table.expect("in-memory build sink cannot finish after spilling")
    }

    /// Finish either the retained hash table or its spill run.
    pub(crate) fn finish_with_spill(mut self) -> Result<BuildResult> {
        if let Some(table) = self.table.take() {
            return Ok(BuildResult::InMemory(table));
        }
        let run = self
            .spill_writer
            .take()
            .ok_or_else(|| VeinError::internal("spilled join build has no writer"))?
            .finish()?;
        Ok(BuildResult::Spilled {
            run,
            schema: self
                .spill_schema
                .take()
                .ok_or_else(|| VeinError::internal("spilled join build has no schema"))?,
        })
    }
}

pub(crate) enum BuildResult {
    InMemory(HashTable),
    Spilled { run: JoinSpillRun, schema: JoinSpillSchema },
}

impl PipelineNode<DataChunkView> for BuildSink {
    fn push(&mut self, view: DataChunkView) -> Result<()> {
        self.rows += view.num_rows() as u64;
        let started = Instant::now();
        if let Some(writer) = self.spill_writer.as_mut() {
            writer.push(&view, &self.build_keys)?;
        } else {
            let table = self
                .table
                .as_mut()
                .ok_or_else(|| VeinError::internal("join build table is unavailable"))?;
            match table.insert_view(view.clone()) {
                Ok(()) => self.spill_backup.push(view),
                Err(VeinError::ResourceExhausted { .. }) if self.spill_schema.is_some() => {
                    let schema = self
                        .spill_schema
                        .as_ref()
                        .ok_or_else(|| VeinError::internal("join spill schema is unavailable"))?
                        .clone();
                    let mut writer = JoinSpillWriter::new(self.tracker.clone(), schema)?;
                    for retained in &self.spill_backup {
                        writer.push(retained, &self.build_keys)?;
                    }
                    writer.push(&view, &self.build_keys)?;
                    self.spill_writer = Some(writer);
                    self.spill_backup.clear();
                    self.table = None;
                    tracing::info!(
                        target: "vein.audit",
                        event = "join_build_spill_started",
                        rows = self.rows,
                        "join_build_spill_started"
                    );
                }
                Err(error) => return Err(error),
            }
        }
        self.insert_us += started.elapsed().as_micros() as u64;
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        if let Some(writer) = self.spill_writer.as_ref() {
            tracing::info!(
                target: "vein.audit",
                event = "join_build_spill_finished",
                rows = self.rows,
                spill_rows = writer.rows(),
                insert_us = self.insert_us,
                "join_build_spill_finished"
            );
            return Ok(());
        }
        let table = self
            .table
            .as_ref()
            .ok_or_else(|| VeinError::internal("join build table is unavailable at flush"))?;
        let (keys, key_materialize_us) = table.build_key_materialization();
        tracing::info!(
            target: "vein.audit",
            event = "join_build_finished",
            rows = self.rows,
            table_rows = table.len(),
            build_chunks = table.chunk_count(),
            insert_us = self.insert_us,
            key_shared_columns = keys.shared_columns,
            key_gathered_columns = keys.gathered_columns,
            key_empty_columns = keys.empty_columns,
            key_gathered_output_buffer_bytes = keys.gathered_output_buffer_bytes,
            key_materialize_us,
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
    output_columns: Vec<ViewColumnRef>,
    probe_types: Vec<DataType>,
    build_types: Vec<DataType>,
    next: Box<dyn PipelineNode<DataChunkView>>,
    /// Reused buckets avoid rebuilding a hash map and per-group vectors for
    /// every probe batch. Keys are retained build-chunk IDs; None is the
    /// null-extended group used by outer joins.
    rows_by_build_chunk: HashMap<Option<u32>, Vec<super::rows::OutputRow>>,
    build_chunk_order: Vec<Option<u32>>,
    probe_rows: Vec<Option<usize>>,
    build_rows: Vec<Option<usize>>,
    residual_view_columns: Vec<ViewColumnRef>,
    residual_vm: Option<Box<dyn Transform<DataChunkView>>>,
    residual_output: Option<JoinFilterCollector>,
    /// The build rows this task consumed, for the flavours that owe unmatched
    /// ones. Kept here, not in the table, because the table is shared.
    matched_build: HashSet<(u32, u32)>,
    /// The join's own accounting: rows in, matches found, and the time split
    /// between finding them and assembling the output.
    probed_rows: u64,
    matched_rows: u64,
    probe_us: u64,
    view_compose_us: u64,
    residual_us: u64,
    /// Probe key conversions; residual expressions have their own VM reports.
    probe_key_materialization: MaterializationReport,
    probe_key_materialize_us: u64,
}

fn view_column_refs(columns: &[OutputColumn]) -> Vec<ViewColumnRef> {
    columns
        .iter()
        .map(|column| match column {
            OutputColumn::Probe(column) => ViewColumnRef {
                input: 0,
                column: *column,
            },
            OutputColumn::Build(column) => ViewColumnRef {
                input: 1,
                column: *column,
            },
        })
        .collect()
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
        next: Box<dyn PipelineNode<DataChunkView>>,
    ) -> Self {
        let output_columns = view_column_refs(&columns);
        Self {
            table,
            join_type,
            output_columns,
            probe_types,
            build_types,
            next,
            rows_by_build_chunk: HashMap::new(),
            build_chunk_order: Vec::new(),
            probe_rows: Vec::new(),
            build_rows: Vec::new(),
            residual_view_columns: Vec::new(),
            residual_vm: None,
            residual_output: None,
            matched_build: HashSet::new(),
            probed_rows: 0,
            matched_rows: 0,
            probe_us: 0,
            view_compose_us: 0,
            residual_us: 0,
            probe_key_materialization: MaterializationReport::default(),
            probe_key_materialize_us: 0,
        }
    }

    /// Installs a vectorized residual predicate that filters hash candidates
    /// before the join decides which rows are unmatched.
    pub fn with_residual_filter(
        mut self,
        columns: Vec<OutputColumn>,
        vm: Box<dyn Transform<DataChunkView>>,
        output: JoinFilterCollector,
    ) -> Self {
        self.residual_view_columns = view_column_refs(&columns);
        self.residual_vm = Some(vm);
        self.residual_output = Some(output);
        self
    }

    /// Probes one reference-backed batch and composes output row mappings.
    fn probe(&mut self, chunk: DataChunkView) -> Result<()> {
        if chunk.is_empty() {
            return Ok(());
        }
        let probe_row_count = u32::try_from(chunk.num_rows()).map_err(|_| {
            VeinError::internal(format!(
                "hash join probe batch has {} rows, exceeding the row-index range",
                chunk.num_rows()
            ))
        })?;
        let mut matches = Vec::new();
        let probe_started = Instant::now();
        let (report, materialize_us) = self.table.probe_view(&chunk, &mut matches)?;
        self.probe_key_materialization.accumulate(report);
        self.probe_key_materialize_us = self.probe_key_materialize_us.saturating_add(materialize_us);
        self.probe_us += probe_started.elapsed().as_micros() as u64;

        if self.residual_vm.is_some() && !matches.is_empty() {
            let (filtered, elapsed) = self.filter_residual_matches(&chunk, &matches)?;
            matches = filtered;
            self.residual_us += elapsed;
        }
        if self.join_type == JoinType::Single {
            let mut seen_probe_rows = HashSet::with_capacity(matches.len());
            for (probe, _) in &matches {
                if !seen_probe_rows.insert(*probe) {
                    return Err(VeinError::stage(
                        "join",
                        "scalar subquery returned more than one row",
                    ));
                }
            }
        }
        if matches!(self.join_type, JoinType::Right | JoinType::Full) {
            self.matched_build.extend(matches.iter().map(|(_, build)| *build));
        }
        self.probed_rows += u64::from(probe_row_count);
        self.matched_rows += matches.len() as u64;

        let started = Instant::now();
        if matches!(self.join_type, JoinType::Inner | JoinType::Cross) {
            self.forward_rows(
                Some(&chunk),
                matches.iter().map(|(probe, build)| super::rows::OutputRow {
                    probe: Some(*probe),
                    build: Some(*build),
                }),
            )?;
        } else {
            let rows = output_rows(self.join_type, 0..probe_row_count, &matches, &[]);
            self.forward_rows(Some(&chunk), rows)?;
        }
        self.view_compose_us += started.elapsed().as_micros() as u64;
        Ok(())
    }

    /// Applies a residual predicate to candidates partitioned by retained build
    /// batch. Each candidate is still a view; only the VM's referenced columns
    /// are materialized by its expression kernels.
    fn filter_residual_matches(
        &mut self,
        probe: &DataChunkView,
        matches: &[(u32, (u32, u32))],
    ) -> Result<(Vec<(u32, (u32, u32))>, u64)> {
        if self.residual_vm.is_none() {
            return Ok((matches.to_vec(), 0));
        }
        let output = self.residual_output.as_ref().ok_or_else(|| {
            VeinError::internal("join residual transform has no output collector")
        })?;
        let mut groups: Vec<(u32, Vec<(u32, (u32, u32))>)> = Vec::new();
        let mut positions = HashMap::with_capacity(matches.len());
        for pair in matches.iter().copied() {
            let key = pair.1.0;
            let group = *positions.entry(key).or_insert_with(|| {
                groups.push((key, Vec::new()));
                groups.len() - 1
            });
            groups[group].1.push(pair);
        }
        let mut accepted = Vec::with_capacity(matches.len());
        let mut elapsed = 0u64;
        for (build_chunk, pairs) in groups {
            let candidates = pairs.iter().map(|(probe, build)| super::rows::OutputRow {
                probe: Some(*probe),
                build: Some(*build),
            }).collect::<Vec<_>>();
            let candidate = Self::compose_group(
                &self.table,
                &self.probe_types,
                &self.build_types,
                &mut self.probe_rows,
                &mut self.build_rows,
                &self.residual_view_columns,
                Some(probe),
                Some(build_chunk),
                &candidates,
            )?;
            output.clear()?;
            let started = Instant::now();
            self.residual_vm
                .as_mut()
                .ok_or_else(|| VeinError::internal("join residual transform is missing"))?
                .push(candidate)?;
            elapsed = elapsed.saturating_add(started.elapsed().as_micros() as u64);
            let filtered = output.take()?;
            if let Some(filtered) = filtered {
                for position in 0..filtered.selection().size() {
                    let candidate_row = filtered.selection().get_index(position) as usize;
                    if let Some(pair) = pairs.get(candidate_row) {
                        accepted.push(*pair);
                    }
                }
            }
        }
        Ok((accepted, elapsed))
    }

    /// Composes a batch from one build chunk (or a null-extended build side).
    fn compose_group(
        table: &HashTable,
        probe_types: &[DataType],
        build_types: &[DataType],
        probe_rows: &mut Vec<Option<usize>>,
        build_rows: &mut Vec<Option<usize>>,
        output_columns: &[ViewColumnRef],
        probe: Option<&DataChunkView>,
        build_chunk: Option<u32>,
        rows: &[super::rows::OutputRow],
    ) -> Result<DataChunkView> {
        let row_count = rows.len();
        let null_probe = if probe.is_none() {
            Some(DataChunkView::nulls(probe_types, row_count)?)
        } else {
            None
        };
        let null_build = if build_chunk.is_none() {
            Some(DataChunkView::nulls(build_types, row_count)?)
        } else {
            None
        };
        let probe_view = probe.or(null_probe.as_ref()).expect("probe view or null extension");
        let build_view = build_chunk
            .map(|chunk| table.build_chunk(chunk))
            .or(null_build.as_ref())
            .expect("build view or null extension");
        probe_rows.clear();
        probe_rows.extend(rows.iter().map(|row| row.probe.map(|row| row as usize)));
        build_rows.clear();
        build_rows.extend(rows.iter().map(|row| row.build.map(|(_, row)| row as usize)));
        let inputs = [
            ViewInputRows { view: probe_view, rows: probe_rows },
            ViewInputRows { view: build_view, rows: build_rows },
        ];
        DataChunkView::combine(&inputs, output_columns)
    }

    /// Preserves output row order while splitting only at build-batch changes.
    fn forward_rows(
        &mut self,
        probe: Option<&DataChunkView>,
        rows: impl IntoIterator<Item = super::rows::OutputRow>,
    ) -> Result<()> {
        for row in rows {
            let key = row.build.map(|(chunk, _)| chunk);
            let group = self.rows_by_build_chunk.entry(key).or_default();
            if group.is_empty() {
                self.build_chunk_order.push(key);
            }
            group.push(row);
        }

        let mut build_chunk_order = std::mem::take(&mut self.build_chunk_order);
        for build_chunk in build_chunk_order.drain(..) {
            let mut group = self
                .rows_by_build_chunk
                .remove(&build_chunk)
                .ok_or_else(|| VeinError::internal("join output row bucket disappeared"))?;
            let composed = Self::compose_group(
                &self.table,
                &self.probe_types,
                &self.build_types,
                &mut self.probe_rows,
                &mut self.build_rows,
                &self.output_columns,
                probe,
                build_chunk,
                &group,
            );
            group.clear();
            self.rows_by_build_chunk.insert(build_chunk, group);
            let view = composed?;
            if !view.is_empty() {
                self.next.push(view)?;
            }
        }
        self.build_chunk_order = build_chunk_order;
        Ok(())
    }
}

impl PipelineNode<DataChunkView> for HashJoin {
    fn push(&mut self, view: DataChunkView) -> Result<()> {
        self.probe(view)
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
                let rows = output_rows(self.join_type, 0..0, &[], &unmatched);
                let started = Instant::now();
                self.forward_rows(None, rows)?;
                self.view_compose_us += started.elapsed().as_micros() as u64;
            }
        }
        tracing::info!(
            target: "vein.audit",
            event = "join_finished",
            probed_rows = self.probed_rows,
            matched_rows = self.matched_rows,
            probe_us = self.probe_us,
            view_compose_us = self.view_compose_us,
            residual_us = self.residual_us,
            key_shared_columns = self.probe_key_materialization.shared_columns,
            key_gathered_columns = self.probe_key_materialization.gathered_columns,
            key_empty_columns = self.probe_key_materialization.empty_columns,
            key_gathered_output_buffer_bytes = self.probe_key_materialization.gathered_output_buffer_bytes,
            key_materialize_us = self.probe_key_materialize_us,
            "join_finished"
        );
        self.next.flush()
    }

    /// Whether the pipeline is done is decided below the join, so the source's
    /// question has to pass through it.
    fn finished(&self) -> bool {
        self.next.finished()
    }

    fn fork_local(&self) -> Result<Option<Box<dyn PipelineNode<DataChunkView>>>> {
        // Right/full joins must account for build rows matched by every probe
        // worker. Residual VM state is task-local and is not reconstructed from
        // the immutable build table, so those plans retain the serial path.
        if matches!(self.join_type, JoinType::Right | JoinType::Full)
            || self.residual_vm.is_some()
        {
            return Ok(None);
        }
        let Some(next) = self.next.fork_local()? else {
            return Ok(None);
        };
        let columns = self
            .output_columns
            .iter()
            .map(|column| match column.input {
                0 => OutputColumn::Probe(column.column),
                1 => OutputColumn::Build(column.column),
                _ => unreachable!("join output references only probe or build input"),
            })
            .collect();
        Ok(Some(Box::new(Self::new(
            Arc::clone(&self.table),
            self.join_type,
            columns,
            self.probe_types.clone(),
            self.build_types.clone(),
            next,
        ))))
    }

    fn combine_local(
        self: Box<Self>,
        canonical: &mut dyn PipelineNode<DataChunkView>,
    ) -> Result<()> {
        let target = canonical
            .as_any_mut()
            .downcast_mut::<HashJoin>()
            .ok_or_else(|| VeinError::internal("hash join local state has a mismatched target"))?;
        if !Arc::ptr_eq(&self.table, &target.table)
            || std::mem::discriminant(&self.join_type)
                != std::mem::discriminant(&target.join_type)
            || self.output_columns.len() != target.output_columns.len()
            || !self
                .output_columns
                .iter()
                .zip(&target.output_columns)
                .all(|(left, right)| left.input == right.input && left.column == right.column)
            || self.probe_types != target.probe_types
            || self.build_types != target.build_types
        {
            return Err(VeinError::internal(
                "cannot combine hash join states from different plans",
            ));
        }
        self.next.combine_local(target.next.as_mut())?;
        target.probed_rows = target.probed_rows.saturating_add(self.probed_rows);
        target.matched_rows = target.matched_rows.saturating_add(self.matched_rows);
        target.probe_us = target.probe_us.saturating_add(self.probe_us);
        target.view_compose_us = target
            .view_compose_us
            .saturating_add(self.view_compose_us);
        target.residual_us = target.residual_us.saturating_add(self.residual_us);
        target
            .probe_key_materialization
            .accumulate(self.probe_key_materialization);
        target.probe_key_materialize_us = target
            .probe_key_materialize_us
            .saturating_add(self.probe_key_materialize_us);
        Ok(())
    }
}

impl crate::pipeline::Transform<DataChunkView> for HashJoin {}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use arrow::array::{Array, ArrayRef, Int64Array};
    use common::protocol::{MicroOp, MicroProgram};

    use super::*;
    use crate::pipeline::DataChunk;
    use crate::udf::FunctionRegistry;
    use crate::vm::VectorVmExecutor;

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

    impl PipelineNode<DataChunkView> for Collect {
        fn push(&mut self, view: DataChunkView) -> Result<()> {
            self.0
                .lock()
                .expect("collect lock")
                .push(view.to_data_chunk()?);
            Ok(())
        }

        fn flush(&mut self) -> Result<()> {
            Ok(())
        }

        fn fork_local(&self) -> Result<Option<Box<dyn PipelineNode<DataChunkView>>>> {
            Ok(Some(Box::new(Self::default())))
        }

        fn combine_local(
            self: Box<Self>,
            canonical: &mut dyn PipelineNode<DataChunkView>,
        ) -> Result<()> {
            let target = canonical
                .as_any_mut()
                .downcast_mut::<Collect>()
                .ok_or_else(|| VeinError::internal("collect local state has a mismatched target"))?;
            let mut local_chunks = self
                .0
                .lock()
                .map_err(|_| VeinError::internal("collect mutex poisoned"))?;
            let mut target_chunks = target
                .0
                .lock()
                .map_err(|_| VeinError::internal("collect mutex poisoned"))?;
            target_chunks.try_reserve(local_chunks.len()).map_err(|error| {
                VeinError::internal(format!("cannot reserve collected test batches: {error}"))
            })?;
            target_chunks.extend(std::mem::take(&mut *local_chunks));
            Ok(())
        }
    }

    impl HashJoin {
        fn push(&mut self, chunk: DataChunk) -> Result<()> {
            PipelineNode::<DataChunkView>::push(self, DataChunkView::from_chunk(chunk)?)
        }

        fn flush(&mut self) -> Result<()> {
            PipelineNode::<DataChunkView>::flush(self)
        }
    }

    fn ints(values: &[i64]) -> ArrayRef {
        Arc::new(Int64Array::from(values.to_vec()))
    }

    fn nullable_ints(values: &[Option<i64>]) -> ArrayRef {
        Arc::new(Int64Array::from(values.to_vec()))
    }

    fn selected_value(chunk: &DataChunk, column: usize, physical_row: usize) -> Option<i64> {
        let values = chunk
            .column(column)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("int64 join output");
        values.is_valid(physical_row).then(|| values.value(physical_row))
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
        (0..chunk.selection.size())
            .map(|logical_row| selected_value(chunk, column, chunk.selection.get_index(logical_row)))
            .collect()
    }

    fn selected_rows(chunk: &DataChunk) -> Vec<[Option<i64>; 4]> {
        (0..chunk.selection.size())
            .map(|position| {
                let physical = chunk.selection.get_index(position);
                std::array::from_fn(|column| selected_value(chunk, column, physical))
            })
            .collect()
    }

    fn residual_greater_than_filter() -> (VectorVmExecutor<DataChunkView>, JoinFilterCollector) {
        let collector = JoinFilterCollector::default();
        let program = Arc::new(MicroProgram::new(
            3,
            vec![
                MicroOp::CmpGt {
                    dst: 2,
                    src1: 0,
                    src2: 1,
                },
                MicroOp::FilterMask { bool_reg: 2 },
            ],
        ));
        let vm = VectorVmExecutor::<DataChunkView>::new_view(
            program,
            Arc::new(FunctionRegistry::new()),
            Box::new(collector.clone()),
        );
        (vm, collector)
    }

    fn run_outer_join_with_residual(join_type: JoinType) -> Vec<[Option<i64>; 4]> {
        let mut table = HashTable::new(JoinKeys::new(vec![0]), JoinKeys::new(vec![0]));
        table
            .insert(DataChunk::from_vectors(vec![
                ints(&[1, 1, 3]),
                nullable_ints(&[Some(5), Some(15), Some(7)]),
            ]))
            .expect("build rows");

        let sink = Collect::default();
        let (vm, collector) = residual_greater_than_filter();
        let mut join = HashJoin::new(
            Arc::new(table),
            join_type,
            vec![
                OutputColumn::Probe(0),
                OutputColumn::Probe(1),
                OutputColumn::Build(0),
                OutputColumn::Build(1),
            ],
            vec![DataType::Int64, DataType::Int64],
            vec![DataType::Int64, DataType::Int64],
            Box::new(sink.clone()),
        )
            .with_residual_filter(
                vec![OutputColumn::Probe(1), OutputColumn::Build(1)],
                Box::new(vm),
                collector,
            );

        join.push(DataChunk::from_vectors(vec![
            ints(&[1, 1, 1, 2]),
            nullable_ints(&[Some(1), Some(10), None, Some(9)]),
        ]))
            .expect("probe rows");
        join.flush().expect("flush outer join");

        let chunks = sink.0.lock().expect("collect lock");
        let mut rows = chunks.iter().flat_map(selected_rows).collect::<Vec<_>>();
        rows.sort_unstable();
        rows
    }

    /// A built table over one chunk of `values`.
    fn build(values: &[i64]) -> HashTable {
        let mut sink = BuildSink::new(JoinKeys::new(vec![0]), JoinKeys::new(vec![0]));
        sink.push(DataChunkView::from_chunk(DataChunk::from_vectors(vec![ints(values)]))
            .expect("build view"))
            .expect("build");
        sink.finish()
    }

    #[derive(Clone, Default)]
    struct ViewCollect(Arc<Mutex<Vec<DataChunkView>>>);

    impl PipelineNode<DataChunkView> for ViewCollect {
        fn push(&mut self, view: DataChunkView) -> Result<()> {
            self.0.lock().expect("view collector lock").push(view);
            Ok(())
        }

        fn flush(&mut self) -> Result<()> {
            Ok(())
        }
    }

    #[test]
    fn join_reports_build_and_repeated_probe_conversions_without_copying_payloads() {
        use common::arrow::SelectionVector;

        let build_payload = ints(&[70, 80, 90]);
        let mut build = BuildSink::new(JoinKeys::new(vec![0]), JoinKeys::new(vec![0]));
        build.push(DataChunkView::from_chunk(DataChunk::new(
            vec![ints(&[7, 8, 9]), Arc::clone(&build_payload)],
            SelectionVector::new_sparse(vec![2, 0]),
        )).unwrap()).unwrap();
        build.flush().unwrap();
        let table = Arc::new(build.finish());
        let build_metrics = table.build_key_materialization();
        assert_eq!(build_metrics.0.gathered_columns, 1);
        assert_eq!(build_metrics.0.shared_columns, 0);
        assert!(build_metrics.0.gathered_output_buffer_bytes > 0);
        let probe_payload = ints(&[700, 900, 800]);
        let keys = ints(&[7, 9, 8]);
        let sink = ViewCollect::default();
        let mut join = HashJoin::new(
            Arc::clone(&table), JoinType::Inner,
            vec![OutputColumn::Probe(1), OutputColumn::Build(1)],
            vec![DataType::Int64; 2], vec![DataType::Int64; 2], Box::new(sink.clone()),
        );
        for selection in [SelectionVector::new_dense(3), SelectionVector::new_sparse(vec![1, 0])] {
            PipelineNode::<DataChunkView>::push(&mut join, DataChunkView::from_chunk(DataChunk::new(
                vec![Arc::clone(&keys), Arc::clone(&probe_payload)], selection,
            )).unwrap()).unwrap();
        }
        join.flush().unwrap();
        assert_eq!(join.probe_key_materialization.shared_columns, 1);
        assert_eq!(join.probe_key_materialization.gathered_columns, 1);
        assert_eq!(join.probe_key_materialization.empty_columns, 0);
        assert!(join.probe_key_materialization.gathered_output_buffer_bytes > 0);
        assert_eq!(table.build_key_materialization(), build_metrics);
        let views = sink.0.lock().unwrap();
        let mut pairs = Vec::new();
        for view in views.iter() {
            for row in 0..view.num_rows() {
                let (probe, probe_row) = view.resolve(0, row).unwrap().unwrap();
                let (build, build_row) = view.resolve(1, row).unwrap().unwrap();
                assert!(Arc::ptr_eq(probe, &probe_payload));
                assert!(Arc::ptr_eq(build, &build_payload));
                pairs.push((probe.as_any().downcast_ref::<Int64Array>().unwrap().value(probe_row),
                            build.as_any().downcast_ref::<Int64Array>().unwrap().value(build_row)));
            }
        }
        assert_eq!(pairs, vec![(700, 70), (900, 90), (900, 90), (700, 70)]);
    }

    #[test]
    fn join_output_view_references_original_probe_and_build_payloads() {
        let build_key = ints(&[7, 8]);
        let build_payload = ints(&[70, 80]);
        let mut table = HashTable::new(JoinKeys::new(vec![0]), JoinKeys::new(vec![0]));
        table
            .insert_view(
                DataChunkView::from_chunk(DataChunk::from_vectors(vec![
                    build_key,
                    Arc::clone(&build_payload),
                ]))
                    .expect("build view"),
            )
            .expect("build table");

        let probe_key = ints(&[8]);
        let probe_payload = ints(&[800]);
        let sink = ViewCollect::default();
        let mut join = HashJoin::new(
            Arc::new(table),
            JoinType::Inner,
            vec![OutputColumn::Probe(1), OutputColumn::Build(1)],
            vec![DataType::Int64, DataType::Int64],
            vec![DataType::Int64, DataType::Int64],
            Box::new(sink.clone()),
        );
        PipelineNode::<DataChunkView>::push(
            &mut join,
            DataChunkView::from_chunk(DataChunk::from_vectors(vec![
                probe_key,
                Arc::clone(&probe_payload),
            ]))
                .expect("probe view"),
        )
            .expect("probe join");
        PipelineNode::<DataChunkView>::flush(&mut join).expect("flush join");

        let outputs = sink.0.lock().expect("view collector lock");
        assert_eq!(outputs.len(), 1);
        let (probe_array, probe_row) = outputs[0].resolve(0, 0).unwrap().unwrap();
        let (build_array, build_row) = outputs[0].resolve(1, 0).unwrap().unwrap();
        assert!(Arc::ptr_eq(probe_array, &probe_payload));
        assert!(Arc::ptr_eq(build_array, &build_payload));
        assert_eq!(probe_array.as_any().downcast_ref::<Int64Array>().unwrap().value(probe_row), 800);
        assert_eq!(build_array.as_any().downcast_ref::<Int64Array>().unwrap().value(build_row), 80);
    }

    #[test]
    fn multiple_build_batches_preserve_duplicate_output_references() {
        let payloads = [ints(&[100, 101]), ints(&[200, 201])];
        let mut table = HashTable::new(JoinKeys::new(vec![0]), JoinKeys::new(vec![0]));
        for payload in &payloads {
            table.insert_view(DataChunkView::from_chunk(DataChunk::from_vectors(vec![
                ints(&[7, 7]), Arc::clone(payload),
            ])).unwrap()).unwrap();
        }
        let probe_payload = ints(&[700]);
        let sink = ViewCollect::default();
        let mut join = HashJoin::new(
            Arc::new(table), JoinType::Inner,
            vec![OutputColumn::Probe(1), OutputColumn::Build(1), OutputColumn::Build(1)],
            vec![DataType::Int64; 2], vec![DataType::Int64; 2], Box::new(sink.clone()),
        );
        PipelineNode::<DataChunkView>::push(&mut join, DataChunkView::from_chunk(
            DataChunk::from_vectors(vec![ints(&[7]), Arc::clone(&probe_payload)])
        ).unwrap()).unwrap();
        join.flush().unwrap();
        let outputs = sink.0.lock().unwrap();
        assert_eq!(outputs.len(), 2, "one reference view per retained build batch");
        let mut values = Vec::new();
        for view in outputs.iter() {
            for row in 0..view.num_rows() {
                let (probe, _) = view.resolve(0, row).unwrap().unwrap();
                let (build, index) = view.resolve(1, row).unwrap().unwrap();
                let (duplicate, duplicate_index) = view.resolve(2, row).unwrap().unwrap();
                assert!(Arc::ptr_eq(probe, &probe_payload));
                assert!(payloads.iter().any(|payload| Arc::ptr_eq(build, payload)));
                assert!(Arc::ptr_eq(build, duplicate));
                assert_eq!(index, duplicate_index);
                values.push(build.as_any().downcast_ref::<Int64Array>().unwrap().value(index));
            }
        }
        values.sort_unstable();
        assert_eq!(values, vec![100, 101, 200, 201]);
    }

    #[test]
    fn zero_column_join_views_retain_match_cardinality() {
        let sink = ViewCollect::default();
        let mut join = HashJoin::new(
            Arc::new(build(&[7, 7])), JoinType::Inner, vec![],
            vec![DataType::Int64], vec![DataType::Int64], Box::new(sink.clone()),
        );
        PipelineNode::<DataChunkView>::push(&mut join, DataChunkView::from_chunk(
            DataChunk::from_vectors(vec![ints(&[7, 8, 7])])
        ).unwrap()).unwrap();
        join.flush().unwrap();
        let outputs = sink.0.lock().unwrap();
        assert_eq!(outputs.iter().map(DataChunkView::num_rows).sum::<usize>(), 4);
        for view in outputs.iter() {
            assert_eq!(view.num_columns(), 0);
            let (chunk, report) = view.to_data_chunk_with_report().unwrap();
            assert_eq!(chunk.num_rows(), view.num_rows());
            assert_eq!(chunk.num_columns(), 0);
            assert_eq!(report, MaterializationReport::default());
        }
    }

    #[test]
    fn outer_join_views_preserve_payloads_with_empty_opposite_sides() {
        for join_type in [JoinType::Left, JoinType::Right, JoinType::Full] {
            for empty_build in [true, false] {
                let payload = ints(&[10, 20]);
                let mut table = HashTable::new(JoinKeys::new(vec![0]), JoinKeys::new(vec![0]));
                if !empty_build {
                    table.insert_view(DataChunkView::from_chunk(DataChunk::from_vectors(
                        vec![Arc::clone(&payload)]
                    )).unwrap()).unwrap();
                }
                let sink = ViewCollect::default();
                let mut join = HashJoin::new(
                    Arc::new(table), join_type,
                    vec![OutputColumn::Probe(0), OutputColumn::Build(0)],
                    vec![DataType::Int64], vec![DataType::Int64], Box::new(sink.clone()),
                );
                let probe = if empty_build { Arc::clone(&payload) } else { ints(&[]) };
                PipelineNode::<DataChunkView>::push(&mut join, DataChunkView::from_chunk(
                    DataChunk::from_vectors(vec![probe])
                ).unwrap()).unwrap();
                join.flush().unwrap();
                let outputs = sink.0.lock().unwrap();
                let emits = join_type == JoinType::Full
                    || (empty_build && join_type == JoinType::Left)
                    || (!empty_build && join_type == JoinType::Right);
                assert_eq!(outputs.iter().map(DataChunkView::num_rows).sum::<usize>(),
                           if emits { 2 } else { 0 });
                for view in outputs.iter() {
                    for row in 0..view.num_rows() {
                        let present = if empty_build { 0 } else { 1 };
                        let absent = 1 - present;
                        let (source, index) = view.resolve(present, row).unwrap().unwrap();
                        assert!(Arc::ptr_eq(source, &payload));
                        assert_eq!(index, row);
                        assert!(view.resolve(absent, row).unwrap().is_none());
                        assert_eq!(view.column_data_type(absent).unwrap(), &DataType::Int64);
                    }
                }
            }
        }
    }

    #[test]
    fn empty_build_null_extension_requires_declared_output_column() {
        let sink = ViewCollect::default();
        let mut join = HashJoin::new(
            Arc::new(HashTable::new(JoinKeys::new(vec![0]), JoinKeys::new(vec![0]))),
            JoinType::Left, vec![OutputColumn::Probe(0), OutputColumn::Build(0)],
            vec![DataType::Int64], vec![], Box::new(sink.clone()),
        );
        let error = PipelineNode::<DataChunkView>::push(&mut join,
                                                        DataChunkView::from_chunk(DataChunk::from_vectors(vec![ints(&[10])])).unwrap(),
        ).expect_err("missing build type must reject the output mapping");
        assert!(error.to_string().contains("missing column 0 in input 1"), "{error}");
        assert!(sink.0.lock().unwrap().is_empty());
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
    fn cross_join_emits_cartesian_product_with_empty_key_vectors() {
        let mut table = HashTable::new(JoinKeys::new(Vec::new()), JoinKeys::new(Vec::new()));
        let build_values = ints(&[10, 20, 30]);
        table
            .insert(DataChunk::from_vectors(vec![Arc::clone(&build_values)]))
            .expect("insert build rows without join keys");

        let probe_values = ints(&[1, 2]);
        let sink = Collect::default();
        let mut join = HashJoin::new(
            Arc::new(table),
            JoinType::Cross,
            vec![OutputColumn::Probe(0), OutputColumn::Build(0)],
            vec![DataType::Int64],
            vec![DataType::Int64],
            Box::new(sink.clone()),
        );

        join.push(DataChunk::from_vectors(vec![Arc::clone(&probe_values)]))
            .expect("probe cross join rows");
        join.flush().expect("flush cross join");

        let chunks = sink.0.lock().expect("collect lock");
        let mut pairs = chunks
            .iter()
            .flat_map(|chunk| {
                selected_cells(chunk, 0)
                    .into_iter()
                    .zip(selected_cells(chunk, 1))
            })
            .collect::<Vec<_>>();
        pairs.sort_unstable();
        assert_eq!(
            pairs,
            vec![
                (Some(1), Some(10)),
                (Some(1), Some(20)),
                (Some(1), Some(30)),
                (Some(2), Some(10)),
                (Some(2), Some(20)),
                (Some(2), Some(30)),
            ]
        );
    }

    #[test]
    fn left_join_residual_false_and_null_leave_probe_rows_unmatched() {
        assert_eq!(
            run_outer_join_with_residual(JoinType::Left),
            vec![
                [Some(1), None, None, None],
                [Some(1), Some(1), None, None],
                [Some(1), Some(10), Some(1), Some(5)],
                [Some(2), Some(9), None, None],
            ]
        );
    }

    #[test]
    fn right_join_residual_false_and_null_leave_build_rows_unmatched() {
        assert_eq!(
            run_outer_join_with_residual(JoinType::Right),
            vec![
                [None, None, Some(1), Some(15)],
                [None, None, Some(3), Some(7)],
                [Some(1), Some(10), Some(1), Some(5)],
            ]
        );
    }

    #[test]
    fn full_join_tracks_matches_only_after_residual_filtering() {
        assert_eq!(
            run_outer_join_with_residual(JoinType::Full),
            vec![
                [None, None, Some(1), Some(15)],
                [None, None, Some(3), Some(7)],
                [Some(1), None, None, None],
                [Some(1), Some(1), None, None],
                [Some(1), Some(10), Some(1), Some(5)],
                [Some(2), Some(9), None, None],
            ]
        );
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
    fn semi_join_materializes_sparse_selected_rows_at_the_result_boundary() {
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
        assert_eq!(chunks[0].num_rows(), 1);
        assert_eq!(cells(&chunks[0], 0), vec![Some(40)]);
    }

    #[test]
    fn anti_join_materializes_unmatched_selected_rows_at_the_result_boundary() {
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
        assert_eq!(chunks[0].num_rows(), 1);
        assert_eq!(cells(&chunks[0], 0), vec![Some(20)]);
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

    #[test]
    fn single_join_checks_cardinality_and_keeps_both_input_buffers() {
        let mut table = HashTable::new(JoinKeys::new(vec![0]), JoinKeys::new(vec![0]));
        let build_keys = ints(&[10, 20]);
        let build_values = ints(&[100, 200]);
        table
            .insert(DataChunk::from_vectors(vec![build_keys, Arc::clone(&build_values)]))
            .expect("insert single-match build rows");

        let probe_keys = ints(&[20, 10]);
        let probe_payload = ints(&[2_000, 1_000]);
        let sink = Collect::default();
        let mut join = HashJoin::new(
            Arc::new(table),
            JoinType::Single,
            vec![OutputColumn::Probe(1), OutputColumn::Build(1)],
            vec![DataType::Int64, DataType::Int64],
            vec![DataType::Int64, DataType::Int64],
            Box::new(sink.clone()),
        );

        join.push(DataChunk::from_vectors(vec![probe_keys, Arc::clone(&probe_payload)]))
            .expect("single match should not multiply probe rows");
        join.flush().expect("flush single join");

        let chunks = sink.0.lock().expect("collect lock");
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].num_rows(), 2);
        let probe = chunks[0].column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        let build = chunks[0].column(1).as_any().downcast_ref::<Int64Array>().unwrap();
        let mut pairs: Vec<(i64, i64)> = (0..chunks[0].num_rows())
            .map(|row| (probe.value(row), build.value(row)))
            .collect();
        pairs.sort_unstable();
        assert_eq!(pairs, vec![(1_000, 100), (2_000, 200)]);
    }

    #[test]
    fn single_join_returns_null_for_an_unmatched_probe_row() {
        let sink = Collect::default();
        let mut join = HashJoin::new(
            Arc::new(build(&[20])),
            JoinType::Single,
            vec![OutputColumn::Probe(0), OutputColumn::Build(0)],
            vec![DataType::Int64],
            vec![DataType::Int64],
            Box::new(sink.clone()),
        );

        join.push(DataChunk::from_vectors(vec![ints(&[10])]))
            .expect("unmatched probe should produce a null scalar");
        join.flush().expect("flush single join");

        let chunks = sink.0.lock().expect("collect lock");
        assert_eq!(chunks.len(), 1);
        assert!(chunks[0].column(1).is_null(0));
    }

    #[test]
    fn single_join_errors_on_multiple_matches_across_build_chunks() {
        let mut table = HashTable::new(JoinKeys::new(vec![0]), JoinKeys::new(vec![0]));
        table.insert(DataChunk::from_vectors(vec![ints(&[20])])).expect("first build batch");
        table.insert(DataChunk::from_vectors(vec![ints(&[20])])).expect("second build batch");
        let sink = Collect::default();
        let mut join = HashJoin::new(
            Arc::new(table),
            JoinType::Single,
            vec![OutputColumn::Probe(0), OutputColumn::Build(0)],
            vec![DataType::Int64],
            vec![DataType::Int64],
            Box::new(sink),
        );

        let error = join
            .push(DataChunk::from_vectors(vec![ints(&[20])]))
            .expect_err("two inner rows violate scalar cardinality");
        assert!(error.to_string().contains("scalar subquery returned more than one row"));
    }

    #[test]
    fn inner_join_reuses_output_buckets_across_probe_batches() {
        let mut table = HashTable::new(JoinKeys::new(vec![0]), JoinKeys::new(vec![0]));
        table
            .insert(DataChunk::from_vectors(vec![ints(&[7, 8]), ints(&[70, 80])]))
            .expect("first build batch");
        table
            .insert(DataChunk::from_vectors(vec![ints(&[7, 9]), ints(&[71, 90])]))
            .expect("second build batch");

        let sink = Collect::default();
        let mut join = HashJoin::new(
            Arc::new(table),
            JoinType::Inner,
            vec![OutputColumn::Probe(1), OutputColumn::Build(1)],
            vec![DataType::Int64, DataType::Int64],
            vec![DataType::Int64, DataType::Int64],
            Box::new(sink.clone()),
        );
        join.push(DataChunk::from_vectors(vec![ints(&[7]), ints(&[700])]))
            .expect("first probe batch");
        join.push(DataChunk::from_vectors(vec![
            ints(&[9, 7]),
            ints(&[900, 701]),
        ]))
        .expect("second probe batch");
        join.flush().expect("flush join");

        let chunks = sink.0.lock().expect("collect lock");
        let probe = chunks.iter().flat_map(|chunk| cells(chunk, 0)).collect::<Vec<_>>();
        let build = chunks.iter().flat_map(|chunk| cells(chunk, 1)).collect::<Vec<_>>();
        let mut pairs = probe.into_iter().zip(build).collect::<Vec<_>>();
        pairs.sort_unstable();

        assert_eq!(
            pairs,
            vec![
                (Some(700), Some(70)),
                (Some(700), Some(71)),
                (Some(701), Some(70)),
                (Some(701), Some(71)),
                (Some(900), Some(90)),
            ],
            "reused build-chunk buckets retain duplicate matches and do not leak prior batches"
        );
    }

    #[test]
    fn inner_join_worker_local_probe_states_combine_duplicate_matches() {
        let mut table = HashTable::new(JoinKeys::new(vec![0]), JoinKeys::new(vec![0]));
        table
            .insert(DataChunk::from_vectors(vec![
                ints(&[7, 7, 8]),
                ints(&[70, 71, 80]),
            ]))
            .expect("build duplicate keys");

        let sink = Collect::default();
        let mut canonical = HashJoin::new(
            Arc::new(table),
            JoinType::Inner,
            vec![OutputColumn::Probe(1), OutputColumn::Build(1)],
            vec![DataType::Int64, DataType::Int64],
            vec![DataType::Int64, DataType::Int64],
            Box::new(sink.clone()),
        );
        let mut first = canonical
            .fork_local()
            .expect("fork join probe state")
            .expect("inner join supports local probe state");
        let mut second = canonical
            .fork_local()
            .expect("fork second join probe state")
            .expect("inner join supports local probe state");

        first
            .push(
                DataChunkView::from_chunk(DataChunk::from_vectors(vec![
                    ints(&[7, 8]),
                    ints(&[700, 800]),
                ]))
                .expect("view first probe batch"),
            )
            .expect("probe first worker batch");
        second
            .push(
                DataChunkView::from_chunk(DataChunk::from_vectors(vec![
                    ints(&[7]),
                    ints(&[701]),
                ]))
                .expect("view second probe batch"),
            )
            .expect("probe second worker batch");

        first
            .combine_local(&mut canonical)
            .expect("combine first worker state");
        second
            .combine_local(&mut canonical)
            .expect("combine second worker state");
        canonical.flush().expect("flush canonical join");

        let chunks = sink.0.lock().expect("collect lock");
        let probe = chunks.iter().flat_map(|chunk| cells(chunk, 0)).collect::<Vec<_>>();
        let build = chunks.iter().flat_map(|chunk| cells(chunk, 1)).collect::<Vec<_>>();
        let mut pairs = probe.into_iter().zip(build).collect::<Vec<_>>();
        pairs.sort_unstable();
        assert_eq!(
            pairs,
            vec![
                (Some(700), Some(70)),
                (Some(700), Some(71)),
                (Some(701), Some(70)),
                (Some(701), Some(71)),
                (Some(800), Some(80)),
            ],
            "combining probe-local state preserves each duplicate build match"
        );
        assert_eq!(canonical.probed_rows, 3);
        assert_eq!(canonical.matched_rows, 5);
    }
}
