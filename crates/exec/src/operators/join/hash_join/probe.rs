use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;

use arrow::datatypes::DataType;

use crate::pipeline::{DataChunkView, MaterializationReport, PipelineNode, Transform, ViewColumnRef, ViewInputRows};
use common::Result;
use error::VeinError;

use super::super::hash_table::HashTable;
use super::super::rows::{JoinType, OutputRow, output_rows};
use super::super::spec::OutputColumn;
use super::JoinFilterCollector;

/// Probes a shared build table and forwards matching rows as chunk views.
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
    rows_by_build_chunk: HashMap<Option<u32>, Vec<OutputRow>>,
    build_chunk_order: Vec<Option<u32>>,
    probe_rows: Vec<Option<usize>>,
    build_rows: Vec<Option<usize>>,
    residual_view_columns: Vec<ViewColumnRef>,
    residual_vm: Option<Box<dyn Transform<DataChunkView>>>,
    residual_output: Option<JoinFilterCollector>,
    /// The build rows this task consumed, for the flavours that owe unmatched
    /// ones. Kept here, not in the table, because the table is shared.
    matched_build: HashSet<(u32, u32)>,
    /// The join's accounting: rows in, matches found, and cumulative worker
    /// elapsed times. Worker times are not additive query wall-clock times.
    pub(super) probed_rows: u64,
    pub(super) matched_rows: u64,
    probe_us: u64,
    /// Output grouping and view construction, excluding downstream execution.
    pub(super) view_compose_us: u64,
    /// Time inside downstream push calls, including calls that return an error.
    pub(super) downstream_push_us: u64,
    residual_us: u64,
    /// Probe key conversions; residual expressions have their own VM reports.
    pub(super) probe_key_materialization: MaterializationReport,
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
            downstream_push_us: 0,
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

        if matches!(self.join_type, JoinType::Inner | JoinType::Cross) {
            self.forward_rows(
                Some(&chunk),
                matches.iter().map(|(probe, build)| OutputRow {
                    probe: Some(*probe),
                    build: Some(*build),
                }),
            )?;
        } else {
            let rows = output_rows(self.join_type, 0..probe_row_count, &matches, &[]);
            self.forward_rows(Some(&chunk), rows)?;
        }
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
            let candidates = pairs.iter().map(|(probe, build)| OutputRow {
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
        rows: &[OutputRow],
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
        rows: impl IntoIterator<Item = OutputRow>,
    ) -> Result<()> {
        let grouping_started = Instant::now();
        for row in rows {
            let key = row.build.map(|(chunk, _)| chunk);
            let group = self.rows_by_build_chunk.entry(key).or_default();
            if group.is_empty() {
                self.build_chunk_order.push(key);
            }
            group.push(row);
        }
        self.view_compose_us = self
            .view_compose_us
            .saturating_add(grouping_started.elapsed().as_micros() as u64);

        let mut build_chunk_order = std::mem::take(&mut self.build_chunk_order);
        for build_chunk in build_chunk_order.drain(..) {
            let compose_started = Instant::now();
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
            self.view_compose_us = self
                .view_compose_us
                .saturating_add(compose_started.elapsed().as_micros() as u64);
            let view = composed?;
            if !view.is_empty() {
                let downstream_started = Instant::now();
                let pushed = self.next.push(view);
                self.downstream_push_us = self
                    .downstream_push_us
                    .saturating_add(downstream_started.elapsed().as_micros() as u64);
                pushed?;
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
                self.forward_rows(None, rows)?;
            }
        }
        tracing::info!(
            target: "vein.audit",
            event = "join_finished",
            probed_rows = self.probed_rows,
            matched_rows = self.matched_rows,
            probe_us = self.probe_us,
            view_compose_us = self.view_compose_us,
            downstream_push_us = self.downstream_push_us,
            timing_scope = "sum_of_worker_elapsed_times",
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
        target.downstream_push_us = target
            .downstream_push_us
            .saturating_add(self.downstream_push_us);
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
