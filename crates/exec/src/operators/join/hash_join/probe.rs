use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;

use arrow::datatypes::DataType;

use crate::pipeline::{
    DataChunkView, MaterializationReport, PipelineNode, Transform, ViewColumnRef, ViewInputRows,
};
use common::Result;
use error::VeinError;

use super::super::hash_table::HashTable;
use super::super::rows::{JoinType, OutputRow, null_aware_anti_rows, output_rows};
use super::super::spec::OutputColumn;
use super::JoinFilterCollector;
use super::output::JoinOutputComposer;

type JoinMatch = (u32, (u32, u32));
type BuildChunkMatchGroups = Vec<(u32, Vec<JoinMatch>)>;

/// Probes a shared build table and forwards matching rows as chunk views.
pub struct HashJoin {
    /// Shared rather than owned, so a probe fan-out can give every task the same
    /// table. Read-only by construction: `probe` takes `&self`.
    table: Arc<HashTable>,
    join_type: JoinType,
    build_batch_rows: usize,
    output_columns: Vec<ViewColumnRef>,
    probe_types: Vec<DataType>,
    build_types: Vec<DataType>,
    next: Box<dyn PipelineNode<DataChunkView>>,
    /// Coalesces matches across retained build chunks into bounded views.
    output_composer: JoinOutputComposer,
    output_batch: Vec<OutputRow>,
    probe_rows: Vec<Option<usize>>,
    build_rows: Vec<Option<usize>>,
    residual_view_columns: Vec<ViewColumnRef>,
    residual_transform: Option<Box<dyn Transform<DataChunkView>>>,
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
    pub(super) output_batches: u64,
    pub(super) output_rows: u64,
    pub(super) max_output_batch_rows: usize,
    residual_us: u64,
    /// Probe key conversions; residual predicate time is tracked separately.
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
        let output_composer = JoinOutputComposer::new(&output_columns, &build_types);
        let build_batch_rows = (0..table.chunk_count())
            .map(|chunk| table.build_chunk(chunk as u32).num_rows())
            .max()
            .unwrap_or(1);
        Self {
            table,
            join_type,
            build_batch_rows,
            output_columns,
            probe_types,
            build_types,
            next,
            output_composer,
            output_batch: Vec::new(),
            probe_rows: Vec::new(),
            build_rows: Vec::new(),
            residual_view_columns: Vec::new(),
            residual_transform: None,
            residual_output: None,
            matched_build: HashSet::new(),
            probed_rows: 0,
            matched_rows: 0,
            probe_us: 0,
            view_compose_us: 0,
            downstream_push_us: 0,
            output_batches: 0,
            output_rows: 0,
            max_output_batch_rows: 0,
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
        transform: Box<dyn Transform<DataChunkView>>,
        output: JoinFilterCollector,
    ) -> Self {
        self.residual_view_columns = view_column_refs(&columns);
        self.residual_transform = Some(transform);
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
        let mut null_probe_rows = Vec::new();
        let probe_started = Instant::now();
        let (report, materialize_us) = if self.join_type == JoinType::NullAwareAnti {
            self.table
                .probe_view_with_null_rows(&chunk, &mut matches, &mut null_probe_rows)?
        } else {
            self.table.probe(&chunk, &mut matches)?
        };
        self.probe_key_materialization.accumulate(report);
        self.probe_key_materialize_us =
            self.probe_key_materialize_us.saturating_add(materialize_us);
        self.probe_us += probe_started.elapsed().as_micros() as u64;

        if self.residual_transform.is_some() && !matches.is_empty() {
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
            self.matched_build
                .extend(matches.iter().map(|(_, build)| *build));
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
            let rows = if self.join_type == JoinType::NullAwareAnti {
                null_aware_anti_rows(
                    0..probe_row_count,
                    &matches,
                    &null_probe_rows,
                    self.table.is_empty(),
                    self.table.has_null_keys(),
                )
            } else {
                output_rows(self.join_type, 0..probe_row_count, &matches, &[])
            };
            self.forward_rows(Some(&chunk), rows)?;
        }
        Ok(())
    }

    /// Applies a residual predicate to candidates partitioned by retained build
    /// batch. Each candidate remains a view that expression kernels consume
    /// through the referenced column mappings.
    fn filter_residual_matches(
        &mut self,
        probe: &DataChunkView,
        matches: &[JoinMatch],
    ) -> Result<(Vec<JoinMatch>, u64)> {
        if self.residual_transform.is_none() {
            return Ok((matches.to_vec(), 0));
        }
        let output = self.residual_output.as_ref().cloned().ok_or_else(|| {
            VeinError::internal("join residual transform has no output collector")
        })?;
        let mut groups = BuildChunkMatchGroups::new();
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
            let candidates = pairs
                .iter()
                .map(|(probe, build)| OutputRow {
                    probe: Some(*probe),
                    build: Some(*build),
                })
                .collect::<Vec<_>>();
            let candidate = self.compose_group(Some(probe), Some(build_chunk), &candidates)?;
            output.clear()?;
            let started = Instant::now();
            self.residual_transform
                .as_mut()
                .ok_or_else(|| VeinError::internal("join residual transform is missing"))?
                .push(candidate)?;
            elapsed = elapsed.saturating_add(started.elapsed().as_micros() as u64);
            let filtered = output.take()?;
            if let Some(filtered) = filtered {
                for position in 0..filtered.selection().size() {
                    let candidate_row = filtered.selection().get_index(position);
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
        &mut self,
        probe: Option<&DataChunkView>,
        build_chunk: Option<u32>,
        rows: &[OutputRow],
    ) -> Result<DataChunkView> {
        let row_count = rows.len();
        let null_probe = if probe.is_none() {
            Some(DataChunkView::nulls(&self.probe_types, row_count)?)
        } else {
            None
        };
        let null_build = if build_chunk.is_none() {
            Some(DataChunkView::nulls(&self.build_types, row_count)?)
        } else {
            None
        };
        let probe_view = probe
            .or(null_probe.as_ref())
            .expect("probe view or null extension");
        let build_view = build_chunk
            .map(|chunk| self.table.build_chunk(chunk))
            .or(null_build.as_ref())
            .expect("build view or null extension");
        self.probe_rows.clear();
        self.probe_rows
            .extend(rows.iter().map(|row| row.probe.map(|row| row as usize)));
        self.build_rows.clear();
        self.build_rows.extend(
            rows.iter()
                .map(|row| row.build.map(|(_, row)| row as usize)),
        );
        let inputs = [
            ViewInputRows {
                view: probe_view,
                rows: &self.probe_rows,
            },
            ViewInputRows {
                view: build_view,
                rows: &self.build_rows,
            },
        ];
        DataChunkView::combine(&inputs, &self.residual_view_columns)
    }

    /// Preserves row order while emitting probe-sized, multi-source views.
    fn forward_rows(
        &mut self,
        probe: Option<&DataChunkView>,
        rows: impl IntoIterator<Item = OutputRow>,
    ) -> Result<()> {
        let batch_limit = probe
            .map_or(self.build_batch_rows, |view| {
                view.num_rows().max(self.build_batch_rows)
            })
            .max(1);
        for row in rows {
            self.output_batch.push(row);
            if self.output_batch.len() == batch_limit {
                self.push_output_batch(probe)?;
            }
        }
        if !self.output_batch.is_empty() {
            self.push_output_batch(probe)?;
        }
        Ok(())
    }

    fn push_output_batch(&mut self, probe: Option<&DataChunkView>) -> Result<()> {
        let mut rows = std::mem::take(&mut self.output_batch);
        let compose_started = Instant::now();
        let composed = self
            .output_composer
            .compose(&self.table, probe, &self.probe_types, &rows);
        self.view_compose_us = self
            .view_compose_us
            .saturating_add(compose_started.elapsed().as_micros() as u64);
        let result = composed.and_then(|view| {
            if view.is_empty() {
                return Ok(());
            }
            let row_count = view.num_rows();
            let downstream_started = Instant::now();
            let pushed = self.next.push(view);
            self.downstream_push_us = self
                .downstream_push_us
                .saturating_add(downstream_started.elapsed().as_micros() as u64);
            pushed?;
            self.output_batches = self.output_batches.saturating_add(1);
            self.output_rows = self.output_rows.saturating_add(row_count as u64);
            self.max_output_batch_rows = self.max_output_batch_rows.max(row_count);
            Ok(())
        });
        rows.clear();
        self.output_batch = rows;
        result
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
            output_batches = self.output_batches,
            output_rows = self.output_rows,
            max_output_batch_rows = self.max_output_batch_rows,
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
        // worker. Residual transform state is task-local and is not reconstructed
        // from the immutable build table, so those plans retain the serial path.
        if matches!(self.join_type, JoinType::Right | JoinType::Full)
            || self.residual_transform.is_some()
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
            || std::mem::discriminant(&self.join_type) != std::mem::discriminant(&target.join_type)
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
        target.output_batches = target.output_batches.saturating_add(self.output_batches);
        target.output_rows = target.output_rows.saturating_add(self.output_rows);
        target.max_output_batch_rows = target.max_output_batch_rows.max(self.max_output_batch_rows);
        target.probe_us = target.probe_us.saturating_add(self.probe_us);
        target.view_compose_us = target.view_compose_us.saturating_add(self.view_compose_us);
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
