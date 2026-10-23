use std::sync::Arc;
use std::time::Instant;

use arrow::datatypes::DataType;

use crate::pipeline::{
    DataChunkView, MaterializationReport, PipelineNode, Transform, ViewColumnRef,
};
use common::Result;
use error::VeinError;

use super::super::hash_table::HashTable;
use super::super::hash_table::probe::metrics::ProbeMetrics;
use super::super::rows::{JoinType, OutputRow};
use super::super::spec::OutputColumn;
use super::JoinFilterCollector;
use super::output::JoinOutputComposer;

type JoinMatch = (u32, (u32, u32));
mod execute;
mod residual;
mod output;

mod parallel;
mod finish;
mod match_memory;

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
    next: Box<dyn PipelineNode>,
    /// Coalesces matches across retained build chunks into bounded views.
    output_composer: JoinOutputComposer,
    output_batch: Vec<OutputRow>,
    residual_view_columns: Vec<ViewColumnRef>,
    residual_composer: Option<JoinOutputComposer>,
    residual_rows: Vec<OutputRow>,
    residual_transform: Option<Box<dyn Transform>>,
    residual_output: Option<JoinFilterCollector>,
    /// The build rows this task consumed, for the flavours that owe unmatched
    /// ones. Kept here, not in the table, because the table is shared.
    matched_build: Vec<u64>,
    build_chunk_offsets: Vec<usize>,
    build_row_count: usize,
    /// Only the canonical node emits unmatched build rows after worker combine.
    emit_unmatched_build: bool,
    matched_build_memory: Option<crate::memory::MemoryReservation>,
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
    pub(super) max_candidate_batch_rows: usize,
    pub(super) candidate_rows_examined: u64,
    pub(super) probe_metrics: ProbeMetrics,
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
        next: Box<dyn PipelineNode>,
    ) -> Self {
        let output_columns = view_column_refs(&columns);
        let output_composer = JoinOutputComposer::new(&output_columns, &build_types);
        let build_batch_rows = (0..table.chunk_count())
            .map(|chunk| table.build_chunk(chunk as u32).num_rows())
            .max()
            .unwrap_or(1);
        let mut build_chunk_offsets = Vec::with_capacity(table.chunk_count());
        let mut build_row_count = 0usize;
        for chunk in 0..table.chunk_count() {
            build_chunk_offsets.push(build_row_count);
            build_row_count = build_row_count.saturating_add(
                table.build_chunk(chunk as u32).num_rows(),
            );
        }
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
            residual_view_columns: Vec::new(),
            residual_composer: None,
            residual_rows: Vec::new(),
            residual_transform: None,
            residual_output: None,
            matched_build: Vec::new(),
            build_chunk_offsets,
            build_row_count,
            emit_unmatched_build: true,
            matched_build_memory: None,
            probed_rows: 0,
            matched_rows: 0,
            probe_us: 0,
            view_compose_us: 0,
            downstream_push_us: 0,
            output_batches: 0,
            output_rows: 0,
            max_output_batch_rows: 0,
            max_candidate_batch_rows: 0,
            candidate_rows_examined: 0,
            probe_metrics: ProbeMetrics::default(),
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
        transform: Box<dyn Transform>,
        output: JoinFilterCollector,
    ) -> Self {
        self.residual_view_columns = view_column_refs(&columns);
        self.residual_composer = Some(JoinOutputComposer::new(&self.residual_view_columns, &self.build_types));
        self.residual_transform = Some(transform);
        self.residual_output = Some(output);
        self
    }
}

impl PipelineNode for HashJoin {
    fn push(&mut self, view: DataChunkView) -> Result<()> {
        self.probe(view)
    }

    fn flush(&mut self) -> Result<()> {
        self.finish()
    }
    /// Whether the pipeline is done is decided below the join, so the source's
    /// question has to pass through it.
    fn finished(&self) -> bool {
        self.next.finished()
    }

    fn fork_local(&self) -> Result<Option<Box<dyn PipelineNode>>> {
        self.fork_worker()
    }

    fn combine_local(self: Box<Self>, canonical: &mut dyn PipelineNode) -> Result<()> {
        self.combine_worker(canonical)
    }
}

impl crate::pipeline::Transform for HashJoin {}
