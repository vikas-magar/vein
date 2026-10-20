//! Blocking Arrow row sort that retains input column buffers until final gather.

use arrow::datatypes::{Field, Schema, SchemaRef};
use arrow::ipc::reader::FileReader;
use arrow::record_batch::RecordBatch;
use common::Result;
use common::protocol::{LimitSpec, SortPlan};
use error::VeinError;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::mem::size_of;
use std::sync::Arc;
use std::time::Instant;

use crate::memory::{MemoryReservation, QueryMemoryTracker};
use crate::spill::{IpcRunWriter, SpillRun, SpillStream};
use crate::pipeline::view::ViewMemoryLease;
use crate::pipeline::{DataChunkView, MaterializationReport, PipelineNode, Transform};

mod spill;
use spill::HeapSize;
mod view_order;
use view_order::ViewRowOrder;

const OUTPUT_BATCH_ROWS: usize = 8192;
const SPILL_MERGE_FAN_IN: usize = 32;

/// A row's location in the retained input chunks.
#[derive(Clone, Copy)]
struct RowRef {
    chunk: usize,
    row: usize,
}

impl HeapSize for RowRef {
    fn heap_size(&self) -> usize {
        0
    }
}

/// Retains the smallest `capacity` values using a max-heap whose root is the
/// worst retained candidate. The ordering is supplied by the caller so entries
/// can refer to source rows instead of owning copied sort keys.
struct BoundedTopN<T> {
    capacity: usize,
    entries: Vec<T>,
}

enum TopNChange<T> {
    Rejected,
    Inserted,
    Replaced(T),
}

impl<T> BoundedTopN<T> {
    fn new(capacity: usize) -> Self {
        Self {
            capacity,
            entries: Vec::new(),
        }
    }

    fn is_full(&self) -> bool {
        self.entries.len() >= self.capacity
    }

    fn peek(&self) -> Option<&T> {
        self.entries.first()
    }

    fn offer(
        &mut self,
        value: T,
        compare: &mut impl FnMut(&T, &T) -> Result<Ordering>,
    ) -> Result<TopNChange<T>> {
        if self.capacity == 0 {
            return Ok(TopNChange::Rejected);
        }
        if self.entries.len() < self.capacity {
            self.entries.push(value);
            self.sift_up(self.entries.len() - 1, compare)?;
            return Ok(TopNChange::Inserted);
        }
        if compare(&value, &self.entries[0])? != Ordering::Less {
            return Ok(TopNChange::Rejected);
        }
        let worst = std::mem::replace(&mut self.entries[0], value);
        self.sift_down(0, compare)?;
        Ok(TopNChange::Replaced(worst))
    }

    fn sift_up(
        &mut self,
        mut child: usize,
        compare: &mut impl FnMut(&T, &T) -> Result<Ordering>,
    ) -> Result<()> {
        while child > 0 {
            let parent = (child - 1) / 2;
            if compare(&self.entries[parent], &self.entries[child])? != Ordering::Less {
                break;
            }
            self.entries.swap(parent, child);
            child = parent;
        }
        Ok(())
    }

    fn sift_down(
        &mut self,
        mut parent: usize,
        compare: &mut impl FnMut(&T, &T) -> Result<Ordering>,
    ) -> Result<()> {
        loop {
            let left = parent * 2 + 1;
            if left >= self.entries.len() {
                return Ok(());
            }
            let right = left + 1;
            let worst_child = if right < self.entries.len()
                && compare(&self.entries[left], &self.entries[right])? == Ordering::Less
            {
                right
            } else {
                left
            };
            if compare(&self.entries[parent], &self.entries[worst_child])? != Ordering::Less {
                return Ok(());
            }
            self.entries.swap(parent, worst_child);
            parent = worst_child;
        }
    }

    fn sort_and_take_by(
        &mut self,
        compare: &mut impl FnMut(&T, &T) -> Result<Ordering>,
    ) -> Result<Vec<T>> {
        let mut compare_error = None;
        self.entries.sort_unstable_by(|left, right| {
            if compare_error.is_some() {
                return Ordering::Equal;
            }
            match compare(left, right) {
                Ok(ordering) => ordering,
                Err(error) => {
                    compare_error = Some(error);
                    Ordering::Equal
                }
            }
        });
        if let Some(error) = compare_error {
            return Err(error);
        }
        Ok(std::mem::take(&mut self.entries))
    }
}

/// Collects one task's views, sorts references to their rows, then composes output views.
///
/// Input arrays stay shared while row references are compared and sorted.
/// Payload values remain in their source Arrow arrays until a downstream
/// operator explicitly requests materialized arrays.
pub struct SortTransform {
    plan: SortPlan,
    output_columns: Option<usize>,
    chunks: Vec<Option<DataChunkView>>,
    top_n_references: Vec<usize>,
    top_n: Option<BoundedTopN<RowRef>>,
    metrics: SortMetrics,
    memory: MemoryReservation,
    tracker: QueryMemoryTracker,
    chunk_reservations: Vec<usize>,
    spill_runs: Vec<SpillRun>,
    spill_schema: Option<SchemaRef>,
    spilling: bool,
    next: Box<dyn PipelineNode<DataChunkView>>,
}

#[derive(Default)]
struct SortMetrics {
    /// Sort-key conversions on the external spill path only.
    key_materialization: MaterializationReport,
    key_materialize_us: u64,
    input_batches: u64,
    input_rows: u64,
    candidate_rows_examined: u64,
    candidate_insertions: u64,
    candidate_replacements: u64,
    row_comparisons: u64,
    comparator_cache_entries: u64,
    output_rows: u64,
    key_encode_us: u64,
    selection_us: u64,
    sort_us: u64,
    emit_us: u64,
    spill_runs: u64,
    spill_bytes: u64,
    spill_merge_passes: u64,
}

impl SortTransform {
    /// Create a global sort boundary for one task's input stream.
    pub fn new(plan: SortPlan, next: Box<dyn PipelineNode<DataChunkView>>) -> Self {
        Self::with_output_columns(plan, None, next)
    }

    /// Create a global sort that drops temporary sort-key columns from output.
    pub fn with_output_columns(
        plan: SortPlan,
        output_columns: Option<usize>,
        next: Box<dyn PipelineNode<DataChunkView>>,
    ) -> Self {
        Self::with_limit(plan, output_columns, None, next)
    }

    /// Create a global sort that retains at most `offset + count` candidates
    /// when a row window follows it.
    pub(crate) fn with_limit(
        plan: SortPlan,
        output_columns: Option<usize>,
        limit: Option<LimitSpec>,
        next: Box<dyn PipelineNode<DataChunkView>>,
    ) -> Self {
        Self::with_limit_and_memory_tracker(
            plan,
            output_columns,
            limit,
            QueryMemoryTracker::unlimited(common::cancellation::CancellationToken::new()),
            next,
        )
            .expect("unlimited sort budget must construct sort state")
    }

    /// Create a global sort that charges its reference and key buffers to `tracker`.
    pub fn with_memory_tracker(
        plan: SortPlan,
        output_columns: Option<usize>,
        tracker: QueryMemoryTracker,
        next: Box<dyn PipelineNode<DataChunkView>>,
    ) -> Result<Self> {
        Self::with_limit_and_memory_tracker(plan, output_columns, None, tracker, next)
    }

    fn with_limit_and_memory_tracker(
        plan: SortPlan,
        output_columns: Option<usize>,
        limit: Option<LimitSpec>,
        tracker: QueryMemoryTracker,
        next: Box<dyn PipelineNode<DataChunkView>>,
    ) -> Result<Self> {
        let candidate_limit = limit.map(|limit| {
            if limit.count == 0 {
                0
            } else {
                usize::try_from(limit.offset.saturating_add(limit.count)).unwrap_or(usize::MAX)
            }
        });
        let memory = tracker.reserve(0, "sort buffers")?;
        Ok(Self {
            plan,
            output_columns,
            chunks: Vec::new(),
            top_n_references: Vec::new(),
            top_n: candidate_limit.map(BoundedTopN::new),
            metrics: SortMetrics::default(),
            memory,
            tracker,
            chunk_reservations: Vec::new(),
            spill_runs: Vec::new(),
            spill_schema: None,
            spilling: false,
            next,
        })
    }

    fn sort_and_emit(&mut self) -> Result<()> {
        let order = if let Some(top_n) = self.top_n.as_mut() {
            let started = Instant::now();
            let mut ordering = ViewRowOrder::new(&self.plan, self.tracker.clone())?;
            let order = {
                let chunks = &self.chunks;
                let mut compare = |left: &RowRef, right: &RowRef| {
                    ordering.compare_rows(chunks, *left, *right)
                };
                top_n.sort_and_take_by(&mut compare)?
            };
            self.metrics.row_comparisons = self
                .metrics
                .row_comparisons
                .saturating_add(ordering.comparisons());
            self.metrics.comparator_cache_entries = self
                .metrics
                .comparator_cache_entries
                .saturating_add(ordering.cache_entries_created());
            self.metrics.sort_us = self
                .metrics
                .sort_us
                .saturating_add(started.elapsed().as_micros() as u64);
            order
        } else {
            self.sort_all_rows()?
        };
        self.metrics.output_rows = order.len() as u64;
        let emit_started = Instant::now();
        let result = self.emit_rows(order);
        self.metrics.emit_us = self
            .metrics
            .emit_us
            .saturating_add(emit_started.elapsed().as_micros() as u64);
        result
    }

    fn sort_all_rows(&mut self) -> Result<Vec<RowRef>> {
        let Some(first) = self.chunks.iter().find_map(Option::as_ref) else {
            return Ok(Vec::new());
        };
        let output_columns = self.output_columns.unwrap_or(first.num_columns());
        if output_columns > first.num_columns() {
            return Err(VeinError::planning(
                "ORDER output width exceeds the sort input schema",
                None,
            ));
        }

        let mut order = Vec::new();
        for (chunk_index, chunk) in self.chunks.iter().enumerate() {
            let Some(chunk) = chunk else {
                continue;
            };
            for active_row in 0..chunk.num_rows() {
                order.push(RowRef {
                    chunk: chunk_index,
                    row: active_row,
                });
            }
        }

        let sort_started = Instant::now();
        let mut ordering = ViewRowOrder::new(&self.plan, self.tracker.clone())?;
        let mut compare_error = None;
        order.sort_unstable_by(|left, right| {
            if compare_error.is_some() {
                return Ordering::Equal;
            }
            match ordering.compare_rows(&self.chunks, *left, *right) {
                Ok(ordering) => ordering,
                Err(error) => {
                    compare_error = Some(error);
                    Ordering::Equal
                }
            }
        });
        if let Some(error) = compare_error {
            return Err(error);
        }
        self.metrics.row_comparisons = self
            .metrics
            .row_comparisons
            .saturating_add(ordering.comparisons());
        self.metrics.comparator_cache_entries = self
            .metrics
            .comparator_cache_entries
            .saturating_add(ordering.cache_entries_created());
        self.metrics.sort_us = self
            .metrics
            .sort_us
            .saturating_add(sort_started.elapsed().as_micros() as u64);
        Ok(order)
    }

    fn emit_rows(&mut self, order: Vec<RowRef>) -> Result<()> {
        let Some(first_row) = order.first() else {
            return Ok(());
        };
        let first = self.chunks[first_row.chunk].as_ref().ok_or_else(|| {
            VeinError::internal("sort candidate references a released input chunk")
        })?;
        let output_columns = self.output_columns.unwrap_or(first.num_columns());
        if output_columns > first.num_columns() {
            return Err(VeinError::planning(
                "ORDER output width exceeds the sort input schema",
                None,
            ));
        }

        let mut chunk_positions = vec![None; self.chunks.len()];
        let mut inputs = Vec::new();
        for (index, chunk) in self.chunks.iter().enumerate() {
            if let Some(view) = chunk {
                chunk_positions[index] = Some(inputs.len());
                inputs.push(view);
            }
        }
        for rows in order.chunks(OUTPUT_BATCH_ROWS) {
            let indices: Vec<(usize, usize)> = rows
                .iter()
                .map(|row| {
                    chunk_positions[row.chunk]
                        .map(|chunk| (chunk, row.row))
                        .ok_or_else(|| {
                            VeinError::internal("sort candidate references a released input chunk")
                        })
                })
                .collect::<Result<_>>()?;
            self.next.push(DataChunkView::compose_rows(
                &inputs,
                &indices,
                output_columns,
            )?)?;
        }
        Ok(())
    }

    fn spill_schema_for(&mut self, view: &DataChunkView) -> Result<SchemaRef> {
        if let Some(schema) = &self.spill_schema {
            if schema.fields().len() != view.num_columns()
                || schema.fields().iter().enumerate().any(|(index, field)| {
                match view.column_data_type(index) {
                    Ok(data_type) => field.data_type() != data_type,
                    Err(_) => true,
                }
            })
            {
                return Err(VeinError::internal(
                    "sort spill input schema changed between batches",
                ));
            }
            return Ok(Arc::clone(schema));
        }

        let fields = (0..view.num_columns())
            .map(|index| {
                let data_type = view.column_data_type(index).map_err(|_| {
                    VeinError::planning(
                        format!("sort spill column {index} is outside the input schema"),
                        None,
                    )
                })?;
                Ok(Field::new(
                    format!("column_{index}"),
                    data_type.clone(),
                    true,
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        let schema = Arc::new(Schema::new(fields));
        self.spill_schema = Some(Arc::clone(&schema));
        Ok(schema)
    }

    /// Sorts one bounded input view and writes its rows as an Arrow IPC run.
    /// The conversion to Arrow arrays happens at this disk boundary; the normal
    /// in-memory path continues to compose references to its source arrays.
    fn spill_view(&mut self, view: &DataChunkView) -> Result<()> {
        if view.is_empty() {
            return Ok(());
        }
        let schema = self.spill_schema_for(view)?;
        let row_bytes = view
            .num_rows()
            .checked_mul(size_of::<usize>())
            .ok_or_else(|| VeinError::internal("sort spill row index size overflow"))?;
        self.memory.grow(row_bytes, "sort spill row indexes")?;
        let result = (|| {
            let mut order = (0..view.num_rows()).collect::<Vec<_>>();
            let sort_started = Instant::now();
            let mut ordering = ViewRowOrder::new(&self.plan, self.tracker.clone())?;
            let mut compare_error = None;
            order.sort_unstable_by(|left, right| {
                if compare_error.is_some() {
                    return Ordering::Equal;
                }
                match ordering
                    .compare_view_rows(view, *left, view, *right)
                    .map(|ordering| ordering.then_with(|| left.cmp(right)))
                {
                    Ok(ordering) => ordering,
                    Err(error) => {
                        compare_error = Some(error);
                        Ordering::Equal
                    }
                }
            });
            if let Some(error) = compare_error {
                return Err(error);
            }
            self.metrics.row_comparisons = self
                .metrics
                .row_comparisons
                .saturating_add(ordering.comparisons());
            self.metrics.comparator_cache_entries = self
                .metrics
                .comparator_cache_entries
                .saturating_add(ordering.cache_entries_created());
            ordering.clear_cache();
            self.metrics.sort_us = self
                .metrics
                .sort_us
                .saturating_add(sort_started.elapsed().as_micros() as u64);

            let mut pending = self.tracker.spill_manager().create_run()?;
            let output = pending.writer()?;
            let file_started = Instant::now();
            let mut writer = IpcRunWriter::new(output, &schema, "create sort spill writer")?;
            for rows in order.chunks(OUTPUT_BATCH_ROWS) {
                self.tracker.check_cancelled()?;
                let row_refs = rows.iter().map(|row| (0, *row)).collect::<Vec<_>>();
                let sorted_view =
                    DataChunkView::compose_rows(&[view], &row_refs, view.num_columns())?;
                let (batch, report) =
                    sorted_view.to_record_batch_with_report(Arc::clone(&schema))?;
                self.metrics.key_materialization.accumulate(report);
                writer.write_batch(&batch, "write sort spill batch")?;
            }
            let output = writer.finish("finish sort spill run")?;
            let run = pending.commit(output)?;
            let bytes = run.bytes();
            self.spill_runs.push(run);
            self.metrics.spill_runs = self.metrics.spill_runs.saturating_add(1);
            self.metrics.spill_bytes = self.metrics.spill_bytes.saturating_add(bytes);
            self.metrics.emit_us = self
                .metrics
                .emit_us
                .saturating_add(file_started.elapsed().as_micros() as u64);
            Ok(())
        })();
        self.memory.shrink(row_bytes);
        result
    }

    fn spill_merged_group(&mut self, paths: &[SpillRun]) -> Result<SpillRun> {
        let schema = self
            .spill_schema
            .as_ref()
            .cloned()
            .ok_or_else(|| VeinError::internal("sort spill schema is missing"))?;
        let mut pending = self.tracker.spill_manager().create_run()?;
        let output = pending.writer()?;
        let mut writer = IpcRunWriter::new(output, &schema, "create merged sort run")?;
        let plan = self.plan.clone();
        merge_spill_group(
            paths,
            &plan,
            schema.fields().len(),
            &self.tracker,
            &mut self.metrics,
            |view| {
                let (batch, _) =
                    view.to_record_batch_with_report(Arc::clone(&schema))?;
                writer.write_batch(&batch, "write merged sort batch")
            },
        )?;
        let output = writer.finish("finish merged sort run")?;
        let run = pending.commit(output)?;
        let bytes = run.bytes();
        self.metrics.spill_bytes = self.metrics.spill_bytes.saturating_add(bytes);
        self.metrics.spill_runs = self.metrics.spill_runs.saturating_add(1);
        Ok(run)
    }

    fn merge_spill_runs(&mut self) -> Result<()> {
        if self.spill_runs.is_empty() {
            return Ok(());
        }
        let mut runs = std::mem::take(&mut self.spill_runs);
        while runs.len() > SPILL_MERGE_FAN_IN {
            let mut next_pass = Vec::with_capacity(runs.len().div_ceil(SPILL_MERGE_FAN_IN));
            for group in runs.chunks(SPILL_MERGE_FAN_IN) {
                if group.len() == 1 {
                    next_pass.push(group[0].clone());
                    continue;
                }
                let merged = self.spill_merged_group(group)?;
                next_pass.push(merged);
            }
            runs = next_pass;
            self.metrics.spill_merge_passes = self.metrics.spill_merge_passes.saturating_add(1);
        }

        let schema = self
            .spill_schema
            .as_ref()
            .cloned()
            .ok_or_else(|| VeinError::internal("sort spill schema is missing"))?;
        let output_columns = self.output_columns.unwrap_or(schema.fields().len());
        if output_columns > schema.fields().len() {
            return Err(VeinError::planning(
                "ORDER output width exceeds the spill input schema",
                None,
            ));
        }
        let plan = self.plan.clone();
        let next = &mut self.next;
        let mut output_rows = 0u64;
        merge_spill_group(
            &runs,
            &plan,
            output_columns,
            &self.tracker,
            &mut self.metrics,
            |view| {
                output_rows = output_rows.saturating_add(view.num_rows() as u64);
                next.push(view)
            },
        )?;
        self.metrics.output_rows = output_rows;
        Ok(())
    }

    fn account_retained_arrays(&self, view: &mut DataChunkView) -> Result<()> {
        let tracker_id = self.tracker.identity();
        view.account_source_arrays(tracker_id, |identity, bytes| {
            let lease: Arc<dyn ViewMemoryLease> = self
                .tracker
                .lease_retained_array(identity, bytes, "sort retained arrays")?;
            Ok(lease)
        })
    }

    fn spill_retained_chunks(&mut self) -> Result<()> {
        self.spilling = true;
        for (index, retained) in std::mem::take(&mut self.chunks).into_iter().enumerate() {
            let Some(retained) = retained else {
                continue;
            };
            let reserved = self.chunk_reservations.get(index).copied().unwrap_or(0);
            self.memory.shrink(reserved);
            self.spill_view(&retained)?;
        }
        self.chunk_reservations.clear();
        Ok(())
    }

    fn push_top_n(&mut self, chunk: DataChunkView) -> Result<()> {
        if chunk.is_empty() {
            return Ok(());
        }
        let Some(top_n) = self.top_n.as_ref() else {
            return Err(VeinError::internal("Top-N selector is missing"));
        };
        if top_n.capacity == 0 {
            return Ok(());
        }
        self.metrics.input_batches = self.metrics.input_batches.saturating_add(1);
        self.metrics.input_rows = self
            .metrics
            .input_rows
            .saturating_add(chunk.num_rows() as u64);

        let row_count = chunk.num_rows();
        let selection_started = Instant::now();
        let chunk_index = self.chunks.len();
        let chunk_metadata = size_of::<Option<DataChunkView>>()
            .checked_add(size_of::<usize>())
            .ok_or_else(|| VeinError::internal("sort chunk metadata size overflow"))?;
        let mut retained_chunk = None;
        let mut ordering = ViewRowOrder::new(&self.plan, self.tracker.clone())?;
        for active_row in 0..row_count {
            self.metrics.candidate_rows_examined =
                self.metrics.candidate_rows_examined.saturating_add(1);
            let candidate_row = RowRef {
                chunk: chunk_index,
                row: active_row,
            };
            let is_full = self.top_n.as_ref().is_some_and(BoundedTopN::is_full);
            if is_full {
                let worst = *self
                    .top_n
                    .as_ref()
                    .and_then(BoundedTopN::peek)
                    .ok_or_else(|| VeinError::internal("full Top-N heap is empty"))?;
                let worst_view = self
                    .chunks
                    .get(worst.chunk)
                    .and_then(Option::as_ref)
                    .ok_or_else(|| {
                        VeinError::internal("Top-N candidate references a released chunk")
                    })?;
                let key_order = ordering.compare_view_rows(&chunk, active_row, worst_view, worst.row)?;
                let candidate_order = key_order
                    .then_with(|| candidate_row.chunk.cmp(&worst.chunk))
                    .then_with(|| candidate_row.row.cmp(&worst.row));
                if candidate_order != Ordering::Less {
                    continue;
                }
            }

            if self.chunks.len() == chunk_index {
                let mut retained = chunk.clone();
                self.account_retained_arrays(&mut retained)?;
                self.memory.grow(chunk_metadata, "sort chunk references")?;
                self.chunks.push(Some(retained.clone()));
                self.top_n_references.push(0);
                retained_chunk = Some(retained);
            } else if self.chunks[chunk_index].is_none() {
                let retained = if let Some(retained) = retained_chunk.as_ref() {
                    retained.clone()
                } else {
                    let mut retained = chunk.clone();
                    self.account_retained_arrays(&mut retained)?;
                    retained_chunk = Some(retained.clone());
                    retained
                };
                self.chunks[chunk_index] = Some(retained);
            }

            if !is_full {
                self.memory
                    .grow(size_of::<RowRef>(), "sort Top-N row references")?;
            }
            let change = {
                let top_n = self.top_n.as_mut().expect("Top-N selector checked above");
                let chunks = &self.chunks;
                let mut compare = |left: &RowRef, right: &RowRef| {
                    ordering.compare_rows(chunks, *left, *right)
                };
                top_n.offer(candidate_row, &mut compare)?
            };
            let replaced_row = match change {
                TopNChange::Rejected => {
                    if !is_full {
                        self.memory.shrink(size_of::<RowRef>());
                    }
                    continue;
                }
                TopNChange::Inserted => None,
                TopNChange::Replaced(row) => Some(row),
            };
            self.metrics.candidate_insertions = self.metrics.candidate_insertions.saturating_add(1);
            self.top_n_references[chunk_index] = self.top_n_references[chunk_index]
                .checked_add(1)
                .ok_or_else(|| VeinError::internal("Top-N candidate count overflow"))?;
            if let Some(replaced_row) = replaced_row {
                self.metrics.candidate_replacements =
                    self.metrics.candidate_replacements.saturating_add(1);
                self.top_n_references[replaced_row.chunk] = self.top_n_references
                    [replaced_row.chunk]
                    .checked_sub(1)
                    .ok_or_else(|| VeinError::internal("Top-N candidate count underflow"))?;
                if self.top_n_references[replaced_row.chunk] == 0 {
                    ordering.clear_cache();
                    self.chunks[replaced_row.chunk] = None;
                    if replaced_row.chunk == chunk_index {
                        retained_chunk = None;
                    }
                }
            }
        }
        self.metrics.row_comparisons = self
            .metrics
            .row_comparisons
            .saturating_add(ordering.comparisons());
        self.metrics.comparator_cache_entries = self
            .metrics
            .comparator_cache_entries
            .saturating_add(ordering.cache_entries_created());
        self.metrics.selection_us = self
            .metrics
            .selection_us
            .saturating_add(selection_started.elapsed().as_micros() as u64);
        if self.top_n_references.get(chunk_index).copied().unwrap_or(0) == 0
            && self.chunks.len() > chunk_index
        {
            ordering.clear_cache();
            self.chunks[chunk_index] = None;
            drop(retained_chunk.take());
        }
        Ok(())
    }
}

struct SpillMergeCursor {
    reader: FileReader<Box<dyn SpillStream>>,
    view: DataChunkView,
    row: usize,
    snapshot: usize,
}

fn view_from_spill_batch(
    batch: RecordBatch,
    tracker: &QueryMemoryTracker,
) -> Result<DataChunkView> {
    let mut view = DataChunkView::from_record_batch(batch)?;
    let leases = view
        .source_array_memory_sizes()?
        .into_iter()
        .map(|(identity, bytes)| {
            let lease: Arc<dyn ViewMemoryLease> =
                tracker.lease_retained_array(identity, bytes, "sort spill arrays")?;
            Ok((identity, lease))
        })
        .collect::<Result<HashMap<_, _>>>()?;
    view.attach_memory_leases(&leases);
    Ok(view)
}

/// Merge at most `SPILL_MERGE_FAN_IN` sorted IPC runs and pass bounded batches
/// of reference-backed rows to the caller. The callback either writes an
/// intermediate run or forwards the final views downstream.
fn merge_spill_group(
    paths: &[SpillRun],
    plan: &SortPlan,
    output_columns: usize,
    tracker: &QueryMemoryTracker,
    metrics: &mut SortMetrics,
    mut consume: impl FnMut(DataChunkView) -> Result<()>,
) -> Result<()> {
    if paths.is_empty() {
        return Ok(());
    }
    let workspace_bytes = paths
        .len()
        .checked_mul(
            size_of::<FileReader<Box<dyn SpillStream>>>()
                .saturating_add(size_of::<Option<DataChunkView>>())
                .saturating_add(size_of::<SpillMergeCursor>())
                .saturating_add(size_of::<&DataChunkView>()),
        )
        .and_then(|bytes| {
            OUTPUT_BATCH_ROWS
                .checked_mul(size_of::<(usize, usize)>())
                .and_then(|row_bytes| bytes.checked_add(row_bytes))
        })
        .ok_or_else(|| VeinError::internal("sort merge workspace size overflow"))?;
    let _workspace_memory = tracker.reserve(workspace_bytes, "sort merge workspace")?;
    let mut readers = paths
        .iter()
        .map(|run| {
            FileReader::try_new(run.open()?, None)
                .map_err(|error| VeinError::internal(format!("read sort spill run: {error}")))
        })
        .collect::<Result<Vec<_>>>()?;

    let mut initial = Vec::with_capacity(readers.len());
    let mut first_view = None;
    for reader in &mut readers {
        let batch = reader
            .next()
            .transpose()
            .map_err(|error| VeinError::internal(format!("decode sort spill batch: {error}")))?;
        let view = batch
            .map(|batch| view_from_spill_batch(batch, tracker))
            .transpose()?;
        if first_view.is_none() {
            first_view = view.clone();
        }
        initial.push(view);
    }
    let Some(first_view) = first_view else {
        return Ok(());
    };
    for key in &plan.keys {
        first_view
            .column_data_type(key.column as usize)
            .map_err(|_| {
                VeinError::planning(
                    format!("ORDER BY column {} is outside the spill schema", key.column),
                    None,
                )
            })?;
    }
    drop(first_view);

    let mut snapshots = Vec::<DataChunkView>::new();
    let mut cursors = Vec::with_capacity(readers.len());
    let mut heap = spill::KWayMerge::<RowRef, (usize, usize)>::new(tracker)?;
    let mut ordering = ViewRowOrder::new(plan, tracker.clone())?;
    for (reader, view) in readers.into_iter().zip(initial) {
        let Some(view) = view else {
            continue;
        };
        let snapshot = snapshots.len();
        snapshots.push(view.clone());
        let cursor_index = cursors.len();
        cursors.push(SpillMergeCursor {
            reader,
            view,
            row: 0,
            snapshot,
        });
        let key = RowRef {
            chunk: cursor_index,
            row: 0,
        };
        let mut compare = |left: &RowRef, right: &RowRef| {
            compare_spill_rows(&mut ordering, &cursors, *left, *right)
        };
        heap.push(key, (0, 0), (cursor_index, 0), &mut compare)?;
    }

    let mut rows = Vec::<(usize, usize)>::with_capacity(OUTPUT_BATCH_ROWS);
    loop {
        let next = {
            let mut compare = |left: &RowRef, right: &RowRef| {
                compare_spill_rows(&mut ordering, &cursors, *left, *right)
            };
            heap.pop(&mut compare)?
        };
        let Some((run, row)) = next else {
            break;
        };
        tracker.check_cancelled()?;
        let cursor = &mut cursors[run];
        rows.push((cursor.snapshot, row));
        cursor.row += 1;
        if cursor.row < cursor.view.num_rows() {
            let next_row = cursor.row;
            let key = RowRef {
                chunk: run,
                row: next_row,
            };
            let mut compare = |left: &RowRef, right: &RowRef| {
                compare_spill_rows(&mut ordering, &cursors, *left, *right)
            };
            heap.push(key, (0, 0), (run, next_row), &mut compare)?;
        } else {
            let batch = cursor.reader.next().transpose().map_err(|error| {
                VeinError::internal(format!("decode sort spill batch: {error}"))
            })?;
            if let Some(batch) = batch {
                ordering.clear_cache();
                cursor.view = view_from_spill_batch(batch, tracker)?;
                cursor.row = 0;
                cursor.snapshot = snapshots.len();
                snapshots.push(cursor.view.clone());
                let key = RowRef { chunk: run, row: 0 };
                let mut compare = |left: &RowRef, right: &RowRef| {
                    compare_spill_rows(&mut ordering, &cursors, *left, *right)
                };
                heap.push(key, (0, 0), (run, 0), &mut compare)?;
            }
        }

        if rows.len() == OUTPUT_BATCH_ROWS {
            let inputs = snapshots.iter().collect::<Vec<_>>();
            let output = DataChunkView::compose_rows(&inputs, &rows, output_columns)?;
            consume(output)?;
            rows.clear();
            snapshots.clear();
            for cursor in &mut cursors {
                if cursor.row < cursor.view.num_rows() {
                    cursor.snapshot = snapshots.len();
                    snapshots.push(cursor.view.clone());
                }
            }
        }
    }
    if !rows.is_empty() {
        let inputs = snapshots.iter().collect::<Vec<_>>();
        let output = DataChunkView::compose_rows(&inputs, &rows, output_columns)?;
        consume(output)?;
    }
    metrics.row_comparisons = metrics
        .row_comparisons
        .saturating_add(ordering.comparisons());
    metrics.comparator_cache_entries = metrics
        .comparator_cache_entries
        .saturating_add(ordering.cache_entries_created());
    Ok(())
}

fn compare_spill_rows(
    ordering: &mut ViewRowOrder<'_>,
    cursors: &[SpillMergeCursor],
    left: RowRef,
    right: RowRef,
) -> Result<Ordering> {
    let left_view = cursors
        .get(left.chunk)
        .ok_or_else(|| VeinError::internal("sort merge row references a missing run"))?;
    let right_view = cursors
        .get(right.chunk)
        .ok_or_else(|| VeinError::internal("sort merge row references a missing run"))?;
    ordering
        .compare_view_rows(&left_view.view, left.row, &right_view.view, right.row)
        .map(|ordering| ordering.then_with(|| left.chunk.cmp(&right.chunk)).then_with(|| left.row.cmp(&right.row)))
}

impl PipelineNode<DataChunkView> for SortTransform {
    fn push(&mut self, chunk: DataChunkView) -> Result<()> {
        if self.top_n.is_some() {
            self.push_top_n(chunk)
        } else if self.spilling {
            if !chunk.is_empty() {
                self.metrics.input_batches = self.metrics.input_batches.saturating_add(1);
                self.metrics.input_rows = self
                    .metrics
                    .input_rows
                    .saturating_add(chunk.num_rows() as u64);
            }
            self.spill_view(&chunk)
        } else if !chunk.is_empty() {
            self.metrics.input_batches = self.metrics.input_batches.saturating_add(1);
            self.metrics.input_rows = self
                .metrics
                .input_rows
                .saturating_add(chunk.num_rows() as u64);
            let mut chunk = chunk;
            let array_accounting = self.account_retained_arrays(&mut chunk);
            if let Err(error) = array_accounting {
                if !matches!(error, VeinError::ResourceExhausted { .. }) {
                    return Err(error);
                }
                self.spill_retained_chunks()?;
                self.spill_view(&chunk)?;
                return Ok(());
            }
            let rows_bytes = chunk
                .num_rows()
                .checked_mul(size_of::<RowRef>())
                .and_then(|bytes| {
                    bytes.checked_add(size_of::<Option<DataChunkView>>() + size_of::<usize>())
                })
                .ok_or_else(|| VeinError::internal("sort buffer size overflow"))?;
            if let Err(error) = self.memory.grow(rows_bytes, "sort row references") {
                if !matches!(error, VeinError::ResourceExhausted { .. }) {
                    return Err(error);
                }
                self.spill_retained_chunks()?;
                self.spill_view(&chunk)?;
                return Ok(());
            }
            self.chunks.push(Some(chunk));
            self.chunk_reservations.push(rows_bytes);
            Ok(())
        } else {
            Ok(())
        }
    }

    fn flush(&mut self) -> Result<()> {
        if self.spilling {
            self.merge_spill_runs()?;
        } else {
            self.sort_and_emit()?;
        }
        tracing::info!(
            target: "vein.audit",
            event = "sort_finished",
            mode = if self.top_n.is_some() {
                "top_n"
            } else if self.spilling {
                "external_merge_sort"
            } else {
                "full_sort"
            },
            keys = self.plan.keys.len(),
            input_batches = self.metrics.input_batches,
            input_rows = self.metrics.input_rows,
            candidate_rows_examined = self.metrics.candidate_rows_examined,
            candidate_insertions = self.metrics.candidate_insertions,
            candidate_replacements = self.metrics.candidate_replacements,
            row_comparisons = self.metrics.row_comparisons,
            comparator_cache_entries = self.metrics.comparator_cache_entries,
            output_rows = self.metrics.output_rows,
            key_encode_us = self.metrics.key_encode_us,
            key_shared_columns = self.metrics.key_materialization.shared_columns,
            key_gathered_columns = self.metrics.key_materialization.gathered_columns,
            key_empty_columns = self.metrics.key_materialization.empty_columns,
            key_gathered_output_buffer_bytes = self.metrics.key_materialization.gathered_output_buffer_bytes,
            key_materialize_us = self.metrics.key_materialize_us,
            selection_us = self.metrics.selection_us,
            sort_us = self.metrics.sort_us,
            emit_us = self.metrics.emit_us,
            spill_runs = self.metrics.spill_runs,
            spill_bytes = self.metrics.spill_bytes,
            spill_merge_passes = self.metrics.spill_merge_passes,
            "sort_finished"
        );
        self.chunks.clear();
        self.chunk_reservations.clear();
        self.top_n_references.clear();
        self.next.flush()
    }

    fn finished(&self) -> bool {
        self.top_n.as_ref().is_some_and(|top_n| top_n.capacity == 0)
    }
}

impl Transform<DataChunkView> for SortTransform {}

#[cfg(test)]
mod tests;

#[cfg(test)]
#[path = "sort/view_order_tests.rs"]
mod view_order_tests;
