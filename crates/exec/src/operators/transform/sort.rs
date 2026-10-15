//! Blocking Arrow row sort that retains input column buffers until final gather.

use arrow::array::ArrayRef;
use arrow::compute::SortOptions;
use arrow::datatypes::{Field, Schema, SchemaRef};
use arrow::error::ArrowError;
use arrow::ipc::reader::FileReader;
use arrow::record_batch::RecordBatch;
use arrow_row::{RowConverter, Rows, SortField};
use common::Result;
use common::protocol::{LimitSpec, SortPlan};
use error::VeinError;
use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap};
use std::mem::size_of;
use std::sync::Arc;
use std::time::Instant;

use crate::memory::{MemoryReservation, QueryMemoryTracker};
use crate::spill::{IpcRunWriter, SpillRun, SpillStream};
use crate::pipeline::view::ViewMemoryLease;
use crate::pipeline::{DataChunkView, MaterializationReport, PipelineNode, Transform};

mod spill;
use spill::HeapSize;

const OUTPUT_BATCH_ROWS: usize = 8192;
const SPILL_MERGE_FAN_IN: usize = 32;

/// A row's location in the retained input chunks.
#[derive(Clone, Copy)]
struct RowRef {
    chunk: usize,
    row: usize,
}

/// A candidate owns only its encoded sort key; its payload stays in the input
/// Arrow chunk referenced by `row`.
struct TopNEntry {
    key: Vec<u8>,
    row: RowRef,
}

impl PartialEq for TopNEntry {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key && self.row.chunk == other.row.chunk && self.row.row == other.row.row
    }
}

impl Eq for TopNEntry {}

impl PartialOrd for TopNEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for TopNEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        self.key
            .cmp(&other.key)
            .then_with(|| self.row.chunk.cmp(&other.row.chunk))
            .then_with(|| self.row.row.cmp(&other.row.row))
    }
}

/// Retains the smallest `capacity` values using a max-heap whose root is the
/// worst retained candidate. Generic over `Ord` so the selection logic is
/// reusable without dynamic dispatch.
struct BoundedTopN<T: Ord> {
    capacity: usize,
    entries: BinaryHeap<T>,
}

impl<T: Ord> BoundedTopN<T> {
    fn new(capacity: usize) -> Self {
        Self {
            capacity,
            entries: BinaryHeap::new(),
        }
    }

    fn is_full(&self) -> bool {
        self.entries.len() >= self.capacity
    }

    fn peek(&self) -> Option<&T> {
        self.entries.peek()
    }

    fn push(&mut self, value: T) {
        if self.capacity == 0 {
            return;
        }
        if self.entries.len() < self.capacity {
            self.entries.push(value);
        } else if self.entries.peek().is_some_and(|worst| value < *worst) {
            self.entries.pop();
            self.entries.push(value);
        }
    }

    fn replace_worst_with<R>(&mut self, replace: impl FnOnce(&mut T) -> R) -> Option<R> {
        let mut worst = self.entries.peek_mut()?;
        Some(replace(&mut worst))
    }

    fn drain_sorted(&mut self) -> Vec<T> {
        std::mem::take(&mut self.entries).into_sorted_vec()
    }
}

/// Collects one task's views, sorts references to their rows, then composes output views.
///
/// Input arrays stay shared while the keys are encoded and sorted. Sort-key
/// arrays and encoded keys may be materialized, but payload columns remain
/// references until a downstream operator explicitly requests Arrow arrays.
pub struct SortTransform {
    plan: SortPlan,
    output_columns: Option<usize>,
    chunks: Vec<Option<DataChunkView>>,
    top_n_references: Vec<usize>,
    top_n: Option<BoundedTopN<TopNEntry>>,
    row_converter: Option<RowConverter>,
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
    /// Sort-key array conversions, excluding encoded row keys and payloads.
    key_materialization: MaterializationReport,
    key_materialize_us: u64,
    input_batches: u64,
    input_rows: u64,
    candidate_rows_examined: u64,
    candidate_insertions: u64,
    candidate_replacements: u64,
    candidate_key_buffer_allocations: u64,
    candidate_key_bytes_copied: u64,
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
            row_converter: None,
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

    fn sort_fields(&self, first: &DataChunkView) -> Result<Vec<SortField>> {
        if self.plan.keys.is_empty() {
            return Err(VeinError::planning(
                "ORDER requires at least one sort key",
                None,
            ));
        }

        self.plan
            .keys
            .iter()
            .map(|key| {
                let data_type = first.column_data_type(key.column as usize).map_err(|_| {
                    VeinError::planning(
                        format!("ORDER BY column {} is outside the input schema", key.column),
                        None,
                    )
                })?;
                Ok(SortField::new_with_options(
                    data_type.clone(),
                    SortOptions {
                        descending: !key.ascending,
                        nulls_first: key.nulls_first,
                    },
                ))
            })
            .collect()
    }

    fn sort_key_columns(
        &self,
        chunk: &DataChunkView,
    ) -> Result<(Vec<ArrayRef>, MaterializationReport, u64)> {
        let started = Instant::now();
        let mut report = MaterializationReport::default();
        let columns = self
            .plan
            .keys
            .iter()
            .map(|key| {
                chunk
                    .materialize_column_with_report(key.column as usize)
                    .map(|(array, column_report)| {
                        report.accumulate(column_report);
                        array
                    })
                    .map_err(|_| {
                        VeinError::planning(
                            format!("ORDER BY column {} is outside the input schema", key.column),
                            None,
                        )
                    })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok((
            columns,
            report,
            u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX),
        ))
    }

    fn sort_and_emit(&mut self) -> Result<()> {
        let order = if let Some(top_n) = self.top_n.as_mut() {
            let started = Instant::now();
            let order = top_n
                .drain_sorted()
                .into_iter()
                .map(|entry| entry.row)
                .collect::<Vec<_>>();
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

        let converter = RowConverter::new(self.sort_fields(first)?).map_err(row_sort_error)?;
        let mut encoded_keys = Vec::<Rows>::with_capacity(self.chunks.len());
        let mut order = Vec::new();

        let encode_started = Instant::now();
        for (chunk_index, chunk) in self.chunks.iter().enumerate() {
            let Some(chunk) = chunk else {
                continue;
            };
            let (key_columns, report, materialize_us) = self.sort_key_columns(chunk)?;
            self.metrics.key_materialization.accumulate(report);
            self.metrics.key_materialize_us = self
                .metrics
                .key_materialize_us
                .saturating_add(materialize_us);
            let keys = converter
                .convert_columns(&key_columns)
                .map_err(row_sort_error)?;
            let encoded_bytes = (0..chunk.num_rows())
                .try_fold(0usize, |bytes, row| {
                    bytes.checked_add(keys.row(row).data().len())
                })
                .ok_or_else(|| VeinError::internal("encoded sort key size overflow"))?;
            self.memory.grow(encoded_bytes, "sort encoded keys")?;
            for active_row in 0..chunk.num_rows() {
                order.push(RowRef {
                    chunk: chunk_index,
                    row: active_row,
                });
            }
            encoded_keys.push(keys);
        }
        self.metrics.key_encode_us = self
            .metrics
            .key_encode_us
            .saturating_add(encode_started.elapsed().as_micros() as u64);

        let sort_started = Instant::now();
        order.sort_unstable_by(|left, right| {
            encoded_keys[left.chunk]
                .row(left.row)
                .cmp(&encoded_keys[right.chunk].row(right.row))
        });
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
        let mut scratch_bytes = row_bytes;
        let result = (|| {
            let (key_columns, report, materialize_us) = self.sort_key_columns(view)?;
            self.metrics.key_materialization.accumulate(report);
            self.metrics.key_materialize_us = self
                .metrics
                .key_materialize_us
                .saturating_add(materialize_us);
            let converter = RowConverter::new(self.sort_fields(view)?).map_err(row_sort_error)?;
            let encode_started = Instant::now();
            let keys = converter
                .convert_columns(&key_columns)
                .map_err(row_sort_error)?;
            self.metrics.key_encode_us = self
                .metrics
                .key_encode_us
                .saturating_add(encode_started.elapsed().as_micros() as u64);
            let key_bytes = (0..view.num_rows())
                .try_fold(0usize, |total, row| {
                    total.checked_add(keys.row(row).data().len())
                })
                .ok_or_else(|| VeinError::internal("sort spill key size overflow"))?;
            let next_scratch = scratch_bytes
                .checked_add(key_bytes)
                .ok_or_else(|| VeinError::internal("sort spill scratch size overflow"))?;
            self.memory.grow(key_bytes, "sort spill encoded keys")?;
            scratch_bytes = next_scratch;

            let mut order = (0..view.num_rows()).collect::<Vec<_>>();
            let sort_started = Instant::now();
            order.sort_unstable_by(|left, right| keys.row(*left).cmp(&keys.row(*right)));
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
                let chunk = sorted_view.to_data_chunk()?;
                let batch =
                    RecordBatch::try_new(Arc::clone(&schema), chunk.vectors).map_err(|error| {
                        VeinError::internal(format!("build sort spill batch: {error}"))
                    })?;
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
        self.memory.shrink(scratch_bytes);
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
        merge_spill_group(paths, &plan, schema.fields().len(), &self.tracker, |view| {
            let chunk = view.to_data_chunk()?;
            let batch =
                RecordBatch::try_new(Arc::clone(&schema), chunk.vectors).map_err(|error| {
                    VeinError::internal(format!("build merged sort batch: {error}"))
                })?;
            writer.write_batch(&batch, "write merged sort batch")
        })?;
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
        merge_spill_group(&runs, &plan, output_columns, &self.tracker, |view| {
            output_rows = output_rows.saturating_add(view.num_rows() as u64);
            next.push(view)
        })?;
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

        if self.row_converter.is_none() {
            self.row_converter =
                Some(RowConverter::new(self.sort_fields(&chunk)?).map_err(row_sort_error)?);
        }
        let (key_columns, report, materialize_us) = self.sort_key_columns(&chunk)?;
        self.metrics.key_materialization.accumulate(report);
        self.metrics.key_materialize_us = self
            .metrics
            .key_materialize_us
            .saturating_add(materialize_us);
        let encode_started = Instant::now();
        let keys = self
            .row_converter
            .as_ref()
            .expect("row converter initialized above")
            .convert_columns(&key_columns)
            .map_err(row_sort_error)?;
        self.metrics.key_encode_us = self
            .metrics
            .key_encode_us
            .saturating_add(encode_started.elapsed().as_micros() as u64);

        let row_count = chunk.num_rows();
        let selection_started = Instant::now();
        let has_candidate = if top_n.is_full() {
            let worst = top_n
                .peek()
                .ok_or_else(|| VeinError::internal("full Top-N heap is empty"))?;
            (0..row_count).any(|row| keys.row(row).data() < worst.key.as_slice())
        } else {
            true
        };
        if !has_candidate {
            self.metrics.candidate_rows_examined = self
                .metrics
                .candidate_rows_examined
                .saturating_add(row_count as u64);
            self.metrics.selection_us = self
                .metrics
                .selection_us
                .saturating_add(selection_started.elapsed().as_micros() as u64);
            return Ok(());
        }

        let mut chunk = chunk;
        self.account_retained_arrays(&mut chunk)?;
        let chunk_index = self.chunks.len();
        let chunk_metadata = size_of::<Option<DataChunkView>>()
            .checked_add(size_of::<usize>())
            .ok_or_else(|| VeinError::internal("sort chunk metadata size overflow"))?;
        self.memory.grow(chunk_metadata, "sort chunk references")?;
        self.chunks.push(Some(chunk));
        self.top_n_references.push(0);
        let top_n = self.top_n.as_mut().expect("Top-N selector checked above");
        for active_row in 0..row_count {
            self.metrics.candidate_rows_examined =
                self.metrics.candidate_rows_examined.saturating_add(1);
            let key = keys.row(active_row).data();
            let candidate_row = RowRef {
                chunk: chunk_index,
                row: active_row,
            };
            let replaced_row = if top_n.is_full() {
                let Some(worst) = top_n.peek() else {
                    continue;
                };
                if key >= worst.key.as_slice() {
                    continue;
                }
                let old_key_len = worst.key.len();
                if key.len() > old_key_len {
                    self.memory
                        .grow(key.len() - old_key_len, "sort Top-N keys")?;
                }
                let (replaced_row, buffer_grew) = top_n
                    .replace_worst_with(|worst| {
                        let replaced_row = worst.row;
                        let capacity = worst.key.capacity();
                        worst.key.clear();
                        worst.key.extend_from_slice(key);
                        worst.row = candidate_row;
                        (replaced_row, worst.key.capacity() > capacity)
                    })
                    .ok_or_else(|| VeinError::internal("Top-N heap unexpectedly empty"))?;
                if buffer_grew {
                    self.metrics.candidate_key_buffer_allocations = self
                        .metrics
                        .candidate_key_buffer_allocations
                        .saturating_add(1);
                }
                Some(replaced_row)
            } else {
                let candidate_bytes = key
                    .len()
                    .checked_add(size_of::<TopNEntry>())
                    .ok_or_else(|| VeinError::internal("Top-N candidate size overflow"))?;
                self.memory.grow(candidate_bytes, "sort Top-N keys")?;
                self.metrics.candidate_key_buffer_allocations = self
                    .metrics
                    .candidate_key_buffer_allocations
                    .saturating_add(1);
                top_n.push(TopNEntry {
                    key: key.to_vec(),
                    row: candidate_row,
                });
                None
            };
            self.metrics.candidate_key_bytes_copied = self
                .metrics
                .candidate_key_bytes_copied
                .saturating_add(key.len() as u64);
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
                    self.chunks[replaced_row.chunk] = None;
                }
            }
        }
        self.metrics.selection_us = self
            .metrics
            .selection_us
            .saturating_add(selection_started.elapsed().as_micros() as u64);
        if self.top_n_references[chunk_index] == 0 {
            self.chunks[chunk_index] = None;
        }
        Ok(())
    }
}

fn row_sort_error(error: ArrowError) -> VeinError {
    VeinError::planning(format!("ORDER BY key cannot be sorted: {error}"), None)
}

struct SpillMergeCursor {
    reader: FileReader<Box<dyn SpillStream>>,
    view: DataChunkView,
    keys: Option<Arc<Rows>>,
    key_memory: MemoryReservation,
    key_bytes: usize,
    row: usize,
    snapshot: usize,
}

#[derive(Clone)]
struct SpillRowKey {
    keys: Arc<Rows>,
    row: usize,
}

impl PartialEq for SpillRowKey {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for SpillRowKey {}

impl PartialOrd for SpillRowKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SpillRowKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.keys.row(self.row).cmp(&other.keys.row(other.row))
    }
}

impl HeapSize for SpillRowKey {
    fn heap_size(&self) -> usize {
        size_of::<Self>()
    }
}

fn encode_spill_keys(
    view: &DataChunkView,
    plan: &SortPlan,
    converter: &RowConverter,
) -> Result<Rows> {
    let columns = plan
        .keys
        .iter()
        .map(|key| {
            view.materialize_column(key.column as usize).map_err(|_| {
                VeinError::planning(
                    format!("ORDER BY column {} is outside the spill schema", key.column),
                    None,
                )
            })
        })
        .collect::<Result<Vec<_>>>()?;
    converter.convert_columns(&columns).map_err(row_sort_error)
}

fn encoded_rows_size(rows: &Rows) -> Result<usize> {
    (0..rows.num_rows())
        .try_fold(0usize, |bytes, row| {
            bytes.checked_add(rows.row(row).data().len())
        })
        .ok_or_else(|| VeinError::internal("encoded spill key size overflow"))
}

fn view_from_spill_batch(
    batch: RecordBatch,
    tracker: &QueryMemoryTracker,
) -> Result<DataChunkView> {
    let mut view = DataChunkView::from_chunk(crate::pipeline::DataChunk::from_record_batch(batch))?;
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
    let fields = plan
        .keys
        .iter()
        .map(|key| {
            let data_type = first_view
                .column_data_type(key.column as usize)
                .map_err(|_| {
                    VeinError::planning(
                        format!("ORDER BY column {} is outside the spill schema", key.column),
                        None,
                    )
                })?;
            Ok(SortField::new_with_options(
                data_type.clone(),
                SortOptions {
                    descending: !key.ascending,
                    nulls_first: key.nulls_first,
                },
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    let converter = RowConverter::new(fields).map_err(row_sort_error)?;
    drop(first_view);

    let mut snapshots = Vec::<DataChunkView>::new();
    let mut cursors = Vec::with_capacity(readers.len());
    let mut heap = spill::KWayMerge::new(tracker)?;
    for (reader, view) in readers.into_iter().zip(initial) {
        let Some(view) = view else {
            continue;
        };
        let keys = Arc::new(encode_spill_keys(&view, plan, &converter)?);
        let key_bytes = encoded_rows_size(&keys)?;
        let key_memory = tracker.reserve(key_bytes, "sort merge encoded keys")?;
        let snapshot = snapshots.len();
        snapshots.push(view.clone());
        let row = 0;
        let cursor_index = cursors.len();
        cursors.push(SpillMergeCursor {
            reader,
            view,
            keys: Some(Arc::clone(&keys)),
            key_memory,
            key_bytes,
            row,
            snapshot,
        });
        heap.push(
            SpillRowKey { keys, row },
            (cursor_index, row),
            (cursor_index, row),
        )?;
    }

    let mut rows = Vec::<(usize, usize)>::with_capacity(OUTPUT_BATCH_ROWS);
    while let Some((run, row)) = heap.pop() {
        tracker.check_cancelled()?;
        let cursor = &mut cursors[run];
        rows.push((cursor.snapshot, row));
        cursor.row += 1;
        if cursor.row < cursor.view.num_rows() {
            let keys = Arc::clone(
                cursor
                    .keys
                    .as_ref()
                    .ok_or_else(|| VeinError::internal("sort merge cursor keys are missing"))?,
            );
            heap.push(
                SpillRowKey {
                    keys,
                    row: cursor.row,
                },
                (run, cursor.row),
                (run, cursor.row),
            )?;
        } else {
            cursor.key_memory.shrink(cursor.key_bytes);
            cursor.key_bytes = 0;
            cursor.keys = None;
            let batch = cursor.reader.next().transpose().map_err(|error| {
                VeinError::internal(format!("decode sort spill batch: {error}"))
            })?;
            if let Some(batch) = batch {
                cursor.view = view_from_spill_batch(batch, tracker)?;
                let keys = Arc::new(encode_spill_keys(&cursor.view, plan, &converter)?);
                let key_bytes = encoded_rows_size(&keys)?;
                cursor
                    .key_memory
                    .grow(key_bytes, "sort merge encoded keys")?;
                cursor.key_bytes = key_bytes;
                cursor.keys = Some(Arc::clone(&keys));
                cursor.row = 0;
                cursor.snapshot = snapshots.len();
                snapshots.push(cursor.view.clone());
                heap.push(SpillRowKey { keys, row: 0 }, (run, 0), (run, 0))?;
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
    Ok(())
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
            candidate_key_buffer_allocations = self.metrics.candidate_key_buffer_allocations,
            candidate_key_bytes_copied = self.metrics.candidate_key_bytes_copied,
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
