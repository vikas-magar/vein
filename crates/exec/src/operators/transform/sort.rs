//! Blocking Arrow row sort that retains input column buffers until final gather.

use arrow::array::ArrayRef;
use arrow::compute::SortOptions;
use arrow::error::ArrowError;
use arrow_row::{RowConverter, Rows, SortField};
use common::Result;
use common::protocol::{LimitSpec, SortPlan};
use error::VeinError;
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::time::Instant;

use crate::pipeline::{DataChunkView, MaterializationReport, PipelineNode, Transform};

const OUTPUT_BATCH_ROWS: usize = 8192;

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
        let candidate_limit = limit.map(|limit| {
            if limit.count == 0 {
                0
            } else {
                usize::try_from(limit.offset.saturating_add(limit.count)).unwrap_or(usize::MAX)
            }
        });
        Self {
            plan,
            output_columns,
            chunks: Vec::new(),
            top_n_references: Vec::new(),
            top_n: candidate_limit.map(BoundedTopN::new),
            row_converter: None,
            metrics: SortMetrics::default(),
            next,
        }
    }

    fn sort_fields(&self, first: &DataChunkView) -> Result<Vec<SortField>> {
        if self.plan.keys.is_empty() {
            return Err(VeinError::planning("ORDER requires at least one sort key", None));
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

    fn sort_key_columns(&self, chunk: &DataChunkView) -> Result<(Vec<ArrayRef>, MaterializationReport, u64)> {
        let started = Instant::now();
        let mut report = MaterializationReport::default();
        let columns = self.plan
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
        Ok((columns, report, u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX)))
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
            self.metrics.key_materialize_us = self.metrics.key_materialize_us.saturating_add(materialize_us);
            let keys = converter
                .convert_columns(&key_columns)
                .map_err(row_sort_error)?;
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
        let first = self.chunks[first_row.chunk]
            .as_ref()
            .ok_or_else(|| VeinError::internal("sort candidate references a released input chunk"))?;
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
            self.row_converter = Some(RowConverter::new(self.sort_fields(&chunk)?).map_err(row_sort_error)?);
        }
        let (key_columns, report, materialize_us) = self.sort_key_columns(&chunk)?;
        self.metrics.key_materialization.accumulate(report);
        self.metrics.key_materialize_us = self.metrics.key_materialize_us.saturating_add(materialize_us);
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

        let chunk_index = self.chunks.len();
        self.chunks.push(Some(chunk));
        self.top_n_references.push(0);
        let row_count = self.chunks[chunk_index]
            .as_ref()
            .expect("new view is retained")
            .num_rows();
        let top_n = self.top_n.as_mut().expect("Top-N selector checked above");
        let selection_started = Instant::now();
        for active_row in 0..row_count {
            self.metrics.candidate_rows_examined = self
                .metrics
                .candidate_rows_examined
                .saturating_add(1);
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
            self.metrics.candidate_insertions =
                self.metrics.candidate_insertions.saturating_add(1);
            self.top_n_references[chunk_index] = self.top_n_references[chunk_index]
                .checked_add(1)
                .ok_or_else(|| VeinError::internal("Top-N candidate count overflow"))?;
            if let Some(replaced_row) = replaced_row {
                self.metrics.candidate_replacements = self
                    .metrics
                    .candidate_replacements
                    .saturating_add(1);
                self.top_n_references[replaced_row.chunk] = self.top_n_references[replaced_row.chunk]
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

impl PipelineNode<DataChunkView> for SortTransform {
    fn push(&mut self, chunk: DataChunkView) -> Result<()> {
        if self.top_n.is_some() {
            self.push_top_n(chunk)
        } else if !chunk.is_empty() {
            self.metrics.input_batches = self.metrics.input_batches.saturating_add(1);
            self.metrics.input_rows = self
                .metrics
                .input_rows
                .saturating_add(chunk.num_rows() as u64);
            self.chunks.push(Some(chunk));
            Ok(())
        } else {
            Ok(())
        }
    }

    fn flush(&mut self) -> Result<()> {
        self.sort_and_emit()?;
        tracing::info!(
            target: "vein.audit",
            event = "sort_finished",
            mode = if self.top_n.is_some() { "top_n" } else { "full_sort" },
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
            "sort_finished"
        );
        self.chunks.clear();
        self.top_n_references.clear();
        self.next.flush()
    }

    fn finished(&self) -> bool {
        self.top_n
            .as_ref()
            .is_some_and(|top_n| top_n.capacity == 0)
    }
}

impl Transform<DataChunkView> for SortTransform {}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use arrow::array::{Array, Int64Array, StringArray};
    use common::arrow::SelectionVector;
    use common::protocol::{LimitSpec, SortKey, SortPlan};

    use super::*;
    use crate::operators::transform::limit::LimitTransform;
    use crate::pipeline::DataChunk;

    #[derive(Clone)]
    struct Collect(Arc<Mutex<Vec<DataChunkView>>>);

    impl PipelineNode<DataChunkView> for Collect {
        fn push(&mut self, view: DataChunkView) -> Result<()> {
            self.0.lock().unwrap().push(view);
            Ok(())
        }

        fn flush(&mut self) -> Result<()> {
            Ok(())
        }
    }

    fn strings(values: &[Option<&str>]) -> ArrayRef {
        Arc::new(StringArray::from(values.to_vec()))
    }

    fn integers(values: &[i64]) -> ArrayRef {
        Arc::new(Int64Array::from(values.to_vec()))
    }

    fn view(chunk: DataChunk) -> DataChunkView {
        DataChunkView::from_chunk(chunk).expect("materialized fixture becomes a view")
    }

    fn int_at(view: &DataChunkView, column: usize, row: usize) -> Option<i64> {
        let Some((array, physical_row)) = view.resolve(column, row).expect("resolve view cell") else {
            return None;
        };
        let values = array
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("int64 view column");
        values.is_valid(physical_row).then(|| values.value(physical_row))
    }

    fn string_at(view: &DataChunkView, column: usize, row: usize) -> Option<String> {
        let Some((array, physical_row)) = view.resolve(column, row).expect("resolve view cell") else {
            return None;
        };
        let values = array
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("string view column");
        values
            .is_valid(physical_row)
            .then(|| values.value(physical_row).to_owned())
    }

    #[test]
    fn globally_sorts_multiple_chunks_by_keys_and_null_order() {
        let output = Arc::new(Mutex::new(Vec::new()));
        let plan = SortPlan {
            keys: vec![
                SortKey {
                    column: 0,
                    ascending: true,
                    nulls_first: false,
                },
                SortKey {
                    column: 1,
                    ascending: false,
                    nulls_first: false,
                },
            ],
        };
        let mut sort = SortTransform::new(plan, Box::new(Collect(output.clone())));
        sort.push(view(DataChunk::new(
            vec![strings(&[Some("b"), Some("a")]), integers(&[1, 1])],
            SelectionVector::new_dense(2),
        )))
            .unwrap();
        sort.push(view(DataChunk::new(
            vec![strings(&[Some("b"), None]), integers(&[3, 9])],
            SelectionVector::new_dense(2),
        )))
            .unwrap();

        sort.flush().unwrap();

        let chunks = output.lock().unwrap();
        let labels: Vec<Option<String>> = chunks
            .iter()
            .flat_map(|view| (0..view.num_rows()).map(|row| string_at(view, 0, row)))
            .collect();
        let tie_breakers: Vec<i64> = chunks
            .iter()
            .flat_map(|view| (0..view.num_rows()).map(|row| int_at(view, 1, row).unwrap()))
            .collect();

        assert_eq!(labels, vec![Some("a".into()), Some("b".into()), Some("b".into()), None]);
        assert_eq!(tie_breakers, vec![1, 3, 1, 9]);
    }

    #[test]
    fn sort_composes_cross_batch_payload_references_with_sparse_selections() {
        let output = Arc::new(Mutex::new(Vec::new()));
        let first_keys = integers(&[4, 999, 1]);
        let first_payload = integers(&[40, 9_990, 10]);
        let second_keys = integers(&[3, 2]);
        let second_payload = integers(&[30, 20]);
        let mut sort = SortTransform::new(
            SortPlan {
                keys: vec![SortKey {
                    column: 0,
                    ascending: true,
                    nulls_first: false,
                }],
            },
            Box::new(Collect(output.clone())),
        );
        sort.push(view(DataChunk::new(
            vec![Arc::clone(&first_keys), Arc::clone(&first_payload)],
            SelectionVector::new_sparse(vec![0, 2]),
        )))
            .expect("push first sparse input view");
        sort.push(view(DataChunk::new(
            vec![Arc::clone(&second_keys), Arc::clone(&second_payload)],
            SelectionVector::new_dense(2),
        )))
            .expect("push second input view");

        sort.flush().expect("sort sparse input views");

        let output = output.lock().unwrap();
        let sorted_keys = output
            .iter()
            .flat_map(|view| (0..view.num_rows()).map(|row| int_at(view, 0, row).unwrap()))
            .collect::<Vec<_>>();
        let sorted_payload = output
            .iter()
            .flat_map(|view| (0..view.num_rows()).map(|row| int_at(view, 1, row).unwrap()))
            .collect::<Vec<_>>();
        assert_eq!(sorted_keys, vec![1, 2, 3, 4]);
        assert_eq!(sorted_payload, vec![10, 20, 30, 40]);
        for view in output.iter() {
            for row in 0..view.num_rows() {
                let (array, _) = view.resolve(1, row).unwrap().unwrap();
                assert!(
                    Arc::ptr_eq(array, &first_payload)
                        || Arc::ptr_eq(array, &second_payload),
                    "sorted payload must still reference an input array"
                );
            }
        }
    }

    #[test]
    fn single_chunk_sort_reuses_arrow_columns_and_reorders_selection() {
        let output = Arc::new(Mutex::new(Vec::new()));
        let values = integers(&[3, 1, 2]);
        let mut sort = SortTransform::new(
            SortPlan {
                keys: vec![SortKey {
                    column: 0,
                    ascending: true,
                    nulls_first: false,
                }],
            },
            Box::new(Collect(output.clone())),
        );
        sort.push(view(DataChunk::from_vectors(vec![values.clone()])))
            .unwrap();

        sort.flush().unwrap();

        let chunks = output.lock().unwrap();
        assert_eq!(chunks.len(), 1);
        let (array, _) = chunks[0].resolve(0, 0).unwrap().unwrap();
        assert!(Arc::ptr_eq(array, &values));
        let selected: Vec<i64> = (0..chunks[0].num_rows())
            .map(|row| int_at(&chunks[0], 0, row).unwrap())
            .collect();
        assert_eq!(selected, vec![1, 2, 3]);
    }

    #[test]
    fn full_sort_and_top_n_report_only_sort_key_conversions() {
        for top_n in [false, true] {
            let output = Arc::new(Mutex::new(Vec::new()));
            let keys = integers(&[30, 10, 20]);
            let payload = integers(&[300, 100, 200]);
            let mut sort = SortTransform::with_limit(
                SortPlan { keys: vec![SortKey { column: 0, ascending: true, nulls_first: false }] },
                None,
                top_n.then_some(LimitSpec { offset: 0, count: 2 }),
                Box::new(Collect(Arc::clone(&output))),
            );
            for selection in [SelectionVector::new_dense(3), SelectionVector::new_sparse(vec![2, 0])] {
                sort.push(view(DataChunk::new(vec![Arc::clone(&keys), Arc::clone(&payload)], selection))).unwrap();
            }
            sort.flush().unwrap();
            assert_eq!(sort.metrics.key_materialization.shared_columns, 1);
            assert_eq!(sort.metrics.key_materialization.gathered_columns, 1);
            assert_eq!(sort.metrics.key_materialization.empty_columns, 0);
            assert!(sort.metrics.key_materialization.gathered_output_buffer_bytes > 0);
            let views = output.lock().unwrap();
            let rows: Vec<_> = views.iter().flat_map(|view| (0..view.num_rows()).map(|row| {
                assert!(Arc::ptr_eq(view.resolve(1, row).unwrap().unwrap().0, &payload));
                (int_at(view, 0, row).unwrap(), int_at(view, 1, row).unwrap())
            })).collect();
            let expected = if top_n { vec![(10, 100), (20, 200)] } else {
                vec![(10, 100), (20, 200), (20, 200), (30, 300), (30, 300)]
            };
            assert_eq!(rows, expected);
        }
    }

    #[test]
    fn top_n_sorts_only_offset_plus_count_and_keeps_single_chunk_payload_shared() {
        let output = Arc::new(Mutex::new(Vec::new()));
        let values = integers(&[4, 1, 3, 2]);
        let sink = Box::new(Collect(output.clone()));
        let window = Box::new(LimitTransform::new(1, 2, sink));
        let mut sort = SortTransform::with_limit(
            SortPlan {
                keys: vec![SortKey {
                    column: 0,
                    ascending: true,
                    nulls_first: false,
                }],
            },
            None,
            Some(LimitSpec { offset: 1, count: 2 }),
            window,
        );
        sort.push(view(DataChunk::from_vectors(vec![values.clone()])))
            .unwrap();

        sort.flush().unwrap();

        let chunks = output.lock().unwrap();
        assert_eq!(chunks.len(), 1);
        let (array, _) = chunks[0].resolve(0, 0).unwrap().unwrap();
        assert!(Arc::ptr_eq(array, &values));
        let sorted: Vec<i64> = (0..chunks[0].num_rows())
            .map(|row| int_at(&chunks[0], 0, row).unwrap())
            .collect();
        assert_eq!(sorted, vec![2, 3]);
    }

    #[test]
    fn top_n_releases_chunks_without_current_winning_rows() {
        let output = Arc::new(Mutex::new(Vec::new()));
        let winner = integers(&[1]);
        let mut sort = SortTransform::with_limit(
            SortPlan {
                keys: vec![SortKey {
                    column: 0,
                    ascending: true,
                    nulls_first: false,
                }],
            },
            None,
            Some(LimitSpec { offset: 0, count: 1 }),
            Box::new(Collect(output.clone())),
        );
        sort.push(view(DataChunk::from_vectors(vec![integers(&[8, 9])])))
            .unwrap();
        sort.push(view(DataChunk::from_vectors(vec![winner.clone()])))
            .unwrap();

        assert!(sort.chunks[0].is_none());
        assert!(sort.chunks[1].is_some());
        sort.flush().unwrap();

        let chunks = output.lock().unwrap();
        assert_eq!(chunks.len(), 1);
        let (array, _) = chunks[0].resolve(0, 0).unwrap().unwrap();
        assert!(Arc::ptr_eq(array, &winner));
    }

    #[test]
    fn top_n_gathers_sorted_winners_from_multiple_chunks() {
        let output = Arc::new(Mutex::new(Vec::new()));
        let mut sort = SortTransform::with_limit(
            SortPlan {
                keys: vec![SortKey {
                    column: 0,
                    ascending: true,
                    nulls_first: false,
                }],
            },
            None,
            Some(LimitSpec { offset: 0, count: 3 }),
            Box::new(Collect(output.clone())),
        );
        let first = integers(&[4, 1]);
        let second = integers(&[3, 2]);
        sort.push(view(DataChunk::from_vectors(vec![first.clone()])))
            .unwrap();
        let evicted_key_buffer = sort
            .top_n
            .as_ref()
            .unwrap()
            .peek()
            .unwrap()
            .key
            .as_ptr();
        sort.push(view(DataChunk::from_vectors(vec![second.clone()])))
            .unwrap();

        let replacement = sort
            .top_n
            .as_ref()
            .unwrap()
            .entries
            .iter()
            .find(|entry| entry.row.chunk == 1 && entry.row.row == 1)
            .expect("second chunk's better candidate remains in the heap");
        assert_eq!(replacement.key.as_ptr(), evicted_key_buffer);

        sort.flush().unwrap();

        assert_eq!(sort.metrics.input_batches, 2);
        assert_eq!(sort.metrics.input_rows, 4);
        assert_eq!(sort.metrics.candidate_rows_examined, 4);
        assert_eq!(sort.metrics.candidate_insertions, 4);
        assert_eq!(sort.metrics.candidate_replacements, 1);
        assert_eq!(sort.metrics.candidate_key_buffer_allocations, 3);
        assert!(sort.metrics.candidate_key_bytes_copied > 0);
        assert_eq!(sort.metrics.output_rows, 3);

        let chunks = output.lock().unwrap();
        let sorted: Vec<i64> = chunks
            .iter()
            .flat_map(|view| (0..view.num_rows()).map(|row| int_at(view, 0, row).unwrap()))
            .collect();
        assert_eq!(sorted, vec![1, 2, 3]);
        for output in chunks.iter() {
            for row in 0..output.num_rows() {
                let (array, _) = output.resolve(0, row).unwrap().unwrap();
                assert!(Arc::ptr_eq(array, &first) || Arc::ptr_eq(array, &second));
            }
        }
    }

    #[test]
    fn zero_count_top_n_finishes_without_retaining_input() {
        let output = Arc::new(Mutex::new(Vec::new()));
        let mut sort = SortTransform::with_limit(
            SortPlan {
                keys: vec![SortKey {
                    column: 0,
                    ascending: true,
                    nulls_first: false,
                }],
            },
            None,
            Some(LimitSpec { offset: 10, count: 0 }),
            Box::new(Collect(output.clone())),
        );

        assert!(sort.finished());
        sort.push(view(DataChunk::from_vectors(vec![integers(&[3, 1, 2])])))
            .unwrap();
        sort.flush().unwrap();

        assert!(sort.chunks.is_empty());
        assert!(output.lock().unwrap().is_empty());
    }
}
