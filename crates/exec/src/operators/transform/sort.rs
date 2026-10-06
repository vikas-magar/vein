//! Blocking Arrow row sort that retains input column buffers until final gather.

use arrow::array::{Array, ArrayRef};
use arrow::compute::SortOptions;
use arrow::error::ArrowError;
use arrow_row::{RowConverter, Rows, SortField};
use common::arrow::SelectionVector;
use common::Result;
use common::protocol::{LimitSpec, SortPlan};
use error::VeinError;
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::sync::Arc;
use std::time::Instant;

use crate::operators::join::gather::gather;
use crate::pipeline::{DataChunk, PipelineNode, Transform};

const OUTPUT_BATCH_ROWS: usize = 8192;

/// A row's location in the retained input chunks.
#[derive(Clone, Copy)]
struct RowRef {
    chunk: usize,
    row: usize,
}

/// A candidate shares the encoded keys for its input batch; its payload stays in
/// the input Arrow chunk referenced by `row`.
struct TopNEntry {
    keys: Arc<Rows>,
    key_row: usize,
    row: RowRef,
}

impl PartialEq for TopNEntry {
    fn eq(&self, other: &Self) -> bool {
        self.keys.row(self.key_row) == other.keys.row(other.key_row)
            && self.row.chunk == other.row.chunk
            && self.row.row == other.row.row
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
        self.keys
            .row(self.key_row)
            .cmp(&other.keys.row(other.key_row))
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

    fn drain_sorted(&mut self) -> Vec<T> {
        std::mem::take(&mut self.entries).into_sorted_vec()
    }
}

/// Collects one task's chunks, sorts references to their rows, then gathers output.
///
/// Input arrays stay shared while the keys are encoded and sorted. The only full
/// column materialization is the final Arrow interleave into output batches.
pub struct SortTransform {
    plan: SortPlan,
    output_columns: Option<usize>,
    chunks: Vec<Option<DataChunk>>,
    top_n_references: Vec<usize>,
    top_n: Option<BoundedTopN<TopNEntry>>,
    row_converter: Option<RowConverter>,
    metrics: SortMetrics,
    next: Box<dyn PipelineNode<DataChunk>>,
}

#[derive(Default)]
struct SortMetrics {
    input_batches: u64,
    input_rows: u64,
    candidate_rows_examined: u64,
    candidate_insertions: u64,
    candidate_replacements: u64,
    candidate_key_bytes_copied: u64,
    output_rows: u64,
    key_encode_us: u64,
    selection_us: u64,
    sort_us: u64,
    emit_us: u64,
}

impl SortTransform {
    /// Create a global sort boundary for one task's input stream.
    pub fn new(plan: SortPlan, next: Box<dyn PipelineNode<DataChunk>>) -> Self {
        Self::with_output_columns(plan, None, next)
    }

    /// Create a global sort that drops temporary sort-key columns from output.
    pub fn with_output_columns(
        plan: SortPlan,
        output_columns: Option<usize>,
        next: Box<dyn PipelineNode<DataChunk>>,
    ) -> Self {
        Self::with_limit(plan, output_columns, None, next)
    }

    /// Create a global sort that retains at most `offset + count` candidates
    /// when a row window follows it.
    pub(crate) fn with_limit(
        plan: SortPlan,
        output_columns: Option<usize>,
        limit: Option<LimitSpec>,
        next: Box<dyn PipelineNode<DataChunk>>,
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

    fn sort_fields(&self, first: &DataChunk) -> Result<Vec<SortField>> {
        if self.plan.keys.is_empty() {
            return Err(VeinError::planning("ORDER requires at least one sort key", None));
        }

        self.plan
            .keys
            .iter()
            .map(|key| {
                let column = first.vectors.get(key.column as usize).ok_or_else(|| {
                    VeinError::planning(
                        format!("ORDER BY column {} is outside the input schema", key.column),
                        None,
                    )
                })?;
                Ok(SortField::new_with_options(
                    column.data_type().clone(),
                    SortOptions {
                        descending: !key.ascending,
                        nulls_first: key.nulls_first,
                    },
                ))
            })
            .collect()
    }

    fn sort_key_columns(&self, chunk: &DataChunk) -> Result<Vec<ArrayRef>> {
        self.plan
            .keys
            .iter()
            .map(|key| {
                chunk.vectors.get(key.column as usize).cloned().ok_or_else(|| {
                    VeinError::planning(
                        format!("ORDER BY column {} is outside the input schema", key.column),
                        None,
                    )
                })
            })
            .collect()
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
        let output_columns = self.output_columns.unwrap_or(first.vectors.len());
        if output_columns > first.vectors.len() {
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
            let key_columns = self.sort_key_columns(chunk)?;
            let keys = converter
                .convert_columns(&key_columns)
                .map_err(row_sort_error)?;
            for active_row in 0..chunk.selection.size() {
                order.push(RowRef {
                    chunk: chunk_index,
                    row: chunk.selection.get_index(active_row),
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
        let output_columns = self.output_columns.unwrap_or(first.vectors.len());
        if output_columns > first.vectors.len() {
            return Err(VeinError::planning(
                "ORDER output width exceeds the sort input schema",
                None,
            ));
        }

        if order.iter().all(|row| row.chunk == first_row.chunk) {
            let vectors: Vec<ArrayRef> = first
                .vectors
                .iter()
                .take(output_columns)
                .cloned()
                .collect();
            for rows in order.chunks(OUTPUT_BATCH_ROWS) {
                let selection = SelectionVector::new_sparse(
                    rows.iter().map(|row| row.row as u32).collect(),
                );
                self.next
                    .push(DataChunk::new(vectors.clone(), selection))?;
            }
            return Ok(());
        }

        let mut chunk_positions = vec![None; self.chunks.len()];
        let mut arrays = Vec::new();
        for (index, chunk) in self.chunks.iter().enumerate() {
            if let Some(chunk) = chunk {
                chunk_positions[index] = Some(arrays.len());
                arrays.push(chunk);
            }
        }
        let column_count = output_columns;
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
            let mut output = Vec::with_capacity(column_count);
            for column_index in 0..column_count {
                let values: Vec<&dyn Array> = arrays
                    .iter()
                    .map(|chunk| chunk.vectors[column_index].as_ref())
                    .collect();
                output.push(gather(&values, &indices)?);
            }
            self.next.push(DataChunk::from_vectors(output))?;
        }
        Ok(())
    }

    fn push_top_n(&mut self, chunk: DataChunk) -> Result<()> {
        if chunk.selection.is_empty() {
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
            .saturating_add(chunk.selection.size() as u64);

        if self.row_converter.is_none() {
            self.row_converter = Some(RowConverter::new(self.sort_fields(&chunk)?).map_err(row_sort_error)?);
        }
        let key_columns = self.sort_key_columns(&chunk)?;
        let encode_started = Instant::now();
        let keys = Arc::new(
            self
            .row_converter
            .as_ref()
            .expect("row converter initialized above")
            .convert_columns(&key_columns)
            .map_err(row_sort_error)?,
        );
        self.metrics.key_encode_us = self
            .metrics
            .key_encode_us
            .saturating_add(encode_started.elapsed().as_micros() as u64);

        let chunk_index = self.chunks.len();
        self.chunks.push(Some(chunk));
        self.top_n_references.push(0);
        let selection = self.chunks[chunk_index]
            .as_ref()
            .expect("new chunk is retained")
            .selection
            .clone();
        let top_n = self.top_n.as_mut().expect("Top-N selector checked above");
        let selection_started = Instant::now();
        for active_row in 0..selection.size() {
            self.metrics.candidate_rows_examined = self
                .metrics
                .candidate_rows_examined
                .saturating_add(1);
            let row = selection.get_index(active_row);
            let key = keys.row(row).data();
            let replaced_chunk = if top_n.is_full() {
                let Some(worst) = top_n.peek() else {
                    continue;
                };
                if key >= worst.keys.row(worst.key_row) {
                    continue;
                }
                Some(worst.row.chunk)
            } else {
                None
            };
            top_n.push(TopNEntry {
                keys: Arc::clone(&keys),
                key_row: row,
                row: RowRef {
                    chunk: chunk_index,
                    row,
                },
            });
            self.metrics.candidate_insertions =
                self.metrics.candidate_insertions.saturating_add(1);
            self.top_n_references[chunk_index] = self.top_n_references[chunk_index]
                .checked_add(1)
                .ok_or_else(|| VeinError::internal("Top-N candidate count overflow"))?;
            if let Some(replaced_chunk) = replaced_chunk {
                self.metrics.candidate_replacements = self
                    .metrics
                    .candidate_replacements
                    .saturating_add(1);
                self.top_n_references[replaced_chunk] = self.top_n_references[replaced_chunk]
                    .checked_sub(1)
                    .ok_or_else(|| VeinError::internal("Top-N candidate count underflow"))?;
                if self.top_n_references[replaced_chunk] == 0 {
                    self.chunks[replaced_chunk] = None;
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

impl PipelineNode<DataChunk> for SortTransform {
    fn push(&mut self, chunk: DataChunk) -> Result<()> {
        if self.top_n.is_some() {
            self.push_top_n(chunk)
        } else if !chunk.selection.is_empty() {
            self.metrics.input_batches = self.metrics.input_batches.saturating_add(1);
            self.metrics.input_rows = self
                .metrics
                .input_rows
                .saturating_add(chunk.selection.size() as u64);
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
            candidate_key_bytes_copied = self.metrics.candidate_key_bytes_copied,
            output_rows = self.metrics.output_rows,
            key_encode_us = self.metrics.key_encode_us,
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

impl Transform<DataChunk> for SortTransform {}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use arrow::array::{Int64Array, StringArray};
    use common::arrow::SelectionVector;
    use common::protocol::{LimitSpec, SortKey, SortPlan};

    use super::*;
    use crate::operators::transform::limit::LimitTransform;

    #[derive(Clone)]
    struct Collect(Arc<Mutex<Vec<DataChunk>>>);

    impl PipelineNode<DataChunk> for Collect {
        fn push(&mut self, chunk: DataChunk) -> Result<()> {
            self.0.lock().unwrap().push(chunk);
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
        sort.push(DataChunk::new(
            vec![strings(&[Some("b"), Some("a")]), integers(&[1, 1])],
            SelectionVector::new_dense(2),
        ))
            .unwrap();
        sort.push(DataChunk::new(
            vec![strings(&[Some("b"), None]), integers(&[3, 9])],
            SelectionVector::new_dense(2),
        ))
            .unwrap();

        sort.flush().unwrap();

        let chunks = output.lock().unwrap();
        let labels: Vec<Option<String>> = chunks
            .iter()
            .flat_map(|chunk| {
                let values = chunk.column(0).as_any().downcast_ref::<StringArray>().unwrap();
                (0..values.len())
                    .map(|row| values.is_valid(row).then(|| values.value(row).to_owned()))
                    .collect::<Vec<_>>()
            })
            .collect();
        let tie_breakers: Vec<i64> = chunks
            .iter()
            .flat_map(|chunk| {
                let values = chunk.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
                (0..values.len()).map(|row| values.value(row)).collect::<Vec<_>>()
            })
            .collect();

        assert_eq!(labels, vec![Some("a".into()), Some("b".into()), Some("b".into()), None]);
        assert_eq!(tie_breakers, vec![1, 3, 1, 9]);
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
        sort.push(DataChunk::from_vectors(vec![values.clone()]))
            .unwrap();

        let candidates: Vec<_> = sort
            .top_n
            .as_ref()
            .unwrap()
            .entries
            .iter()
            .collect();
        assert_eq!(candidates.len(), 3);
        assert!(candidates
            .iter()
            .all(|candidate| Arc::ptr_eq(&candidate.keys, &candidates[0].keys)));

        sort.flush().unwrap();

        let chunks = output.lock().unwrap();
        assert_eq!(chunks.len(), 1);
        assert!(Arc::ptr_eq(&chunks[0].vectors[0], &values));
        let selected: Vec<i64> = (0..chunks[0].selection.size())
            .map(|index| {
                let row = chunks[0].selection.get_index(index);
                values.as_any().downcast_ref::<Int64Array>().unwrap().value(row)
            })
            .collect();
        assert_eq!(selected, vec![1, 2, 3]);
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
        sort.push(DataChunk::from_vectors(vec![values.clone()]))
            .unwrap();

        sort.flush().unwrap();

        let chunks = output.lock().unwrap();
        assert_eq!(chunks.len(), 1);
        assert!(Arc::ptr_eq(&chunks[0].vectors[0], &values));
        let values = chunks[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let sorted: Vec<i64> = (0..chunks[0].selection.size())
            .map(|index| values.value(chunks[0].selection.get_index(index)))
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
        sort.push(DataChunk::from_vectors(vec![integers(&[8, 9])]))
            .unwrap();
        sort.push(DataChunk::from_vectors(vec![winner.clone()]))
            .unwrap();

        assert!(sort.chunks[0].is_none());
        assert!(sort.chunks[1].is_some());
        sort.flush().unwrap();

        let chunks = output.lock().unwrap();
        assert_eq!(chunks.len(), 1);
        assert!(Arc::ptr_eq(&chunks[0].vectors[0], &winner));
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
        sort.push(DataChunk::from_vectors(vec![integers(&[4, 1])]))
            .unwrap();
        sort.push(DataChunk::from_vectors(vec![integers(&[3, 2])]))
            .unwrap();

        sort.flush().unwrap();

        assert_eq!(sort.metrics.input_batches, 2);
        assert_eq!(sort.metrics.input_rows, 4);
        assert_eq!(sort.metrics.candidate_rows_examined, 4);
        assert_eq!(sort.metrics.candidate_insertions, 4);
        assert_eq!(sort.metrics.candidate_replacements, 1);
        assert_eq!(sort.metrics.candidate_key_bytes_copied, 0);
        assert_eq!(sort.metrics.output_rows, 3);

        let chunks = output.lock().unwrap();
        let sorted: Vec<i64> = chunks
            .iter()
            .flat_map(|chunk| {
                chunk
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values()
                    .iter()
                    .copied()
            })
            .collect();
        assert_eq!(sorted, vec![1, 2, 3]);
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
        sort.push(DataChunk::from_vectors(vec![integers(&[3, 1, 2])]))
            .unwrap();
        sort.flush().unwrap();

        assert!(sort.chunks.is_empty());
        assert!(output.lock().unwrap().is_empty());
    }
}
