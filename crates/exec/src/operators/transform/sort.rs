//! Blocking Arrow row sort that retains input column buffers until final gather.

use arrow::array::{Array, ArrayRef};
use arrow::compute::SortOptions;
use arrow::error::ArrowError;
use arrow_row::{RowConverter, Rows, SortField};
use common::arrow::SelectionVector;
use common::Result;
use common::protocol::SortPlan;
use error::VeinError;

use crate::operators::join::gather::gather;
use crate::pipeline::{DataChunk, PipelineNode, Transform};

const OUTPUT_BATCH_ROWS: usize = 8192;

/// A row's location in the retained input chunks.
#[derive(Clone, Copy)]
struct RowRef {
    chunk: usize,
    row: usize,
}

/// Collects one task's chunks, sorts references to their rows, then gathers output.
///
/// Input arrays stay shared while the keys are encoded and sorted. The only full
/// column materialization is the final Arrow interleave into output batches.
pub struct SortTransform {
    plan: SortPlan,
    output_columns: Option<usize>,
    chunks: Vec<DataChunk>,
    next: Box<dyn PipelineNode<DataChunk>>,
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
        Self {
            plan,
            output_columns,
            chunks: Vec::new(),
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

    fn sort_and_emit(&mut self) -> Result<()> {
        self.chunks.retain(|chunk| !chunk.selection.is_empty());
        let Some(first) = self.chunks.first() else {
            return Ok(());
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

        for (chunk_index, chunk) in self.chunks.iter().enumerate() {
            let key_columns = self
                .plan
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
                .collect::<Result<Vec<ArrayRef>>>()?;
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

        order.sort_unstable_by(|left, right| {
            encoded_keys[left.chunk]
                .row(left.row)
                .cmp(&encoded_keys[right.chunk].row(right.row))
        });

        if let [chunk] = self.chunks.as_slice() {
            let vectors: Vec<ArrayRef> = chunk
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

        let column_count = output_columns;
        for rows in order.chunks(OUTPUT_BATCH_ROWS) {
            let indices: Vec<(usize, usize)> = rows
                .iter()
                .map(|row| (row.chunk, row.row))
                .collect();
            let mut output = Vec::with_capacity(column_count);
            for column_index in 0..column_count {
                let values: Vec<&dyn Array> = self
                    .chunks
                    .iter()
                    .map(|chunk| chunk.vectors[column_index].as_ref())
                    .collect();
                output.push(gather(&values, &indices)?);
            }
            self.next.push(DataChunk::from_vectors(output))?;
        }
        Ok(())
    }
}

fn row_sort_error(error: ArrowError) -> VeinError {
    VeinError::planning(format!("ORDER BY key cannot be sorted: {error}"), None)
}

impl PipelineNode<DataChunk> for SortTransform {
    fn push(&mut self, chunk: DataChunk) -> Result<()> {
        self.chunks.push(chunk);
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        self.sort_and_emit()?;
        self.chunks.clear();
        self.next.flush()
    }
}

impl Transform<DataChunk> for SortTransform {}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use arrow::array::{Int64Array, StringArray};
    use common::arrow::SelectionVector;
    use common::protocol::{SortKey, SortPlan};

    use super::*;

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
}
