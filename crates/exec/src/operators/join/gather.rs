//! Turning a join's output rows into columns.
//!
//! This is the only place a join materialises anything: an output row has to
//! exist somewhere. Everything else is references — the probe chunk's columns
//! and the table's retained build chunks are read, never copied, and a side that
//! is absent for a row becomes one typed null array rather than a placeholder.
//!
//! One **output chunk per call**, not one per (probe chunk × build chunk) group.
//! `interleave` reads several source arrays in a single pass, so a probe chunk
//! that matched rows in three build chunks still produces one chunk. Emitting a
//! chunk per group instead shattered the output — 113 batches for 45 input
//! chunks — and the per-chunk overhead, not the work, dominated the join.

use arrow::array::{Array, ArrayRef, new_null_array};
use arrow::compute::interleave;
use arrow::datatypes::DataType;

use crate::pipeline::DataChunk;
use common::Result;
use error::VeinError;

use super::hash_table::HashTable;
use super::rows::OutputRow;

/// Where one output column comes from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputColumn {
    /// A column of the probe chunk.
    Probe(usize),
    /// A column of the build side, present in every retained build chunk.
    Build(usize),
}

/// Assembles `rows` into one output chunk, in the column order `columns` gives.
///
/// `probe` may be absent: the unmatched build rows a right or full join emits at
/// flush have no probe chunk. For that reason both `probe_types` and
/// `build_types` are supplied by the caller — a joined null column needs a type,
/// and neither side can always provide one from data.
pub fn assemble(
    probe: Option<&DataChunk>,
    table: &HashTable,
    rows: &[OutputRow],
    columns: &[OutputColumn],
    probe_types: &[DataType],
    build_types: &[DataType],
) -> Result<Vec<DataChunk>> {
    if rows.is_empty() {
        return Ok(Vec::new());
    }

    let mut arrays = Vec::with_capacity(columns.len());
    for column in columns {
        arrays.push(match column {
            OutputColumn::Probe(index) => probe_column(probe, *index, rows, probe_types)?,
            OutputColumn::Build(index) => build_column(table, *index, rows, build_types)?,
        });
    }
    Ok(vec![DataChunk::new(
        arrays,
        common::arrow::SelectionVector::new_dense(rows.len()),
    )])
}

/// Gathers one column from the probe chunk, or from a typed null when a row has
/// no probe side.
fn probe_column(
    probe: Option<&DataChunk>,
    index: usize,
    rows: &[OutputRow],
    probe_types: &[DataType],
) -> Result<ArrayRef> {
    let absent = rows.iter().any(|row| row.probe.is_none());
    match probe {
        Some(chunk) => {
            let source = chunk.column(index);
            let null = absent.then(|| new_null_array(source.data_type(), 1));
            let mut values: Vec<&dyn Array> = vec![source.as_ref()];
            if let Some(null) = &null {
                values.push(null.as_ref());
            }
            let indices: Vec<(usize, usize)> = rows
                .iter()
                .map(|row| match row.probe {
                    Some(row) => (0, row as usize),
                    None => (1, 0),
                })
                .collect();
            gather(&values, &indices)
        }
        // A right or full join emits its unmatched build rows at flush, when
        // there is no probe chunk left to take a type from.
        None if absent => {
            let data_type = probe_types.get(index).ok_or_else(|| {
                VeinError::internal(format!(
                    "join output needs the data type of probe column {index}"
                ))
            })?;
            let null = new_null_array(data_type, 1);
            let values: Vec<&dyn Array> = vec![null.as_ref()];
            let indices: Vec<(usize, usize)> = rows.iter().map(|_| (0, 0)).collect();
            gather(&values, &indices)
        }
        None => Err(VeinError::internal(
            "join output has probe rows but no probe chunk",
        )),
    }
}

/// Gathers one column across every retained build chunk, or from a typed null
/// when a row has no build side.
fn build_column(
    table: &HashTable,
    index: usize,
    rows: &[OutputRow],
    build_types: &[DataType],
) -> Result<ArrayRef> {
    let chunk_count = table.chunk_count();
    let chunks: Vec<ArrayRef> = (0..chunk_count)
        .map(|chunk| std::sync::Arc::clone(table.build_chunk(chunk as u32).column(index)))
        .collect();

    let null = if rows.iter().any(|row| row.build.is_none()) {
        let data_type = chunks
            .first()
            .map(|chunk| chunk.data_type().clone())
            .or_else(|| build_types.get(index).cloned())
            .ok_or_else(|| {
                VeinError::internal(format!(
                    "join output needs the data type of build column {index}"
                ))
            })?;
        Some(new_null_array(&data_type, 1))
    } else {
        None
    };

    let mut values: Vec<&dyn Array> = chunks.iter().map(|chunk| chunk.as_ref()).collect();
    if let Some(null) = &null {
        values.push(null.as_ref());
    }
    let indices: Vec<(usize, usize)> = rows
        .iter()
        .map(|row| match row.build {
            Some((chunk, row)) => (chunk as usize, row as usize),
            None => (chunk_count, 0),
        })
        .collect();
    gather(&values, &indices)
}

pub(crate) fn gather(values: &[&dyn Array], indices: &[(usize, usize)]) -> Result<ArrayRef> {
    interleave(values, indices)
        .map_err(|error| VeinError::internal(format!("join gather failed: {error}")))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Array, ArrayRef, Int64Array};

    use super::super::rows::{JoinType, output_rows};
    use super::*;
    use crate::operators::join::JoinKeys;

    fn ints(values: &[i64]) -> ArrayRef {
        Arc::new(Int64Array::from(values.to_vec()))
    }

    /// A table whose single build chunk holds `values`, plus a probe chunk.
    fn fixture(build: &[i64], probe: &[i64]) -> (HashTable, DataChunk) {
        let mut table = HashTable::new(JoinKeys::new(vec![0]), JoinKeys::new(vec![0]));
        table
            .insert(DataChunk::from_vectors(vec![ints(build)]))
            .expect("insert");
        (table, DataChunk::from_vectors(vec![ints(probe)]))
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

    /// One chunk comes out whatever the grouping, with both sides gathered.
    #[test]
    fn one_chunk_holds_every_row() {
        // probe [10, 20]; build [20, 30]; probe row 1 matches build row 0.
        let (table, probe) = fixture(&[20, 30], &[10, 20]);
        let rows = output_rows(JoinType::Inner, &[0, 1], &[(1, (0, 0))], &[]);

        let chunks = assemble(
            Some(&probe),
            &table,
            &rows,
            &[OutputColumn::Probe(0), OutputColumn::Build(0)],
            &[DataType::Int64],
            &[DataType::Int64],
        )
            .expect("assemble");

        assert_eq!(chunks.len(), 1, "a probe chunk yields one output chunk");
        assert_eq!(cells(&chunks[0], 0), vec![Some(20)]);
        assert_eq!(cells(&chunks[0], 1), vec![Some(20)]);
    }

    #[test]
    fn zero_column_join_output_retains_joined_row_count() {
        let (table, probe) = fixture(&[20], &[20, 30]);
        let rows = output_rows(JoinType::Inner, &[0], &[(0, (0, 0))], &[]);

        let chunks = assemble(
            Some(&probe),
            &table,
            &rows,
            &[],
            &[DataType::Int64],
            &[DataType::Int64],
        )
            .expect("assemble zero-column join output");

        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].num_columns(), 0);
        assert_eq!(chunks[0].num_rows(), 1);
    }

    /// A probe chunk matching several build chunks still emits one chunk.
    #[test]
    fn matches_across_build_chunks_stay_in_one_chunk() {
        let mut table = HashTable::new(JoinKeys::new(vec![0]), JoinKeys::new(vec![0]));
        table
            .insert(DataChunk::from_vectors(vec![ints(&[20])]))
            .expect("insert");
        table
            .insert(DataChunk::from_vectors(vec![ints(&[20])]))
            .expect("insert");
        let probe = DataChunk::from_vectors(vec![ints(&[20])]);

        let rows = output_rows(JoinType::Inner, &[0], &[(0, (0, 0)), (0, (1, 0))], &[]);
        let chunks = assemble(
            Some(&probe),
            &table,
            &rows,
            &[OutputColumn::Probe(0), OutputColumn::Build(0)],
            &[DataType::Int64],
            &[DataType::Int64],
        )
            .expect("assemble");

        assert_eq!(chunks.len(), 1);
        assert_eq!(cells(&chunks[0], 1), vec![Some(20), Some(20)]);
    }

    /// Left: the unmatched probe row is emitted with a null build column.
    #[test]
    fn left_nulls_the_build_side_for_unmatched_probe_rows() {
        let (table, probe) = fixture(&[20], &[10, 20]);
        let rows = output_rows(JoinType::Left, &[0, 1], &[(1, (0, 0))], &[]);

        let chunks = assemble(
            Some(&probe),
            &table,
            &rows,
            &[OutputColumn::Probe(0), OutputColumn::Build(0)],
            &[DataType::Int64],
            &[DataType::Int64],
        )
            .expect("assemble");

        assert_eq!(chunks.len(), 1);
        assert_eq!(cells(&chunks[0], 0), vec![Some(20), Some(10)]);
        assert_eq!(cells(&chunks[0], 1), vec![Some(20), None]);
    }

    /// Right: an unmatched build row is emitted with a null probe column.
    #[test]
    fn right_nulls_the_probe_side_for_unmatched_build_rows() {
        let (table, probe) = fixture(&[20, 40], &[20]);
        let rows = output_rows(JoinType::Right, &[0], &[(0, (0, 0))], &[(0, 1)]);

        let chunks = assemble(
            Some(&probe),
            &table,
            &rows,
            &[OutputColumn::Probe(0), OutputColumn::Build(0)],
            &[DataType::Int64],
            &[DataType::Int64],
        )
            .expect("assemble");

        assert_eq!(chunks.len(), 1);
        assert_eq!(cells(&chunks[0], 0), vec![Some(20), None]);
        assert_eq!(cells(&chunks[0], 1), vec![Some(20), Some(40)]);
    }

    /// With nothing built there is no chunk to take a type from, so the caller's
    /// type is required rather than guessed.
    #[test]
    fn an_empty_build_side_needs_the_caller_type() {
        let table = HashTable::new(JoinKeys::new(vec![0]), JoinKeys::new(vec![0]));
        let probe = DataChunk::from_vectors(vec![ints(&[10])]);
        let rows = output_rows(JoinType::Left, &[0], &[], &[]);

        let error = assemble(
            Some(&probe),
            &table,
            &rows,
            &[OutputColumn::Probe(0), OutputColumn::Build(0)],
            &[DataType::Int64],
            &[],
        )
            .expect_err("the build type is required for a null build column");
        assert!(error.to_string().contains("build column 0"), "{error}");
    }
}
