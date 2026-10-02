//! Turning a join's output rows into columns.
//!
//! This is the only place a join materialises anything: an output row has to
//! exist somewhere. Everything else is references — the probe chunk's columns
//! and the table's retained build chunks are read, never copied, and a column
//! that is null for every row of a group becomes one typed null array rather
//! than a buffer of placeholders.
//!
//! Rows are grouped by the build chunk they reference, so each `take` reads
//! exactly one source array, and by whether the probe side is present, so `take`
//! is never handed a null index.

use std::collections::BTreeMap;

use arrow::array::{ArrayRef, UInt32Array, new_null_array};
use arrow::compute::take;
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

/// Assembles `rows` into output chunks, in the column order `columns` gives.
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
    // Key: the build chunk this row reads from (`None` = no build side), and
    // whether the probe side is present.
    let mut groups: BTreeMap<(Option<u32>, bool), (Vec<u32>, Vec<u32>)> = BTreeMap::new();
    for row in rows {
        let key = (row.build.map(|(chunk, _)| chunk), row.probe.is_some());
        let (probe_rows, build_rows) = groups.entry(key).or_default();
        if let Some(probe_row) = row.probe {
            probe_rows.push(probe_row);
        }
        if let Some((_, build_row)) = row.build {
            build_rows.push(build_row);
        }
    }

    let mut chunks = Vec::with_capacity(groups.len());
    for ((build_chunk, probe_present), (probe_rows, build_rows)) in groups {
        let len = if probe_present {
            probe_rows.len()
        } else {
            build_rows.len()
        };

        let mut arrays = Vec::with_capacity(columns.len());
        for column in columns {
            arrays.push(match column {
                OutputColumn::Probe(index) => {
                    if probe_present {
                        let chunk = probe.ok_or_else(|| {
                            VeinError::internal(
                                "join output has probe rows but no probe chunk",
                            )
                        })?;
                        gather(chunk.column(*index), &probe_rows)?
                    } else {
                        let data_type = probe_types.get(*index).ok_or_else(|| {
                            VeinError::internal(format!(
                                "join output needs the data type of probe column {index}"
                            ))
                        })?;
                        new_null_array(data_type, len)
                    }
                }
                OutputColumn::Build(index) => match build_chunk {
                    Some(chunk) => {
                        gather(table.build_chunk(chunk).column(*index), &build_rows)?
                    }
                    None => {
                        let data_type = build_types.get(*index).ok_or_else(|| {
                            VeinError::internal(format!(
                                "join output needs the data type of build column {index}"
                            ))
                        })?;
                        new_null_array(data_type, len)
                    }
                },
            });
        }
        chunks.push(DataChunk::from_vectors(arrays));
    }
    Ok(chunks)
}

/// Reads `rows` out of `array`.
///
/// The indices are never null: a side that is absent for a group is materialised
/// as a typed null array instead, which is why this can rely on `take`'s plain
/// behaviour and keep the source array's data type.
fn gather(array: &ArrayRef, rows: &[u32]) -> Result<ArrayRef> {
    let indices = UInt32Array::from(rows.to_vec());
    take(array.as_ref(), &indices, None)
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

    /// Inner: one output row per match, both sides gathered from their source.
    #[test]
    fn inner_gathers_both_sides() {
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

        assert_eq!(chunks.len(), 1);
        assert_eq!(cells(&chunks[0], 0), vec![Some(20)]);
        assert_eq!(cells(&chunks[0], 1), vec![Some(20)]);
    }

    /// Left: the unmatched probe row is emitted with a typed null build column.
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

        // Two groups: the match, and the unmatched probe row.
        assert_eq!(chunks.len(), 2);
        let matched = chunks.iter().find(|chunk| cells(chunk, 1) == vec![Some(20)]);
        let unmatched = chunks.iter().find(|chunk| cells(chunk, 1) == vec![None]);
        assert!(matched.is_some(), "the match must carry a build value");
        assert_eq!(
            unmatched.map(|chunk| cells(chunk, 0)),
            Some(vec![Some(10)]),
            "the unmatched probe row keeps its probe value"
        );
    }

    /// Right: an unmatched build row is emitted with a typed null probe column.
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

        assert_eq!(chunks.len(), 2);
        let unmatched = chunks
            .iter()
            .find(|chunk| cells(chunk, 0) == vec![None])
            .expect("an unmatched build row must have a null probe column");
        assert_eq!(cells(unmatched, 1), vec![Some(40)]);
    }

    /// A build column's type is only needed when the build side is empty, and a
    /// missing one is refused rather than guessed.
    #[test]
    fn a_missing_build_type_is_refused() {
        let (table, probe) = fixture(&[20], &[10]);
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
