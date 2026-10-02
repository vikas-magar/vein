//! The differential bridge between the executor's Arrow chunk and the engine's
//! own representation.
//!
//! Phase 1 of `vein_design/07-development-plan.md` replaces the Arrow-backed
//! hot path with Vein's own vectors. That migration is only safe if the two
//! representations can be compared while both exist, which is what
//! `09-testing-and-correctness.md` §4 and Rule 33 require: the existing Arrow
//! path stays as the reference oracle until the cut-over completes.
//!
//! The conversion is semantic, not structural. A dense selection that happens to
//! be contiguous comes back as `Selection::All` and an `Indices` list covering
//! every row likewise, because both denote the same active rows. Tests therefore
//! compare rows, never representations.

use std::sync::Arc;

use common::Result;
use common::arrow::SelectionVector;
use error::VeinError;

use super::chunk::DataChunk;

/// Converts a Vein selection into the executor's selection vector.
pub fn to_selection_vector(selection: &vector::Selection, row_count: u32) -> SelectionVector {
    match selection {
        vector::Selection::All => SelectionVector::new_dense(row_count as usize),
        vector::Selection::None => SelectionVector::new_sparse(Vec::new()),
        vector::Selection::Indices(indices) => SelectionVector::new_sparse(indices.to_vec()),
        vector::Selection::Mask(_) => {
            SelectionVector::new_sparse(selection.iter(row_count).collect())
        }
    }
}

/// Converts the executor's selection vector into a Vein selection.
///
/// A dense vector with a non-zero offset, or a sparse one, becomes `Indices`:
/// `01-final-architecture.md` §4 gives `Selection` no contiguous-range form, so
/// an offset cannot be carried across.
pub fn from_selection_vector(selection: &SelectionVector, row_count: u32) -> vector::Selection {
    if selection.size() == 0 {
        return vector::Selection::None;
    }
    if let Some(range) = selection.contiguous_range()
        && range.start == 0
        && range.len() as u32 == row_count
    {
        return vector::Selection::All;
    }
    let indices: Vec<u32> = (0..selection.size())
        .map(|position| selection.get_index(position) as u32)
        .collect();
    vector::Selection::from_indices(indices)
}

/// Converts the executor's Arrow chunk into the engine's representation.
///
/// Returns an error for a column type the bridge does not yet cover, naming it,
/// rather than silently dropping the column.
pub fn arrow_chunk_to_vector(chunk: &DataChunk) -> Result<vector::DataChunk> {
    let columns = chunk
        .vectors
        .iter()
        .enumerate()
        .map(|(index, array)| {
            vector::arrow::from_arrow(array)
                .map(Arc::new)
                .ok_or_else(|| unsupported(index, array))
        })
        .collect::<Result<Vec<_>>>()?;
    let row_count = chunk.physical_len() as u32;
    let selection = from_selection_vector(&chunk.selection, row_count);
    vector::DataChunk::new(row_count, columns, selection)
        .ok_or_else(|| VeinError::internal("converted columns disagree on their length"))
}

/// Converts the engine's representation back into the executor's Arrow chunk.
pub fn vector_chunk_to_arrow(chunk: &vector::DataChunk) -> Result<DataChunk> {
    let vectors = chunk
        .columns()
        .iter()
        .enumerate()
        .map(|(index, column)| {
            vector::arrow::to_arrow(column).ok_or_else(|| {
                VeinError::internal(format!(
                    "vector chunk column {index} has type {:?}, which the Arrow bridge does not cover",
                    column.data_type()
                ))
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let selection = to_selection_vector(chunk.selection(), chunk.row_count());
    Ok(DataChunk::new(vectors, selection))
}

fn unsupported(index: usize, array: &arrow::array::ArrayRef) -> VeinError {
    use arrow::array::Array;
    VeinError::internal(format!(
        "Arrow chunk column {index} has type {:?}, which the Arrow bridge does not cover",
        array.data_type()
    ))
}

#[cfg(test)]
mod tests {
    use arrow::array::{ArrayRef, Int64Array};

    use super::*;

    fn rows(chunk: &vector::DataChunk) -> Vec<Vec<i64>> {
        let mut out = Vec::new();
        for index in 0..chunk.num_columns() {
            let Some(view) = chunk.view(index) else {
                continue;
            };
            let values = view.resolve_i64().expect("i64 column");
            out.push(view.rows().filter_map(|row| values.at(row)).collect());
        }
        out
    }

    fn arrow_chunk(values: &[i64], selection: SelectionVector) -> DataChunk {
        let array: ArrayRef = Arc::new(Int64Array::from(values.to_vec()));
        DataChunk::new(vec![array], selection)
    }

    #[test]
    fn every_selection_state_round_trips_by_rows() {
        let row_count = 5;
        let states = [
            vector::Selection::All,
            vector::Selection::None,
            vector::Selection::from_indices(vec![0, 2, 4]),
            vector::Selection::from_mask(vec![0b1_0111]),
        ];
        for state in &states {
            let arrow = to_selection_vector(state, row_count);
            let back = from_selection_vector(&arrow, row_count);
            assert_eq!(
                back.iter(row_count).collect::<Vec<_>>(),
                state.iter(row_count).collect::<Vec<_>>(),
                "active rows must survive the round trip for {state:?}"
            );
        }
    }

    #[test]
    fn an_offset_dense_selection_becomes_indices() {
        // The executor can express a dense window at an offset; `Selection`
        // cannot, so it degrades to an index list with the same rows.
        let arrow = SelectionVector::new_dense(10).slice(3, 4);
        assert_eq!(arrow.contiguous_range(), Some(3..7));
        let back = from_selection_vector(&arrow, 10);
        assert_eq!(back.iter(10).collect::<Vec<_>>(), vec![3, 4, 5, 6]);
    }

    #[test]
    fn chunks_round_trip_through_both_representations() {
        let arrow = arrow_chunk(&[10, 20, 30, 40], SelectionVector::new_sparse(vec![1, 3]));
        let vector_chunk = arrow_chunk_to_vector(&arrow).expect("convertible");
        assert_eq!(vector_chunk.row_count(), 4);
        assert_eq!(vector_chunk.active_rows(), 2);
        assert_eq!(rows(&vector_chunk), vec![vec![20, 40]]);

        let back = vector_chunk_to_arrow(&vector_chunk).expect("convertible back");
        assert_eq!(back.physical_len(), 4);
        assert_eq!(back.num_rows(), 2);
        assert_eq!(back.selection.get_index(0), 1);
        assert_eq!(back.selection.get_index(1), 3);
    }

    #[test]
    fn the_reference_oracle_agrees_with_the_engine_representation() {
        // Rule 33: the Arrow path is the oracle. Materialising both ways must
        // yield the same values in the same order.
        let arrow = arrow_chunk(&[7, 8, 9], SelectionVector::new_dense(3));
        let vector_chunk = arrow_chunk_to_vector(&arrow).expect("convertible");
        assert_eq!(
            rows(&vector_chunk),
            vec![vec![7, 8, 9]],
            "no rows may be dropped by the conversion"
        );

        let narrowed = DataChunk::new(
            vec![Arc::new(Int64Array::from(vec![7, 8, 9])) as ArrayRef],
            SelectionVector::new_sparse(vec![0, 2]),
        );
        let converted = arrow_chunk_to_vector(&narrowed).expect("convertible");
        assert_eq!(rows(&converted), vec![vec![7, 9]]);
    }

    #[test]
    fn unsupported_types_are_named_not_dropped() {
        let ints: ArrayRef = Arc::new(arrow::array::Int32Array::from(vec![1, 2]));
        let arrow = DataChunk::new(vec![ints], SelectionVector::new_dense(2));
        let error = arrow_chunk_to_vector(&arrow).expect_err("Int32 is not covered yet");
        assert!(
            error.to_string().contains("Int32"),
            "the error must name the type: {error}"
        );
    }
}
