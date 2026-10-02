//! A chunk of columnar data in flight.
//!
//! `02-execution-and-vir-lld.md` §6 defines the chunk as `row_count`, the
//! columns, and the selection. The chunk is the unit that moves between
//! operators: columns are shared by reference, so moving a chunk copies handles
//! rather than data (Rule 3), and the selection is what lets an operator narrow
//! the active rows without touching the columns at all.
//!
//! Columns are held in a `SmallVec` because chunk widths are small and stable —
//! the common case should not allocate a heap vector every time a chunk is
//! built.

use smallvec::SmallVec;

use crate::selection::Selection;
use crate::vector::{Vector, VectorRef};
use crate::view::VectorView;

/// A horizontal slice of a relation: columns plus the rows that are active.
#[derive(Debug, Clone)]
pub struct DataChunk {
    /// Rows in every column, active ones included.
    row_count: u32,
    /// The columns, each as long as `row_count`.
    columns: SmallVec<[VectorRef; 8]>,
    /// Which rows are active.
    selection: Selection,
}

impl DataChunk {
    /// Builds a chunk, or `None` if a column's length disagrees with
    /// `row_count`.
    ///
    /// The length agreement is the chunk's whole invariant: a view indexes the
    /// columns by physical row, so a short column would be read out of bounds
    /// the moment a selection reached past it. Rejecting it here makes that
    /// impossible rather than merely unlikely.
    pub fn new(row_count: u32, columns: Vec<VectorRef>, selection: Selection) -> Option<Self> {
        if columns.iter().any(|column| column.len() != row_count) {
            return None;
        }
        Some(Self {
            row_count,
            columns: columns.into_iter().collect(),
            selection,
        })
    }

    /// Rows in every column, active ones included.
    pub fn row_count(&self) -> u32 {
        self.row_count
    }

    /// Number of active rows.
    pub fn active_rows(&self) -> u32 {
        self.selection.active_count(self.row_count)
    }

    /// Whether no row is active.
    pub fn is_empty(&self) -> bool {
        self.active_rows() == 0
    }

    /// Number of columns.
    pub fn num_columns(&self) -> usize {
        self.columns.len()
    }

    /// The columns.
    pub fn columns(&self) -> &[VectorRef] {
        &self.columns
    }

    /// A column by position.
    pub fn column(&self, index: usize) -> Option<&VectorRef> {
        self.columns.get(index)
    }

    /// Which rows are active.
    pub fn selection(&self) -> &Selection {
        &self.selection
    }

    /// Replaces the selection, leaving the columns untouched.
    pub fn with_selection(mut self, selection: Selection) -> Self {
        self.selection = selection;
        self
    }

    /// A view of a column under this chunk's selection.
    ///
    /// This is what an operator passes to a kernel: the view borrows the
    /// column's buffers and the selection, so nothing is copied or cloned.
    pub fn view(&self, index: usize) -> Option<VectorView<'_>> {
        let column: &Vector = self.columns.get(index)?;
        Some(VectorView::new(column, &self.selection, self.row_count))
    }

    /// A chunk holding only `columns`, in the given order.
    ///
    /// Columns are shared, not copied: projecting is a handle list, which is
    /// what keeps a projection from materialising anything. Returns `None` if a
    /// position is out of range rather than silently dropping it.
    pub fn project(&self, columns: &[usize]) -> Option<Self> {
        let projected = columns
            .iter()
            .map(|index| self.columns.get(*index).cloned())
            .collect::<Option<SmallVec<[VectorRef; 8]>>>()?;
        Some(Self {
            row_count: self.row_count,
            columns: projected,
            selection: self.selection.clone(),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::validity::Validity;

    fn chunk(row_count: u32) -> DataChunk {
        let a: VectorRef = Arc::new(Vector::from_i64(&(0..row_count as i64).collect::<Vec<_>>()));
        let b: VectorRef = Arc::new(Vector::from_utf8(&vec!["x"; row_count as usize]));
        DataChunk::new(row_count, vec![a, b], Selection::all()).expect("consistent columns")
    }

    #[test]
    fn a_chunk_reports_its_shape() {
        let chunk = chunk(4);
        assert_eq!(chunk.row_count(), 4);
        assert_eq!(chunk.num_columns(), 2);
        assert_eq!(chunk.active_rows(), 4);
        assert!(!chunk.is_empty());
        assert_eq!(chunk.columns().len(), 2);
        assert!(chunk.column(0).is_some());
        assert!(chunk.column(2).is_none());
    }

    /// The length agreement is the invariant that keeps views in bounds.
    #[test]
    fn mismatched_column_lengths_are_rejected() {
        let short: VectorRef = Arc::new(Vector::from_i64(&[1, 2]));
        assert!(DataChunk::new(3, vec![short], Selection::all()).is_none());

        let long: VectorRef = Arc::new(Vector::from_i64(&[1, 2, 3]));
        assert!(DataChunk::new(2, vec![long], Selection::all()).is_none());
    }

    #[test]
    fn a_selection_narrows_the_chunk_without_touching_columns() {
        let chunk = chunk(4).with_selection(Selection::from_indices(vec![0, 2]));
        assert_eq!(chunk.row_count(), 4, "physical rows are unchanged");
        assert_eq!(chunk.active_rows(), 2);
        assert_eq!(chunk.column(0).expect("column").len(), 4);

        let view = chunk.view(0).expect("column 0");
        assert_eq!(view.len(), 2);
        assert_eq!(view.rows().collect::<Vec<_>>(), vec![0, 2]);
        assert_eq!(
            view.resolve_i64().expect("i64").as_flat(),
            Some(&[0, 1, 2, 3][..])
        );
    }

    #[test]
    fn views_carry_the_chunk_selection() {
        let chunk = chunk(3).with_selection(Selection::from_mask(vec![0b101]));
        let ints = chunk.view(0).expect("column 0");
        let strings = chunk.view(1).expect("column 1");
        assert_eq!(ints.rows().collect::<Vec<_>>(), vec![0, 2]);
        assert_eq!(strings.rows().collect::<Vec<_>>(), vec![0, 2]);
        assert_eq!(
            strings
                .resolve_utf8()
                .and_then(|values| values.as_flat())
                .map(|(bytes, _)| bytes),
            Some(&b"xxx"[..])
        );
    }

    #[test]
    fn validity_is_per_column() {
        let column: VectorRef = Arc::new(
            Vector::from_i64(&[1, 2]).with_validity(Validity::from_bitmap(vec![0b01])),
        );
        let chunk = DataChunk::new(2, vec![column], Selection::all()).expect("consistent");
        let view = chunk.view(0).expect("column 0");
        assert!(view.is_valid(0));
        assert!(!view.is_valid(1));
    }

    #[test]
    fn empty_chunks_are_supported() {
        let chunk = chunk(0);
        assert!(chunk.is_empty());
        assert_eq!(chunk.active_rows(), 0);
        assert_eq!(chunk.view(0).expect("column 0").len(), 0);

        // A zero-row chunk with a selection over nothing selects nothing.
        let empty = DataChunk::new(0, Vec::new(), Selection::none()).expect("no columns");
        assert_eq!(empty.num_columns(), 0);
        assert!(empty.view(0).is_none());
        assert!(empty.is_empty());
    }

    #[test]
    fn projecting_keeps_only_the_named_columns_and_still_shares_them() {
        let chunk = chunk(3).with_selection(Selection::from_indices(vec![1]));
        let projected = chunk.project(&[1, 0]).expect("in range");
        assert_eq!(projected.num_columns(), 2);
        assert_eq!(projected.row_count(), 3, "the shape is unchanged");
        assert_eq!(projected.active_rows(), 1, "the selection carries over");

        // The projection shares the source vectors rather than copying them.
        assert!(Arc::ptr_eq(
            projected.column(1).expect("column 1"),
            chunk.column(0).expect("column 0")
        ));
        assert_eq!(
            projected
                .view(0)
                .expect("column 0")
                .resolve_utf8()
                .and_then(|values| values.as_flat())
                .map(|(bytes, _)| bytes),
            Some(&b"xxx"[..])
        );

        // An out-of-range position is rejected, not dropped.
        assert!(chunk.project(&[0, 5]).is_none());
        assert_eq!(chunk.project(&[]).expect("no columns").num_columns(), 0);
    }
}
