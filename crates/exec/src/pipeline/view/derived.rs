use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::ArrayRef;
use arrow::datatypes::DataType;
use common::Result;
use common::arrow::SelectionVector;
use error::VeinError;

use super::{DataChunkView, RowMapping, SourceRow, ViewColumn, ViewMemoryLease};

pub(crate) type ViewSourceRow<'a> =
(&'a ArrayRef, usize, Option<&'a Arc<dyn ViewMemoryLease>>);

/// Builds a derived view column from references to existing Arrow rows.
///
/// Source arrays are retained once by identity; each output row stores only a
/// compact source-array/row mapping. Existing accounting leases follow the
/// arrays they protect.
pub(crate) struct DerivedColumnBuilder {
    data_type: DataType,
    arrays: Vec<ArrayRef>,
    source_lengths: Vec<usize>,
    source_indices: HashMap<usize, u32>,
    memory_leases: Vec<Option<Arc<dyn ViewMemoryLease>>>,
    rows: Vec<Option<SourceRow>>,
}

impl DerivedColumnBuilder {
    pub(crate) fn new(data_type: DataType, capacity: usize) -> Self {
        Self {
            data_type,
            arrays: Vec::new(),
            source_lengths: Vec::new(),
            source_indices: HashMap::new(),
            memory_leases: Vec::new(),
            rows: Vec::with_capacity(capacity),
        }
    }

    pub(crate) fn push(
        &mut self,
        source: Option<ViewSourceRow<'_>>,
    ) -> Result<()> {
        let Some((array, row, lease)) = source else {
            self.rows.push(None);
            return Ok(());
        };
        if array.data_type() != &self.data_type || row >= array.len() {
            return Err(VeinError::internal(format!(
                "derived view source row {row} has type {:?} and length {}, expected type {}",
                array.data_type(),
                array.len(),
                self.data_type
            )));
        }
        let identity = Arc::as_ptr(array) as *const () as usize;
        let source = match self.source_indices.get(&identity).copied() {
            Some(source) => source,
            None => {
                let source = u32::try_from(self.arrays.len()).map_err(|_| {
                    VeinError::internal("derived view has more source arrays than its row map supports")
                })?;
                if source == u32::MAX || row > u32::MAX as usize {
                    return Err(VeinError::internal(
                        "derived view source exceeds the compact row mapping limit",
                    ));
                }
                self.source_indices.insert(identity, source);
                self.arrays.push(Arc::clone(array));
                self.source_lengths.push(array.len());
                self.memory_leases.push(lease.map(Arc::clone));
                source
            }
        };
        if let (Some(lease), Some(stored)) = (lease, self.memory_leases[source as usize].as_ref()) {
            if lease.tracker_id() != stored.tracker_id() {
                return Err(VeinError::internal(
                    "one derived source array carries conflicting memory leases",
                ));
            }
        } else if self.memory_leases[source as usize].is_none() {
            self.memory_leases[source as usize] = lease.map(Arc::clone);
        }
        let row = u32::try_from(row)
            .map_err(|_| VeinError::internal("derived view row exceeds its compact mapping limit"))?;
        self.rows.push(Some(SourceRow::new(source, row)?));
        Ok(())
    }

    pub(crate) fn finish(self) -> Result<DataChunkView> {
        let row_count = self.rows.len();
        let mapping = RowMapping::new(Arc::from(self.source_lengths), self.rows)?;
        let mut column = ViewColumn::new(Arc::from(self.arrays), self.data_type, Arc::new(mapping))?;
        column.memory_leases = self.memory_leases;
        DataChunkView::new(
            row_count,
            vec![column],
            SelectionVector::new_dense(row_count),
        )
    }
}

impl DataChunkView {
    /// Clones a logical column while retaining its source maps and memory leases.
    pub(crate) fn column_view(&self, column: usize) -> Result<ViewColumn> {
        self.columns
            .get(column)
            .cloned()
            .ok_or_else(|| VeinError::internal(format!("view column {column} is out of bounds")))
    }

    /// Resolves a selected row and its source lease without materializing it.
    pub(crate) fn resolve_with_lease(
        &self,
        column: usize,
        logical_row: usize,
    ) -> Result<Option<ViewSourceRow<'_>>> {
        let view_column = self
            .columns
            .get(column)
            .ok_or_else(|| VeinError::internal(format!("view column {column} is out of bounds")))?;
        let Some((source, row)) = self.source_row(column, logical_row)? else {
            return Ok(None);
        };
        let source_array = view_column.arrays.get(source).ok_or_else(|| {
            VeinError::internal(format!("view source array {source} is out of bounds"))
        })?;
        let lease = view_column
            .memory_leases
            .get(source)
            .and_then(Option::as_ref);
        Ok(Some((source_array, row, lease)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;

    #[derive(Debug)]
    struct TestLease(usize);

    impl ViewMemoryLease for TestLease {
        fn tracker_id(&self) -> usize {
            self.0
        }
    }

    #[test]
    fn derived_column_deduplicates_arrays_and_carries_their_lease() {
        let array: ArrayRef = Arc::new(Int64Array::from(vec![10, 20]));
        let lease: Arc<dyn ViewMemoryLease> = Arc::new(TestLease(7));
        let mut builder = DerivedColumnBuilder::new(DataType::Int64, 2);
        builder.push(Some((&array, 1, Some(&lease)))).unwrap();
        builder.push(Some((&array, 0, Some(&lease)))).unwrap();
        let view = builder.finish().unwrap();

        assert_eq!(view.source_arrays_for_column(0).unwrap().len(), 1);
        assert!(Arc::ptr_eq(view.columns[0].memory_leases[0].as_ref().unwrap(), &lease));
        assert_eq!(view.source_row(0, 0).unwrap(), Some((0, 1)));
        assert_eq!(view.source_row(0, 1).unwrap(), Some((0, 0)));
    }
}
