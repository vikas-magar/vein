//! Immutable, reference-backed views over columnar batches.
//!
//! A view separates a row's logical position from the Arrow array and row that
//! supply each column. A compact [`RowMapping`] is shared by columns that have
//! the same lineage; projecting a column clones only its `Arc` handles. Joining
//! views composes row mappings once, so a chained join still points at the
//! original arrays instead of building a chain of per-row objects or copying
//! payload values.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{new_empty_array, new_null_array, Array, ArrayRef};
use arrow::compute::interleave;
use arrow::datatypes::DataType;
use common::arrow::SelectionVector;
use common::Result;
use error::VeinError;

use super::chunk::DataChunk;

/// One physical source row, or the null extension for an outer join.
///
/// The source index uses `u32::MAX` as its null sentinel, keeping explicit row
/// mappings at eight bytes per output row. Valid source indices are checked
/// when a mapping is created.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SourceRow {
    source: u32,
    row: u32,
}

impl SourceRow {
    /// Creates a reference to a row in `source_index`.
    pub fn new(source_index: u32, row_index: u32) -> Result<Self> {
        if source_index == u32::MAX {
            return Err(VeinError::internal(
                "source index u32::MAX is reserved for null-extended rows",
            ));
        }
        Ok(Self {
            source: source_index,
            row: row_index,
        })
    }

    /// A row absent from this input, as occurs on the null-extended side of an
    /// outer join.
    pub const fn null() -> Self {
        Self {
            source: u32::MAX,
            row: 0,
        }
    }

    /// Whether this reference represents a null-extended row.
    pub const fn is_null(self) -> bool {
        self.source == u32::MAX
    }

    /// Source-array group and physical row index. Returns `None` for a null row.
    pub const fn indexes(self) -> Option<(u32, u32)> {
        if self.is_null() {
            None
        } else {
            Some((self.source, self.row))
        }
    }
}

/// A row map shared by all projected columns that follow the same input rows.
///
/// `source_lengths` describes the physical lengths of the source arrays in the
/// same order used by [`SourceRow`]. Identity maps avoid a row-sized allocation;
/// explicit maps use one compact `SourceRow` per output row.
#[derive(Debug, Clone)]
pub struct RowMapping {
    source_lengths: Arc<[usize]>,
    rows: MappingRows,
}

#[derive(Debug, Clone)]
enum MappingRows {
    Identity {
        source: u32,
        first_row: u32,
        len: usize,
    },
    Explicit(Arc<[SourceRow]>),
}

impl RowMapping {
    /// Creates an identity mapping without allocating one reference per row.
    pub fn identity(source_lengths: Arc<[usize]>, source_index: u32, row_count: usize) -> Result<Self> {
        validate_source_index(&source_lengths, source_index)?;
        let source_len = source_lengths[source_index as usize];
        if row_count > source_len || row_count > u32::MAX as usize {
            return Err(VeinError::internal(format!(
                "identity row mapping of {row_count} rows exceeds source {source_index} length {source_len}"
            )));
        }
        Ok(Self {
            source_lengths,
            rows: MappingRows::Identity {
                source: source_index,
                first_row: 0,
                len: row_count,
            },
        })
    }

    /// Creates a mapping from output rows to physical source rows.
    pub fn new(
        source_lengths: Arc<[usize]>,
        rows: impl IntoIterator<Item=Option<SourceRow>>,
    ) -> Result<Self> {
        if source_lengths.len() >= u32::MAX as usize {
            return Err(VeinError::internal(
                "row mapping has more source groups than its u32 index can represent",
            ));
        }

        let rows: Vec<SourceRow> = rows
            .into_iter()
            .map(|row| row.unwrap_or_else(SourceRow::null))
            .collect();
        for (position, row) in rows.iter().copied().enumerate() {
            if let Some((source, index)) = row.indexes() {
                validate_source_index(&source_lengths, source)?;
                let source_len = source_lengths[source as usize];
                if index as usize >= source_len {
                    return Err(VeinError::internal(format!(
                        "row mapping entry {position} references row {index} in source {source}, whose length is {source_len}"
                    )));
                }
            }
        }
        Ok(Self {
            source_lengths,
            rows: MappingRows::Explicit(Arc::from(rows)),
        })
    }

    /// Number of physical output rows represented by this mapping.
    pub fn len(&self) -> usize {
        match &self.rows {
            MappingRows::Identity { len, .. } => *len,
            MappingRows::Explicit(rows) => rows.len(),
        }
    }

    /// Whether the mapping contains no output rows.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn source_row(&self, output_row: usize) -> Result<SourceRow> {
        if output_row >= self.len() {
            return Err(VeinError::internal(format!(
                "view row {output_row} is outside row mapping length {}",
                self.len()
            )));
        }
        Ok(match &self.rows {
            MappingRows::Identity {
                source,
                first_row,
                ..
            } => SourceRow {
                source: *source,
                row: first_row + output_row as u32,
            },
            MappingRows::Explicit(rows) => rows[output_row],
        })
    }

    fn contiguous_source_range(&self, selection: &SelectionVector) -> Option<(usize, usize, usize)> {
        let selection = selection.contiguous_range()?;
        match &self.rows {
            MappingRows::Identity {
                source,
                first_row,
                len,
            } if selection.end <= *len => Some((
                *source as usize,
                *first_row as usize + selection.start,
                selection.len(),
            )),
            _ => None,
        }
    }

    fn compose(&self, selection: &SelectionVector, rows: &[Option<usize>]) -> Result<Self> {
        let mut composed = Vec::with_capacity(rows.len());
        for (output_row, input_row) in rows.iter().copied().enumerate() {
            let Some(input_row) = input_row else {
                composed.push(None);
                continue;
            };
            if input_row >= selection.size() {
                return Err(VeinError::internal(format!(
                    "composed row mapping entry {output_row} references logical input row {input_row}, but the input has {} selected rows",
                    selection.size()
                )));
            }
            let input_physical_row = selection.get_index(input_row);
            let source_row = self.source_row(input_physical_row)?;
            composed.push((!source_row.is_null()).then_some(source_row));
        }
        Self::new(Arc::clone(&self.source_lengths), composed)
    }
}

/// One reference-backed output column.
#[derive(Debug, Clone)]
pub struct ViewColumn {
    arrays: Arc<[ArrayRef]>,
    data_type: DataType,
    mapping: Arc<RowMapping>,
}

impl ViewColumn {
    /// Creates a column from source arrays and the rows that select them.
    pub fn new(
        arrays: impl Into<Arc<[ArrayRef]>>,
        data_type: DataType,
        mapping: Arc<RowMapping>,
    ) -> Result<Self> {
        let arrays = arrays.into();
        if arrays.len() != mapping.source_lengths.len() {
            return Err(VeinError::internal(format!(
                "view column has {} source arrays but its row mapping describes {} source groups",
                arrays.len(),
                mapping.source_lengths.len()
            )));
        }
        for (index, array) in arrays.iter().enumerate() {
            if array.len() != mapping.source_lengths[index] {
                return Err(VeinError::internal(format!(
                    "view column source {index} has {} rows, row mapping declares {}",
                    array.len(),
                    mapping.source_lengths[index]
                )));
            }
            if array.data_type() != &data_type {
                return Err(VeinError::internal(format!(
                    "view column source {index} has type {:?}, expected {data_type:?}",
                    array.data_type()
                )));
            }
        }
        Ok(Self {
            arrays,
            data_type,
            mapping,
        })
    }

    /// The Arrow type of this logical column, including for an empty source.
    pub fn data_type(&self) -> &DataType {
        &self.data_type
    }

    /// Returns the source arrays. Cloning this `Arc` does not copy payload data.
    pub fn source_arrays(&self) -> &Arc<[ArrayRef]> {
        &self.arrays
    }

    /// The shared row mapping used by this column.
    pub fn row_mapping(&self) -> &Arc<RowMapping> {
        &self.mapping
    }

    fn resolve(&self, physical_view_row: usize) -> Result<Option<(&ArrayRef, usize)>> {
        let source_row = self.mapping.source_row(physical_view_row)?;
        let Some((source, row)) = source_row.indexes() else {
            return Ok(None);
        };
        let array = self.arrays.get(source as usize).ok_or_else(|| {
            VeinError::internal(format!("view source index {source} is out of bounds"))
        })?;
        Ok(Some((array, row as usize)))
    }
}

/// A reference-backed columnar batch with a shared active-row selection.
///
/// Each column resolves through an immutable row map to one of its retained
/// source arrays. The selection is applied after the row map, so filters and
/// limits can alter the active rows without gathering column payloads.
#[derive(Debug, Clone)]
pub struct DataChunkView {
    row_count: usize,
    columns: Vec<ViewColumn>,
    selection: SelectionVector,
}

/// Maps output rows from one input view to its logical selected rows.
pub struct ViewInputRows<'a> {
    /// Input view whose arrays are referenced by the combined result.
    pub view: &'a DataChunkView,
    /// Output row to logical input row mapping; `None` marks a null-extended side.
    pub rows: &'a [Option<usize>],
}

/// Identifies an input column to preserve in a composed view.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ViewColumnRef {
    /// Index in the `inputs` passed to [`DataChunkView::combine`].
    pub input: usize,
    /// Column index in that input view.
    pub column: usize,
}

impl DataChunkView {
    /// Creates a view whose rows are null-extended for every supplied column.
    ///
    /// This is used by outer joins when one side has no matching input row. It
    /// stores only row-map metadata; it does not allocate Arrow null arrays.
    pub fn nulls(data_types: &[DataType], row_count: usize) -> Result<Self> {
        let mapping = Arc::new(RowMapping::new(Arc::from(Vec::<usize>::new()), vec![None; row_count])?);
        let columns = data_types
            .iter()
            .map(|data_type| ViewColumn::new(Vec::<ArrayRef>::new(), data_type.clone(), Arc::clone(&mapping)))
            .collect::<Result<Vec<_>>>()?;
        Self::new(row_count, columns, SelectionVector::new_dense(row_count))
    }

    /// Wraps a materialized chunk without copying its Arrow arrays.
    ///
    /// The view keeps the chunk's active selection and represents each source
    /// column through one shared identity row mapping. For a zero-column chunk,
    /// the row count is inferred from its dense selection or the largest sparse
    /// selected row, since `DataChunk` has no separate physical-row-count field.
    pub fn from_chunk(chunk: DataChunk) -> Result<Self> {
        let DataChunk { vectors, selection } = chunk;
        let row_count = infer_physical_row_count(&vectors, &selection);
        if vectors.iter().any(|array| array.len() != row_count) {
            return Err(VeinError::internal(
                "materialized chunk columns have different physical row counts",
            ));
        }
        let source_lengths: Arc<[usize]> = Arc::from(vec![row_count]);
        let mapping = Arc::new(RowMapping::identity(
            source_lengths,
            0,
            row_count,
        )?);
        let columns = vectors
            .into_iter()
            .map(|array| {
                let data_type = array.data_type().clone();
                ViewColumn::new(
                    vec![array],
                    data_type,
                    Arc::clone(&mapping),
                )
            })
            .collect::<Result<Vec<_>>>()?;
        Self::new(row_count, columns, selection)
    }

    /// Creates a view with `row_count` physical mapping entries and an active
    /// selection over those entries.
    pub fn new(row_count: usize, columns: Vec<ViewColumn>, selection: SelectionVector) -> Result<Self> {
        if row_count > u32::MAX as usize {
            return Err(VeinError::internal(format!(
                "view row count {row_count} exceeds the selection vector's u32 index range"
            )));
        }
        for (index, column) in columns.iter().enumerate() {
            if column.mapping.len() != row_count {
                return Err(VeinError::internal(format!(
                    "view column {index} has {} row mappings, expected {row_count}",
                    column.mapping.len()
                )));
            }
        }
        validate_selection(&selection, row_count)?;
        Ok(Self {
            row_count,
            columns,
            selection,
        })
    }

    /// Composes several input views into one output view.
    ///
    /// `inputs` each provide an output-to-input logical row mapping. Output
    /// columns select an input column. The method composes each distinct parent
    /// row mapping once and shares that result between sibling columns; payload
    /// arrays remain the original `ArrayRef`s.
    pub fn combine(
        inputs: &[ViewInputRows<'_>],
        columns: &[ViewColumnRef],
    ) -> Result<Self> {
        let row_count = inputs.first().map_or(0, |input| input.rows.len());
        if inputs.iter().any(|input| input.rows.len() != row_count) {
            return Err(VeinError::internal(
                "composed view inputs have different output row counts",
            ));
        }

        let mut composed_mappings: HashMap<(usize, usize), Arc<RowMapping>> = HashMap::new();
        let mut output = Vec::with_capacity(columns.len());
        for column_ref in columns {
            let input = inputs.get(column_ref.input).ok_or_else(|| {
                VeinError::internal(format!(
                    "view output column references missing input {}",
                    column_ref.input
                ))
            })?;
            let input_column = input.view.columns.get(column_ref.column).ok_or_else(|| {
                VeinError::internal(format!(
                    "view output column references missing column {} in input {}",
                    column_ref.column, column_ref.input
                ))
            })?;
            let mapping_key = (
                column_ref.input,
                Arc::as_ptr(&input_column.mapping) as *const () as usize,
            );
            let mapping = if let Some(mapping) = composed_mappings.get(&mapping_key) {
                Arc::clone(mapping)
            } else {
                let mapping = Arc::new(
                    input_column
                        .mapping
                        .compose(&input.view.selection, input.rows)?,
                );
                composed_mappings.insert(mapping_key, Arc::clone(&mapping));
                mapping
            };
            output.push(ViewColumn::new(
                Arc::clone(&input_column.arrays),
                input_column.data_type.clone(),
                mapping,
            )?);
        }

        Self::new(row_count, output, SelectionVector::new_dense(row_count))
    }

    /// Number of active output rows.
    pub fn num_rows(&self) -> usize {
        self.selection.size()
    }

    /// Whether the active selection contains no rows.
    pub fn is_empty(&self) -> bool {
        self.selection.is_empty()
    }

    /// Number of output columns.
    pub fn num_columns(&self) -> usize {
        self.columns.len()
    }

    /// The shared active-row selection.
    pub fn selection(&self) -> &SelectionVector {
        &self.selection
    }

    /// Resolves an active logical output row to its retained Arrow array and
    /// physical row. `None` is a null-extended row and requires no null array.
    pub fn resolve(&self, column: usize, logical_row: usize) -> Result<Option<(&ArrayRef, usize)>> {
        let column = self.columns.get(column).ok_or_else(|| {
            VeinError::internal(format!("view column {column} is out of bounds"))
        })?;
        if logical_row >= self.selection.size() {
            return Err(VeinError::internal(format!(
                "logical view row {logical_row} is outside selected row count {}",
                self.selection.size()
            )));
        }
        column.resolve(self.selection.get_index(logical_row))
    }

    /// Materializes one active column for an Arrow kernel.
    ///
    /// A contiguous range from one source is returned as a zero-copy slice.
    /// Other mappings are gathered with Arrow's interleave kernel. This keeps
    /// view payloads reference-backed until a consumer explicitly requests an
    /// array.
    pub fn materialize_column(&self, column_index: usize) -> Result<ArrayRef> {
        let column = self.columns.get(column_index).ok_or_else(|| {
            VeinError::internal(format!("view column {column_index} is out of bounds"))
        })?;
        if self.selection.is_empty() {
            return Ok(new_empty_array(&column.data_type));
        }
        if let Some((source, start, len)) =
            column.mapping.contiguous_source_range(&self.selection)
        {
            let array = column.arrays.get(source).ok_or_else(|| {
                VeinError::internal(format!("view source index {source} is out of bounds"))
            })?;
            return Ok(array.slice(start, len));
        }

        let mut source_arrays: Vec<&dyn Array> = column
            .arrays
            .iter()
            .map(|array| array.as_ref())
            .collect();
        let null_source = source_arrays.len();
        let mut indexes = Vec::with_capacity(self.selection.size());
        let mut contiguous: Option<(&ArrayRef, usize, usize)> = None;
        let mut can_slice = true;
        for logical_row in 0..self.selection.size() {
            match column.mapping.source_row(self.selection.get_index(logical_row))?.indexes() {
                Some((source, row)) => {
                    let source = source as usize;
                    let row = row as usize;
                    let array = column.arrays.get(source).ok_or_else(|| {
                        VeinError::internal(format!("view source index {source} is out of bounds"))
                    })?;
                    indexes.push((source, row));
                    match contiguous {
                        None => contiguous = Some((array, row, 1)),
                        Some((first, start, len)) if Arc::ptr_eq(first, array) && row == start + len => {
                            contiguous = Some((first, start, len + 1));
                        }
                        _ => can_slice = false,
                    }
                }
                None => {
                    indexes.push((null_source, 0));
                    can_slice = false;
                }
            }
        }

        if can_slice {
            if let Some((array, start, len)) = contiguous {
                return Ok(array.slice(start, len));
            }
        }

        let null_array = new_null_array(&column.data_type, 1);
        source_arrays.push(null_array.as_ref());
        interleave(&source_arrays, &indexes)
            .map_err(|error| VeinError::internal(format!("materialize view column failed: {error}")))
    }

    /// Materializes active rows into ordinary columns for an operator that
    /// requires Arrow arrays. Contiguous source ranges remain zero-copy slices;
    /// mapped rows and null extensions are gathered by `materialize_column`.
    pub fn to_data_chunk(&self) -> Result<DataChunk> {
        let columns = (0..self.num_columns())
            .map(|column| self.materialize_column(column))
            .collect::<Result<Vec<_>>>()?;
        Ok(DataChunk::new(
            columns,
            SelectionVector::new_dense(self.num_rows()),
        ))
    }

    /// Compacts the active logical rows into a dense view mapping without
    /// copying any source array payloads.
    pub fn compact_active_rows(&self) -> Result<Self> {
        if self.selection.contiguous_range().is_some_and(|range| {
            range.start == 0 && range.len() == self.row_count
        }) {
            return Ok(self.clone());
        }
        let rows = (0..self.num_rows()).map(Some).collect::<Vec<_>>();
        let columns = (0..self.num_columns())
            .map(|column| ViewColumnRef { input: 0, column })
            .collect::<Vec<_>>();
        Self::combine(&[ViewInputRows { view: self, rows: &rows }], &columns)
    }

    /// Projects source-backed columns and computed arrays into one view batch.
    ///
    /// Each `Some(index)` preserves a column from this view; each `None` takes
    /// the next computed array. The output row selection is applied to both
    /// kinds of columns without gathering passthrough payloads.
    pub fn project_with_arrays(
        &self,
        columns: &[Option<usize>],
        computed: Vec<ArrayRef>,
        selection: SelectionVector,
    ) -> Result<Self> {
        let mut computed = computed.into_iter();
        let mut output = Vec::with_capacity(columns.len());
        for column in columns {
            match column {
                Some(index) => output.push(self.columns.get(*index).cloned().ok_or_else(|| {
                    VeinError::internal(format!("view projection column {index} is out of bounds"))
                })?),
                None => {
                    let array = computed.next().ok_or_else(|| {
                        VeinError::internal("view projection is missing a computed array")
                    })?;
                    if array.len() != self.row_count {
                        return Err(VeinError::internal(format!(
                            "computed view column has {} rows, expected {}",
                            array.len(), self.row_count
                        )));
                    }
                    let mapping = Arc::new(RowMapping::identity(
                        Arc::from(vec![array.len()]),
                        0,
                        array.len(),
                    )?);
                    output.push(ViewColumn::new(
                        vec![Arc::clone(&array)],
                        array.data_type().clone(),
                        mapping,
                    )?);
                }
            }
        }
        if computed.next().is_some() {
            return Err(VeinError::internal(
                "view projection has unused computed arrays",
            ));
        }
        Self::new(self.row_count, output, selection)
    }

    /// Returns a projection that may repeat columns without copying their data.
    pub fn project(&self, columns: &[usize]) -> Result<Self> {
        let projected = columns
            .iter()
            .map(|&index| {
                self.columns.get(index).cloned().ok_or_else(|| {
                    VeinError::internal(format!("view projection column {index} is out of bounds"))
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Self::new(self.row_count, projected, self.selection.clone())
    }

    /// Replaces the active row selection while retaining source arrays and maps.
    pub fn with_selection(&self, selection: SelectionVector) -> Result<Self> {
        self.clone().into_selection(selection)
    }

    /// Replaces the active row selection while taking ownership of the view.
    ///
    /// This avoids even cloning the view's `Arc` handles when a transform is
    /// already consuming the input batch.
    pub fn into_selection(mut self, selection: SelectionVector) -> Result<Self> {
        validate_selection(&selection, self.row_count)?;
        self.selection = selection;
        Ok(self)
    }

    /// Whether an active output value in this column is null-extended.
    pub fn is_null(&self, column: usize, logical_row: usize) -> Result<bool> {
        Ok(self.resolve(column, logical_row)?.is_none())
    }
}

fn validate_source_index(source_lengths: &[usize], source_index: u32) -> Result<()> {
    if source_index as usize >= source_lengths.len() {
        return Err(VeinError::internal(format!(
            "row mapping source index {source_index} is outside {} source groups",
            source_lengths.len()
        )));
    }
    Ok(())
}

fn validate_selection(selection: &SelectionVector, row_count: usize) -> Result<()> {
    for logical_row in 0..selection.size() {
        let physical_row = selection.get_index(logical_row);
        if physical_row >= row_count {
            return Err(VeinError::internal(format!(
                "view selection entry {logical_row} references row {physical_row}, but the view has {row_count} mapping rows"
            )));
        }
    }
    Ok(())
}

fn infer_physical_row_count(vectors: &[ArrayRef], selection: &SelectionVector) -> usize {
    if let Some(array) = vectors.first() {
        return array.len();
    }
    selection.contiguous_range().map_or_else(
        || {
            (0..selection.size())
                .map(|row| selection.get_index(row) + 1)
                .max()
                .unwrap_or(0)
        },
        |range| range.end,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Int64Array};

    fn ints(values: &[i64]) -> ArrayRef {
        Arc::new(Int64Array::from(values.to_vec()))
    }

    fn int64_mapping(lengths: &[usize], rows: &[Option<SourceRow>]) -> Arc<RowMapping> {
        Arc::new(
            RowMapping::new(Arc::from(lengths.to_vec()), rows.iter().copied())
                .expect("valid row mapping"),
        )
    }

    fn view_column(arrays: Vec<ArrayRef>, mapping: Arc<RowMapping>) -> ViewColumn {
        ViewColumn::new(arrays, DataType::Int64, mapping).expect("valid view column")
    }

    fn int_at(view: &DataChunkView, column: usize, row: usize) -> Option<i64> {
        let (array, physical_row) = view.resolve(column, row).expect("resolve view row")?;
        Some(
            array
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("int64 view value")
                .value(physical_row),
        )
    }

    #[test]
    fn resolves_rows_from_the_same_and_different_source_chunks_without_copying() {
        let first = ints(&[10, 20]);
        let second = ints(&[30, 40]);
        let mapping = int64_mapping(
            &[2, 2],
            &[
                Some(SourceRow::new(0, 1).unwrap()),
                Some(SourceRow::new(1, 0).unwrap()),
                Some(SourceRow::new(1, 1).unwrap()),
            ],
        );
        let view = DataChunkView::new(
            3,
            vec![view_column(vec![Arc::clone(&first), Arc::clone(&second)], mapping)],
            SelectionVector::new_dense(3),
        )
            .expect("valid view");

        assert_eq!((0..3).map(|row| int_at(&view, 0, row)).collect::<Vec<_>>(), [Some(20), Some(30), Some(40)]);
        let (resolved, row) = view.resolve(0, 1).unwrap().unwrap();
        assert!(Arc::ptr_eq(resolved, &second));
        assert_eq!(row, 0);
        assert_eq!(
            view.resolve(0, 0).unwrap().unwrap().0.as_ref().as_any().downcast_ref::<Int64Array>().unwrap().values().as_ptr(),
            first.as_ref().as_any().downcast_ref::<Int64Array>().unwrap().values().as_ptr(),
            "view resolution must retain the source value buffer"
        );
    }

    #[test]
    fn repeated_rows_and_duplicate_projection_columns_share_the_same_references() {
        let input = ints(&[7, 8]);
        let mapping = int64_mapping(
            &[2],
            &[
                Some(SourceRow::new(0, 1).unwrap()),
                Some(SourceRow::new(0, 1).unwrap()),
                Some(SourceRow::new(0, 0).unwrap()),
            ],
        );
        let view = DataChunkView::new(
            3,
            vec![view_column(vec![Arc::clone(&input)], mapping)],
            SelectionVector::new_dense(3),
        )
            .expect("valid repeated-row view");
        let projected = view.project(&[0, 0]).expect("duplicate projection");

        assert_eq!((0..3).map(|row| int_at(&projected, 0, row)).collect::<Vec<_>>(), [Some(8), Some(8), Some(7)]);
        assert!(Arc::ptr_eq(
            projected.columns[0].row_mapping(),
            projected.columns[1].row_mapping()
        ));
        assert!(Arc::ptr_eq(
            &projected.columns[0].arrays[0],
            &projected.columns[1].arrays[0]
        ));
        assert!(Arc::ptr_eq(&projected.columns[0].arrays[0], &input));
    }

    #[test]
    fn outer_join_null_extension_uses_mapping_metadata_without_null_arrays() {
        let input = ints(&[11, 22]);
        let mapping = int64_mapping(
            &[2],
            &[Some(SourceRow::new(0, 1).unwrap()), None],
        );
        let view = DataChunkView::new(
            2,
            vec![view_column(vec![Arc::clone(&input)], mapping)],
            SelectionVector::new_dense(2),
        )
            .expect("valid outer join view");

        assert_eq!(int_at(&view, 0, 0), Some(22));
        assert!(view.is_null(0, 1).expect("null extension"));
        assert_eq!(view.columns[0].source_arrays().len(), 1);
        assert!(Arc::ptr_eq(&view.columns[0].source_arrays()[0], &input));
    }

    #[test]
    fn empty_input_and_zero_column_views_preserve_their_row_count() {
        let empty_mapping = int64_mapping(&[], &[]);
        let empty = DataChunkView::new(
            0,
            vec![view_column(Vec::new(), empty_mapping)],
            SelectionVector::new_dense(0),
        )
            .expect("typed empty view");
        let zero_columns = DataChunkView::new(
            3,
            Vec::new(),
            SelectionVector::new_dense(3),
        )
            .expect("zero-column view");

        assert_eq!(empty.num_rows(), 0);
        assert_eq!(empty.num_columns(), 1);
        assert_eq!(zero_columns.num_rows(), 3);
        assert_eq!(zero_columns.num_columns(), 0);
    }

    #[test]
    fn composing_join_views_flattens_row_maps_and_keeps_leaf_buffers_alive() {
        let probe = ints(&[1, 2]);
        let build = ints(&[10, 20]);
        let probe_map = int64_mapping(
            &[2],
            &[Some(SourceRow::new(0, 0).unwrap()), Some(SourceRow::new(0, 1).unwrap())],
        );
        let build_map = int64_mapping(
            &[2],
            &[Some(SourceRow::new(0, 0).unwrap()), Some(SourceRow::new(0, 1).unwrap())],
        );
        let left = DataChunkView::new(
            2,
            vec![view_column(vec![Arc::clone(&probe)], probe_map)],
            SelectionVector::new_dense(2),
        )
            .unwrap();
        let right = DataChunkView::new(
            2,
            vec![view_column(vec![Arc::clone(&build)], build_map)],
            SelectionVector::new_dense(2),
        )
            .unwrap();
        let joined = DataChunkView::combine(
            &[
                ViewInputRows { view: &left, rows: &[Some(1), None] },
                ViewInputRows { view: &right, rows: &[Some(0), Some(1)] },
            ],
            &[
                ViewColumnRef { input: 0, column: 0 },
                ViewColumnRef { input: 1, column: 0 },
            ],
        )
            .expect("join two source views");
        let chained = DataChunkView::combine(
            &[ViewInputRows { view: &joined, rows: &[Some(1), Some(0)] }],
            &[
                ViewColumnRef { input: 0, column: 0 },
                ViewColumnRef { input: 0, column: 1 },
            ],
        )
            .expect("compose a chained join projection");

        assert_eq!(int_at(&chained, 0, 0), None);
        assert_eq!(int_at(&chained, 1, 0), Some(20));
        assert_eq!(int_at(&chained, 0, 1), Some(2));
        assert_eq!(int_at(&chained, 1, 1), Some(10));
        assert!(Arc::ptr_eq(
            chained.resolve(0, 1).unwrap().unwrap().0,
            &probe
        ));
        assert!(Arc::ptr_eq(
            chained.resolve(1, 0).unwrap().unwrap().0,
            &build
        ));
    }

    #[test]
    fn view_selection_changes_active_rows_without_changing_source_arrays() {
        let input = ints(&[3, 5, 7]);
        let mapping = Arc::new(RowMapping::identity(Arc::from(vec![3]), 0, 3).unwrap());
        let view = DataChunkView::new(
            3,
            vec![view_column(vec![Arc::clone(&input)], mapping)],
            SelectionVector::new_dense(3),
        )
            .unwrap()
            .with_selection(SelectionVector::new_sparse(vec![2, 0]))
            .expect("select view rows");

        assert_eq!(int_at(&view, 0, 0), Some(7));
        assert_eq!(int_at(&view, 0, 1), Some(3));
        assert!(Arc::ptr_eq(view.columns[0].source_arrays().first().unwrap(), &input));
    }

    #[test]
    fn compacting_a_dense_view_keeps_its_existing_row_mapping() {
        let input = ints(&[3, 5, 7]);
        let mapping = Arc::new(RowMapping::identity(Arc::from(vec![3]), 0, 3).unwrap());
        let view = DataChunkView::new(
            3,
            vec![view_column(vec![Arc::clone(&input)], Arc::clone(&mapping))],
            SelectionVector::new_dense(3),
        )
            .unwrap();

        let compacted = view.compact_active_rows().unwrap();
        assert!(Arc::ptr_eq(compacted.columns[0].row_mapping(), &mapping));
        assert!(Arc::ptr_eq(
            compacted.columns[0].source_arrays().first().unwrap(),
            &input
        ));
    }

    #[test]
    fn materializing_contiguous_view_rows_returns_a_zero_copy_slice() {
        let input = ints(&[10, 20, 30, 40]);
        let input_values = input.as_any().downcast_ref::<Int64Array>().unwrap().values().as_ptr();
        let mapping = Arc::new(RowMapping::identity(Arc::from(vec![4]), 0, 4).unwrap());
        let view = DataChunkView::new(
            4,
            vec![view_column(vec![Arc::clone(&input)], mapping)],
            SelectionVector::new_sparse(vec![1, 2]),
        )
            .unwrap();

        let output = view.materialize_column(0).unwrap();
        let output = output.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(output.values().as_ref(), &[20, 30]);
        assert_eq!(output.values().as_ptr(), input_values.wrapping_add(1));
    }

    #[test]
    fn materializing_mapped_view_rows_gathers_values_and_null_extensions() {
        let first = ints(&[10, 20]);
        let second = ints(&[30, 40]);
        let mapping = int64_mapping(
            &[2, 2],
            &[
                Some(SourceRow::new(0, 1).unwrap()),
                None,
                Some(SourceRow::new(1, 0).unwrap()),
            ],
        );
        let view = DataChunkView::new(
            3,
            vec![view_column(vec![first, second], mapping)],
            SelectionVector::new_dense(3),
        )
            .unwrap();

        let output = view.materialize_column(0).unwrap();
        let output = output.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(output, &Int64Array::from(vec![Some(20), None, Some(30)]));
    }

    #[test]
    fn wrapping_a_chunk_preserves_selection_and_arrow_buffer_identity() {
        let input = ints(&[10, 20, 30]);
        let view = DataChunkView::from_chunk(DataChunk::new(
            vec![Arc::clone(&input)],
            SelectionVector::new_sparse(vec![2, 0]),
        ))
            .expect("wrap chunk as a view");

        assert_eq!(view.num_rows(), 2);
        assert_eq!(int_at(&view, 0, 0), Some(30));
        assert_eq!(int_at(&view, 0, 1), Some(10));
        assert!(Arc::ptr_eq(view.columns[0].source_arrays().first().unwrap(), &input));
    }

    #[test]
    fn explicit_chunk_boundary_materializes_mapped_and_null_extended_rows() {
        let first = ints(&[10, 20]);
        let second = ints(&[30, 40]);
        let mapping = int64_mapping(
            &[2, 2],
            &[
                Some(SourceRow::new(0, 1).unwrap()),
                None,
                Some(SourceRow::new(1, 0).unwrap()),
            ],
        );
        let view = DataChunkView::new(
            3,
            vec![view_column(vec![first, second], mapping)],
            SelectionVector::new_dense(3),
        )
            .expect("joined view");

        let chunk = view.to_data_chunk().expect("materialize at operator boundary");
        let values = chunk
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("int64 output");
        assert_eq!(values, &Int64Array::from(vec![Some(20), None, Some(30)]));
        assert_eq!(chunk.num_rows(), 3);
    }

    #[test]
    fn wrapping_rejects_selection_or_columns_outside_the_physical_rows() {
        let unequal_columns = DataChunk::new(
            vec![ints(&[1, 2]), ints(&[3])],
            SelectionVector::new_dense(1),
        );
        assert!(DataChunkView::from_chunk(unequal_columns).is_err());

        let invalid_selection = DataChunk::new(
            vec![ints(&[1, 2])],
            SelectionVector::new_sparse(vec![2]),
        );
        assert!(DataChunkView::from_chunk(invalid_selection).is_err());
    }

    #[test]
    fn invalid_source_rows_and_selection_indices_are_rejected() {
        assert!(RowMapping::new(
            Arc::from(vec![2]),
            [Some(SourceRow::new(0, 2).unwrap())],
        )
            .is_err());

        let mapping = int64_mapping(&[2], &[Some(SourceRow::new(0, 0).unwrap())]);
        let column = view_column(vec![ints(&[1, 2])], mapping);
        assert!(DataChunkView::new(
            1,
            vec![column],
            SelectionVector::new_sparse(vec![1]),
        )
            .is_err());
    }
}
