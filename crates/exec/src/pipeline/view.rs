//! Immutable, reference-backed views over columnar batches.
//!
//! A view separates a row's logical position from the Arrow array and row that
//! supply each column. A compact [`RowMapping`] is shared by columns that have
//! the same lineage; projecting a column clones only its `Arc` handles. Joining
//! views composes row mappings once, so a chained join still points at the
//! original arrays instead of building a chain of per-row objects or copying
//! payload values.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, new_empty_array, new_null_array};
use arrow::compute::interleave;
use arrow::datatypes::{DataType, SchemaRef};
use common::Result;
use common::arrow::SelectionVector;
use error::VeinError;

/// Describes array work performed when a view crosses an Arrow batch boundary.
///
/// Gathered output buffer bytes are Arrow's reported buffer footprint, not an
/// exact count of bytes copied or newly allocated: dictionary and nested arrays
/// may retain shared buffers. Shared slices and empty outputs contribute zero.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MaterializationReport {
    /// Columns produced by slicing existing source buffers without copying values.
    pub shared_columns: usize,
    /// Columns produced by gathering mapped rows into an Arrow array.
    pub gathered_columns: usize,
    /// Typed empty columns produced when there are no active rows.
    pub empty_columns: usize,
    /// Arrow buffer footprint of gathered outputs, excluding shared slices.
    pub gathered_output_buffer_bytes: usize,
}

impl MaterializationReport {
    pub(crate) fn accumulate(&mut self, other: Self) {
        self.shared_columns = self.shared_columns.saturating_add(other.shared_columns);
        self.gathered_columns = self.gathered_columns.saturating_add(other.gathered_columns);
        self.empty_columns = self.empty_columns.saturating_add(other.empty_columns);
        self.gathered_output_buffer_bytes = self
            .gathered_output_buffer_bytes
            .saturating_add(other.gathered_output_buffer_bytes);
    }
}

#[derive(Clone, Copy)]
enum ColumnMaterialization {
    Shared,
    Gathered,
    Empty,
}

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
    Concatenated {
        end_offsets: Arc<[usize]>,
        len: usize,
    },
    Repeated {
        source: u32,
        row: u32,
        len: usize,
    },
    Explicit(Arc<[SourceRow]>),
}

impl RowMapping {
    /// Creates an identity mapping without allocating one reference per row.
    pub fn identity(
        source_lengths: Arc<[usize]>,
        source_index: u32,
        row_count: usize,
    ) -> Result<Self> {
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

    /// Maps a dense output range across adjacent source arrays without one row
    /// mapping entry per value.
    pub fn concatenated(source_lengths: Arc<[usize]>) -> Result<Self> {
        if source_lengths.len() >= u32::MAX as usize {
            return Err(VeinError::internal(
                "concatenated row mapping has more source groups than its u32 index can represent",
            ));
        }
        let mut row_count = 0usize;
        let mut end_offsets = Vec::with_capacity(source_lengths.len());
        for (index, source_len) in source_lengths.iter().copied().enumerate() {
            if source_len > u32::MAX as usize {
                return Err(VeinError::internal(format!(
                    "concatenated row mapping source {index} exceeds the u32 row index limit"
                )));
            }
            row_count = row_count.checked_add(source_len).ok_or_else(|| {
                VeinError::internal("concatenated row mapping length overflow")
            })?;
            end_offsets.push(row_count);
        }
        if row_count > u32::MAX as usize {
            return Err(VeinError::internal(
                "concatenated row mapping exceeds the u32 output index limit",
            ));
        }
        if source_lengths.len() == 1 {
            return Self::identity(source_lengths, 0, row_count);
        }
        Ok(Self {
            source_lengths,
            rows: MappingRows::Concatenated {
                end_offsets: Arc::from(end_offsets),
                len: row_count,
            },
        })
    }

    /// Maps each output row to one scalar source value without expanding it.
    pub fn repeated(
        source_lengths: Arc<[usize]>,
        source_index: u32,
        source_row: u32,
        row_count: usize,
    ) -> Result<Self> {
        validate_source_index(&source_lengths, source_index)?;
        let source_len = source_lengths[source_index as usize];
        if source_row as usize >= source_len || row_count > u32::MAX as usize {
            return Err(VeinError::internal(format!(
                "repeated row mapping source row {source_row} or output length {row_count} is invalid"
            )));
        }
        Ok(Self {
            source_lengths,
            rows: MappingRows::Repeated {
                source: source_index,
                row: source_row,
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
            MappingRows::Identity { len, .. }
            | MappingRows::Repeated { len, .. }
            | MappingRows::Concatenated { len, .. } => *len,
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
                source, first_row, ..
            } => SourceRow {
                source: *source,
                row: first_row + output_row as u32,
            },
            MappingRows::Concatenated { end_offsets, .. } => {
                let source = end_offsets.partition_point(|offset| *offset <= output_row);
                let first_row = source
                    .checked_sub(1)
                    .map_or(0, |previous| end_offsets[previous]);
                SourceRow {
                    source: source as u32,
                    row: (output_row - first_row) as u32,
                }
            }
            MappingRows::Repeated { source, row, .. } => SourceRow {
                source: *source,
                row: *row,
            },
            MappingRows::Explicit(rows) => rows[output_row],
        })
    }

    fn contiguous_source_range(
        &self,
        selection: &SelectionVector,
    ) -> Option<(usize, usize, usize)> {
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
            MappingRows::Concatenated { len, .. }
            if selection.end <= *len && selection.start < selection.end =>
                {
                    let (first_source, first_row) = self.source_row(selection.start).ok()?.indexes()?;
                    let (last_source, last_row) = self.source_row(selection.end - 1).ok()?.indexes()?;
                    (first_source == last_source
                        && last_row as usize - first_row as usize + 1 == selection.len())
                        .then_some((first_source as usize, first_row as usize, selection.len()))
                }
            MappingRows::Repeated { source, row, len }
            if selection.end <= *len && selection.len() == 1 =>
                {
                    Some((*source as usize, *row as usize, 1))
                }
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
    memory_leases: Vec<Option<Arc<dyn ViewMemoryLease>>>,
}

/// Keeps an external memory reservation alive while a view retains its source array.
///
/// The trait keeps view ownership independent from the query memory tracker and
/// permits other allocation-accounting strategies to provide the same lifetime
/// behavior.
pub(crate) trait ViewMemoryLease: std::fmt::Debug + Send + Sync {
    fn tracker_id(&self) -> usize;
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
            memory_leases: vec![None; arrays.len()],
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

    /// The accounting lease associated with each retained source array.
    fn memory_leases(&self) -> &[Option<Arc<dyn ViewMemoryLease>>] {
        &self.memory_leases
    }

    /// Replace this column's source leases with entries keyed by array identity.
    fn attach_memory_leases(&mut self, leases: &HashMap<usize, Arc<dyn ViewMemoryLease>>) {
        self.memory_leases = self
            .arrays
            .iter()
            .map(|array| {
                let identity = Arc::as_ptr(array) as *const () as usize;
                leases.get(&identity).cloned()
            })
            .collect();
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
    schema: Option<SchemaRef>,
}

impl common::statistics::StatisticsInput for DataChunkView {
    fn num_rows(&self) -> usize {
        DataChunkView::num_rows(self)
    }
    fn num_columns(&self) -> usize {
        DataChunkView::num_columns(self)
    }
    fn column_type(&self, column: usize) -> Result<&DataType> {
        self.column_data_type(column)
    }
    fn value(&self, column: usize, row: usize) -> Result<Option<(&dyn Array, usize)>> {
        Ok(self
            .resolve(column, row)?
            .map(|(array, index)| (array.as_ref(), index)))
    }
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
        let mapping = Arc::new(RowMapping::new(
            Arc::from(Vec::<usize>::new()),
            vec![None; row_count],
        )?);
        let columns = data_types
            .iter()
            .map(|data_type| {
                ViewColumn::new(
                    Vec::<ArrayRef>::new(),
                    data_type.clone(),
                    Arc::clone(&mapping),
                )
            })
            .collect::<Result<Vec<_>>>()?;
        Self::new(row_count, columns, SelectionVector::new_dense(row_count))
    }

    /// Creates a view over owned Arrow array handles and an explicit active-row
    /// selection. Array payload buffers are retained by reference.
    pub fn from_arrays(
        arrays: Vec<ArrayRef>,
        row_count: usize,
        selection: SelectionVector,
    ) -> Result<Self> {
        if arrays.iter().any(|array| array.len() != row_count) {
            return Err(VeinError::internal(
                "batch columns have different physical row counts",
            ));
        }
        let source_lengths: Arc<[usize]> = Arc::from(vec![row_count]);
        let mapping = Arc::new(RowMapping::identity(source_lengths, 0, row_count)?);
        let columns = arrays
            .into_iter()
            .map(|array| {
                let data_type = array.data_type().clone();
                ViewColumn::new(vec![array], data_type, Arc::clone(&mapping))
            })
            .collect::<Result<Vec<_>>>()?;
        Self::new(row_count, columns, selection)
    }

    /// Creates a view with `row_count` physical mapping entries and an active
    /// selection over those entries.
    pub fn new(
        row_count: usize,
        columns: Vec<ViewColumn>,
        selection: SelectionVector,
    ) -> Result<Self> {
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
            schema: None,
        })
    }

    /// Composes several input views into one output view.
    ///
    /// `inputs` each provide an output-to-input logical row mapping. Output
    /// columns select an input column. The method composes each distinct parent
    /// row mapping once and shares that result between sibling columns; payload
    /// arrays remain the original `ArrayRef`s.
    pub fn combine(inputs: &[ViewInputRows<'_>], columns: &[ViewColumnRef]) -> Result<Self> {
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
            let mut output_column = ViewColumn::new(
                Arc::clone(&input_column.arrays),
                input_column.data_type.clone(),
                mapping,
            )?;
            output_column.memory_leases = input_column.memory_leases.clone();
            output.push(output_column);
        }

        Self::new(row_count, output, SelectionVector::new_dense(row_count))
    }

    /// Composes output rows from several views while retaining their source arrays.
    ///
    /// `rows` contains `(input_index, logical_row)` pairs. Unlike [`Self::combine`],
    /// each output column may draw rows from any input view, which is useful for
    /// operators such as global sort that reorder rows across batches. Only row
    /// mapping metadata and `Arc` handles are created; Arrow payload arrays are
    /// never gathered here.
    pub fn compose_rows(
        inputs: &[&DataChunkView],
        rows: &[(usize, usize)],
        output_columns: usize,
    ) -> Result<Self> {
        if inputs.is_empty() && !rows.is_empty() {
            return Err(VeinError::internal(
                "composed view rows require at least one input view",
            ));
        }

        for (output_row, (input_index, logical_row)) in rows.iter().copied().enumerate() {
            let input = inputs.get(input_index).ok_or_else(|| {
                VeinError::internal(format!(
                    "composed output row {output_row} references missing input view {input_index}"
                ))
            })?;
            if logical_row >= input.num_rows() {
                return Err(VeinError::internal(format!(
                    "composed output row {output_row} references logical row {logical_row}, but input view {input_index} has {} selected rows",
                    input.num_rows()
                )));
            }
        }

        if let Some(first) = inputs.first() {
            for (input_index, input) in inputs.iter().enumerate() {
                if input.num_columns() < output_columns {
                    return Err(VeinError::internal(format!(
                        "composed input view {input_index} has {} columns, expected at least {output_columns}",
                        input.num_columns()
                    )));
                }
                for column_index in 0..output_columns {
                    let expected = &first.columns[column_index].data_type;
                    let actual = &input.columns[column_index].data_type;
                    if actual != expected {
                        return Err(VeinError::internal(format!(
                            "composed input view {input_index} column {column_index} has type {actual:?}, expected {expected:?}"
                        )));
                    }
                }
            }
        } else if output_columns != 0 {
            return Err(VeinError::internal(
                "composed output columns require at least one input view",
            ));
        }

        let mut output = Vec::with_capacity(output_columns);
        for column_index in 0..output_columns {
            let data_type = inputs[0].columns[column_index].data_type.clone();
            let mut arrays = Vec::<ArrayRef>::new();
            let mut memory_leases = Vec::<Option<Arc<dyn ViewMemoryLease>>>::new();
            let mut source_lengths = Vec::<usize>::new();
            let mut source_by_pointer = HashMap::<usize, u32>::new();
            let mut source_remap = inputs
                .iter()
                .map(|input| vec![None; input.columns[column_index].arrays.len()])
                .collect::<Vec<Vec<Option<u32>>>>();

            let mut mapped_rows = Vec::with_capacity(rows.len());
            for (input_index, logical_row) in rows.iter().copied() {
                let input = inputs[input_index];
                let column = &input.columns[column_index];
                let physical_row = input.selection.get_index(logical_row);
                let source_row = column.mapping.source_row(physical_row)?;
                let mapped = if let Some((source, row)) = source_row.indexes() {
                    let source_index = source as usize;
                    let array = column.arrays.get(source_index).ok_or_else(|| {
                        VeinError::internal(format!(
                            "composed view input {input_index} column {column_index} references missing source array {source}"
                        ))
                    })?;
                    let global_source = if let Some(global_source) =
                        source_remap[input_index][source_index]
                    {
                        global_source
                    } else {
                        let pointer = Arc::as_ptr(array) as *const () as usize;
                        let global_source = if let Some(global_source) =
                            source_by_pointer.get(&pointer)
                        {
                            *global_source
                        } else {
                            let global_source = u32::try_from(arrays.len()).map_err(|_| {
                                VeinError::internal(
                                    "composed view has more source arrays than its row mapping can represent",
                                )
                            })?;
                            if global_source == u32::MAX {
                                return Err(VeinError::internal(
                                    "composed view source index exceeds the row mapping range",
                                ));
                            }
                            arrays.push(Arc::clone(array));
                            memory_leases.push(column.memory_leases()[source_index].clone());
                            source_lengths.push(array.len());
                            source_by_pointer.insert(pointer, global_source);
                            global_source
                        };
                        if memory_leases[global_source as usize].is_none() {
                            memory_leases[global_source as usize] =
                                column.memory_leases()[source_index].clone();
                        }
                        source_remap[input_index][source_index] = Some(global_source);
                        global_source
                    };
                    Some(SourceRow::new(global_source, row)?)
                } else {
                    None
                };
                mapped_rows.push(mapped);
            }

            let mapping = Arc::new(RowMapping::new(Arc::from(source_lengths), mapped_rows)?);
            let mut output_column = ViewColumn::new(arrays, data_type, mapping)?;
            output_column.memory_leases = memory_leases;
            output.push(output_column);
        }

        Self::new(rows.len(), output, SelectionVector::new_dense(rows.len()))
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

    /// Returns the unique source-array count and Arrow-reported footprint retained
    /// by this view. Arrays shared by multiple columns are counted once within
    /// this view; separate views and arrays sharing nested Arrow buffers are not
    /// deduplicated. This is a buffer-footprint observation, not newly allocated
    /// or resident process memory.
    pub fn source_array_footprint(&self) -> (usize, usize) {
        let mut seen = HashSet::new();
        let mut arrays = 0usize;
        let mut bytes = 0usize;
        for array in self.columns.iter().flat_map(|column| column.arrays.iter()) {
            let identity = Arc::as_ptr(array) as *const () as usize;
            if seen.insert(identity) {
                arrays = arrays.saturating_add(1);
                bytes = bytes.saturating_add(array.get_array_memory_size());
            }
        }
        (arrays, bytes)
    }

    /// The Arrow type of a logical output column, including null-extended columns.
    pub fn column_data_type(&self, column: usize) -> Result<&DataType> {
        self.columns
            .get(column)
            .map(|column| &column.data_type)
            .ok_or_else(|| VeinError::internal(format!("view column {column} is out of bounds")))
    }

    /// Returns retained source arrays for one column without copying payloads.
    pub fn source_arrays_for_column(&self, column: usize) -> Result<&[ArrayRef]> {
        self.columns
            .get(column)
            .map(|column| column.source_arrays().as_ref())
            .ok_or_else(|| VeinError::internal(format!("view column {column} is out of bounds")))
    }

    /// Arrow footprint estimates for distinct retained source arrays.
    ///
    /// Array identity deduplicates repeated references to the same Arrow array.
    /// Distinct arrays that internally share buffers may be counted more than
    /// once; these are conservative estimates rather than allocator telemetry.
    pub fn source_array_memory_sizes(&self) -> Result<Vec<(usize, usize)>> {
        let mut seen = HashSet::new();
        self.columns
            .iter()
            .flat_map(|column| column.arrays.iter())
            .try_fold(Vec::new(), |mut sizes, array| {
                let identity = Arc::as_ptr(array) as *const () as usize;
                if !seen.insert(identity) {
                    return Ok(sizes);
                }
                sizes.push((identity, array.get_array_memory_size()));
                Ok(sizes)
            })
    }

    /// Attach shared reservations to the distinct source arrays retained by this
    /// view. Existing leases from the same tracker are kept; leases from another
    /// tracker are replaced so hash-table merges can transfer ownership.
    pub(crate) fn account_source_arrays(
        &mut self,
        tracker_id: usize,
        mut acquire: impl FnMut(usize, usize) -> Result<Arc<dyn ViewMemoryLease>>,
    ) -> Result<()> {
        let sizes = self.source_array_memory_sizes()?;
        let mut leases_by_array = HashMap::<usize, Arc<dyn ViewMemoryLease>>::new();

        for column in &self.columns {
            for (array, lease) in column.arrays.iter().zip(&column.memory_leases) {
                if let Some(lease) = lease
                    .as_ref()
                    .filter(|lease| lease.tracker_id() == tracker_id)
                {
                    let identity = Arc::as_ptr(array) as *const () as usize;
                    leases_by_array
                        .entry(identity)
                        .or_insert_with(|| Arc::clone(lease));
                }
            }
        }

        for (identity, bytes) in sizes {
            if let std::collections::hash_map::Entry::Vacant(e) = leases_by_array.entry(identity) {
                e.insert(acquire(identity, bytes)?);
            }
        }

        for column in &mut self.columns {
            for (array, lease) in column.arrays.iter().zip(&mut column.memory_leases) {
                let identity = Arc::as_ptr(array) as *const () as usize;
                let shared = leases_by_array.get(&identity).ok_or_else(|| {
                    VeinError::internal("retained array is missing its memory lease")
                })?;
                if lease
                    .as_ref()
                    .is_none_or(|current| current.tracker_id() != tracker_id)
                {
                    *lease = Some(Arc::clone(shared));
                }
            }
        }
        Ok(())
    }

    /// Attach RAII accounting leases to source arrays by their shared identity.
    pub(crate) fn attach_memory_leases(
        &mut self,
        leases: &HashMap<usize, Arc<dyn ViewMemoryLease>>,
    ) {
        for column in &mut self.columns {
            column.attach_memory_leases(leases);
        }
    }

    /// The shared active-row selection.
    pub fn selection(&self) -> &SelectionVector {
        &self.selection
    }

    /// Resolves an active logical output row to its retained Arrow array and
    /// physical row. `None` is a null-extended row and requires no null array.
    pub fn resolve(&self, column: usize, logical_row: usize) -> Result<Option<(&ArrayRef, usize)>> {
        let column = self
            .columns
            .get(column)
            .ok_or_else(|| VeinError::internal(format!("view column {column} is out of bounds")))?;
        if logical_row >= self.selection.size() {
            return Err(VeinError::internal(format!(
                "logical view row {logical_row} is outside selected row count {}",
                self.selection.size()
            )));
        }
        column.resolve(self.selection.get_index(logical_row))
    }

    /// Resolves an active logical row to its source-array index and physical row.
    /// This exposes row lineage to kernels that can read typed values directly
    /// from retained arrays instead of materializing a selected Arrow column.
    pub fn source_row(&self, column: usize, logical_row: usize) -> Result<Option<(usize, usize)>> {
        let view_column = self
            .columns
            .get(column)
            .ok_or_else(|| VeinError::internal(format!("view column {column} is out of bounds")))?;
        if logical_row >= self.selection.size() {
            return Err(VeinError::internal(format!(
                "logical view row {logical_row} is outside selected row count {}",
                self.selection.size()
            )));
        }
        Ok(view_column
            .mapping
            .source_row(self.selection.get_index(logical_row))?
            .indexes()
            .map(|(source, row)| (source as usize, row as usize)))
    }

    /// Materializes one active column for an Arrow kernel.
    ///
    /// A contiguous range from one source is returned as a zero-copy slice.
    /// Other mappings are gathered with Arrow's interleave kernel. This keeps
    /// view payloads reference-backed until a consumer explicitly requests an
    /// array.
    pub fn materialize_column(&self, column_index: usize) -> Result<ArrayRef> {
        self.materialize_column_impl(column_index)
            .map(|(array, _)| array)
    }

    /// Materializes one active column and reports whether its buffers were
    /// shared, gathered, or empty. Computed result allocations are not included.
    pub fn materialize_column_with_report(
        &self,
        column_index: usize,
    ) -> Result<(ArrayRef, MaterializationReport)> {
        let (array, materialization) = self.materialize_column_impl(column_index)?;
        let mut report = MaterializationReport::default();
        match materialization {
            ColumnMaterialization::Shared => report.shared_columns = 1,
            ColumnMaterialization::Empty => report.empty_columns = 1,
            ColumnMaterialization::Gathered => {
                report.gathered_columns = 1;
                report.gathered_output_buffer_bytes = array.get_buffer_memory_size();
            }
        }
        Ok((array, report))
    }

    fn materialize_column_impl(
        &self,
        column_index: usize,
    ) -> Result<(ArrayRef, ColumnMaterialization)> {
        let column = self.columns.get(column_index).ok_or_else(|| {
            VeinError::internal(format!("view column {column_index} is out of bounds"))
        })?;
        if self.selection.is_empty() {
            return Ok((
                new_empty_array(&column.data_type),
                ColumnMaterialization::Empty,
            ));
        }
        if let Some((source, start, len)) = column.mapping.contiguous_source_range(&self.selection)
        {
            let array = column.arrays.get(source).ok_or_else(|| {
                VeinError::internal(format!("view source index {source} is out of bounds"))
            })?;
            return Ok((array.slice(start, len), ColumnMaterialization::Shared));
        }

        let mut source_arrays: Vec<&dyn Array> =
            column.arrays.iter().map(|array| array.as_ref()).collect();
        let null_source = source_arrays.len();
        let mut indexes = Vec::with_capacity(self.selection.size());
        let mut contiguous: Option<(&ArrayRef, usize, usize)> = None;
        let mut can_slice = true;
        for logical_row in 0..self.selection.size() {
            match column
                .mapping
                .source_row(self.selection.get_index(logical_row))?
                .indexes()
            {
                Some((source, row)) => {
                    let source = source as usize;
                    let row = row as usize;
                    let array = column.arrays.get(source).ok_or_else(|| {
                        VeinError::internal(format!("view source index {source} is out of bounds"))
                    })?;
                    indexes.push((source, row));
                    match contiguous {
                        None => contiguous = Some((array, row, 1)),
                        Some((first, start, len))
                        if Arc::ptr_eq(first, array) && row == start + len =>
                            {
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

        if can_slice && let Some((array, start, len)) = contiguous {
            return Ok((array.slice(start, len), ColumnMaterialization::Shared));
        }

        let null_array = new_null_array(&column.data_type, 1);
        source_arrays.push(null_array.as_ref());
        interleave(&source_arrays, &indexes)
            .map(|array| (array, ColumnMaterialization::Gathered))
            .map_err(|error| {
                VeinError::internal(format!("materialize view column failed: {error}"))
            })
    }

    /// Checks the result schema against active rows without flattening the view.
    ///
    /// Non-nullable fields are checked only against selected rows, so excluded
    /// source NULLs do not invalidate an otherwise valid result.
    pub fn validate_schema(&self, schema: &arrow::datatypes::Schema) -> Result<()> {
        self.validate_schema_types(schema)?;
        for (index, field) in schema.fields().iter().enumerate() {
            if !field.is_nullable() {
                // Dense/contiguous non-NULL source ranges need no per-row scan.
                let column = &self.columns[index];
                if let Some((source, _, _)) =
                    column.mapping.contiguous_source_range(&self.selection)
                    && column.arrays[source].null_count() == 0
                {
                    continue;
                }
                for row in 0..self.num_rows() {
                    let null = match self.resolve(index, row)? {
                        Some((array, source_row)) => array.is_null(source_row),
                        None => true,
                    };
                    if null {
                        return Err(VeinError::internal(format!(
                            "view result column {index} ('{}') is non-nullable but contains NULL",
                            field.name()
                        )));
                    }
                }
            }
        }
        Ok(())
    }

    fn validate_schema_types(&self, schema: &arrow::datatypes::Schema) -> Result<()> {
        if schema.fields().len() != self.num_columns() {
            return Err(VeinError::internal(format!(
                "view result has {} columns, schema requires {}",
                self.num_columns(),
                schema.fields().len()
            )));
        }
        for (index, field) in schema.fields().iter().enumerate() {
            let actual = self.column_data_type(index)?;
            if actual != field.data_type() {
                return Err(VeinError::internal(format!(
                    "view result column {index} ('{}') has type {actual:?}, expected {:?}",
                    field.name(),
                    field.data_type()
                )));
            }
        }
        Ok(())
    }

    pub(crate) fn materialize_columns_with_report(
        &self,
    ) -> Result<(Vec<ArrayRef>, MaterializationReport)> {
        let mut columns = Vec::with_capacity(self.num_columns());
        let mut report = MaterializationReport::default();
        for column in 0..self.num_columns() {
            let (array, column_report) = self.materialize_column_with_report(column)?;
            report.accumulate(column_report);
            columns.push(array);
        }
        Ok((columns, report))
    }

    /// Compacts the active logical rows into a dense view mapping without
    /// copying any source array payloads.
    pub fn compact_active_rows(&self) -> Result<Self> {
        if self
            .selection
            .contiguous_range()
            .is_some_and(|range| range.start == 0 && range.len() == self.row_count)
        {
            return Ok(self.clone());
        }
        let rows = (0..self.num_rows()).map(Some).collect::<Vec<_>>();
        let columns = (0..self.num_columns())
            .map(|column| ViewColumnRef { input: 0, column })
            .collect::<Vec<_>>();
        Self::combine(
            &[ViewInputRows {
                view: self,
                rows: &rows,
            }],
            &columns,
        )
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
        let computed = computed
            .into_iter()
            .map(|array| {
                let mapping = Arc::new(RowMapping::identity(
                    Arc::from(vec![array.len()]),
                    0,
                    array.len(),
                )?);
                ViewColumn::new(
                    vec![Arc::clone(&array)],
                    array.data_type().clone(),
                    mapping,
                )
            })
            .collect::<Result<Vec<_>>>()?;
        self.project_with_columns(columns, computed, selection)
    }

    /// Projects passthrough and computed view columns without materializing
    /// their source arrays.
    pub fn project_with_columns(
        &self,
        columns: &[Option<usize>],
        computed: Vec<ViewColumn>,
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
                    let column = computed.next().ok_or_else(|| {
                        VeinError::internal("view projection is missing a computed column")
                    })?;
                    if column.mapping.len() != self.row_count {
                        return Err(VeinError::internal(format!(
                            "computed view column mapping has {} rows, expected {}",
                            column.mapping.len(),
                            self.row_count
                        )));
                    }
                    output.push(column);
                }
            }
        }
        if computed.next().is_some() {
            return Err(VeinError::internal(
                "view projection has unused computed columns",
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
        let projected_schema = self
            .schema
            .as_ref()
            .map(|schema| schema::project_schema(schema, columns))
            .transpose()?;
        let mut view = Self::new(self.row_count, projected, self.selection.clone())?;
        if let Some(schema) = projected_schema {
            // The source schema was validated and each projected field follows
            // the same checked index as its already-validated view column.
            view.schema = Some(schema);
        }
        Ok(view)
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

    /// Whether an active output value is NULL in its source array or null-extended.
    pub fn is_null(&self, column: usize, logical_row: usize) -> Result<bool> {
        match self.resolve(column, logical_row)? {
            Some((array, physical_row)) => Ok(array.is_null(physical_row)),
            None => Ok(true),
        }
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

#[cfg(test)]
#[path = "view/tests.rs"]
mod tests;

#[cfg(test)]
#[path = "view/mapping_tests.rs"]
mod mapping_tests;

#[path = "view/derived.rs"]
mod derived;
#[path = "view/constructors.rs"]
mod constructors;
#[path = "view/schema.rs"]
mod schema;
#[path = "view/selection.rs"]
mod selection;
#[cfg(test)]
#[path = "view/schema_tests.rs"]
mod schema_tests;
#[cfg(test)]
#[path = "view/selection_tests.rs"]
mod selection_tests;
#[cfg(test)]
#[path = "view/null_tests.rs"]
mod null_tests;
pub(crate) use derived::{DerivedColumnBuilder, ViewSourceRow};
