use std::sync::{Arc, OnceLock};

use arrow::array::{Array, ArrayRef, Datum};
use arrow::datatypes::DataType;
use common::Result;
use error::VeinError;

use super::SegmentedArray;
use crate::pipeline::view::ViewSourceRow;
use crate::pipeline::{DataChunkView, MaterializationReport, ViewColumn};
#[path = "segment.rs"]
pub(crate) mod segment;

/// Register value storage; mapped columns keep source arrays and row selection.
#[derive(Debug, Clone)]
pub enum RegValue {
    /// Dense Arrow array produced by a scan or an expression kernel.
    Array(ArrayRef),
    /// One-value Arrow array interpreted as a broadcast scalar.
    Scalar(ArrayRef),
    /// Logical column backed by source arrays and a view row mapping.
    ViewColumn(ViewColumnValue),
    /// Computed column backed by several ordered Arrow result arrays.
    Segments(SegmentedArray),
}

/// Cached view-backed register metadata shared by register aliases.
#[derive(Debug, Clone)]
pub struct ViewColumnValue(Arc<ViewColumnInner>);

#[derive(Debug)]
struct ViewColumnInner {
    view: Arc<DataChunkView>,
    column: usize,
    data_type: DataType,
    materialized: OnceLock<ArrayRef>,
}

/// Dense view of a register that implements Arrow's scalar/array input contract.
#[derive(Clone, Copy)]
pub struct RegisterDatum<'a> {
    array: &'a ArrayRef,
    scalar: bool,
}

impl RegValue {
    /// Whether this value is represented as a broadcast scalar.
    pub fn is_scalar(&self) -> bool {
        matches!(self, Self::Scalar(_))
    }

    /// Returns a dense Arrow array when one is present.
    ///
    /// Mapped and segmented values must first be materialized through their register file.
    pub fn array(&self) -> Result<&ArrayRef> {
        match self {
            Self::Array(array) | Self::Scalar(array) => Ok(array),
            Self::ViewColumn(_) => Err(VeinError::internal(
                "view-backed VM register must be materialized before Arrow array access",
            )),
            Self::Segments(_) => Err(VeinError::internal(
                "segmented VM register must be materialized before Arrow array access",
            )),
        }
    }

    /// Whether this value can be consumed through zero-copy row segments.
    pub(crate) fn is_mapped(&self) -> bool {
        matches!(self, Self::ViewColumn(_) | Self::Segments(_))
    }

    /// Materializes an expression result, but never implicitly gathers a source view.
    pub(crate) fn materialized_array(&self) -> Result<ArrayRef> {
        match self {
            Self::Array(array) | Self::Scalar(array) => Ok(Arc::clone(array)),
            Self::Segments(segments) => segments.materialized_array(),
            Self::ViewColumn(_) => Err(VeinError::internal(
                "view-backed VM register requires register-file materialization",
            )),
        }
    }

    /// Logical row count represented by this register value.
    pub fn physical_len(&self) -> usize {
        match self {
            Self::Array(array) | Self::Scalar(array) => array.len(),
            Self::ViewColumn(column) => column.0.view.num_rows(),
            Self::Segments(segments) => segments.len(),
        }
    }

    /// Arrow type of this register value.
    pub fn data_type(&self) -> &DataType {
        match self {
            Self::Array(array) | Self::Scalar(array) => array.data_type(),
            Self::ViewColumn(column) => &column.0.data_type,
            Self::Segments(segments) => segments.data_type(),
        }
    }

    /// Wraps an already dense value for APIs that accept Arrow's `Datum` trait.
    pub fn dense_datum(&self) -> Result<RegisterDatum<'_>> {
        Ok(RegisterDatum::from_array(self.array()?, self.is_scalar()))
    }

    pub(crate) fn view_column(view: Arc<DataChunkView>, column: usize) -> Result<Self> {
        let data_type = view.column_data_type(column)?.clone();
        Ok(Self::ViewColumn(ViewColumnValue(Arc::new(ViewColumnInner {
            view,
            column,
            data_type,
            materialized: OnceLock::new(),
        }))))
    }

    pub(crate) fn view_column_index(&self) -> Option<usize> {
        match self {
            Self::ViewColumn(column) => Some(column.0.column),
            Self::Array(_) | Self::Scalar(_) | Self::Segments(_) => None,
        }
    }

    pub(crate) fn source_row(
        &self,
        row: usize,
        expected_rows: usize,
    ) -> Result<Option<ViewSourceRow<'_>>> {
        if row >= expected_rows {
            return Err(VeinError::internal(format!(
                "VM source row {row} is outside {expected_rows} rows"
            )));
        }
        match self {
            Self::Array(array) => {
                if array.len() != expected_rows {
                    return Err(VeinError::internal(format!(
                        "VM array input has {} rows, expected {expected_rows}",
                        array.len()
                    )));
                }
                Ok(Some((array, row, None)))
            }
            Self::Scalar(array) => {
                if array.len() != 1 {
                    return Err(VeinError::internal(format!(
                        "VM scalar input has {} values, expected one",
                        array.len()
                    )));
                }
                Ok(Some((array, 0, None)))
            }
            Self::ViewColumn(column) => {
                if column.0.view.num_rows() != expected_rows {
                    return Err(VeinError::internal(format!(
                        "VM view input has {} rows, expected {expected_rows}",
                        column.0.view.num_rows()
                    )));
                }
                column
                    .0
                    .view
                    .resolve_with_lease(column.0.column, row)
            }
            Self::Segments(segments) => {
                let (array, source_row) = segments.source_at(row, expected_rows)?;
                Ok(Some((array, source_row, None)))
            }
        }
    }
}

impl ViewColumnValue {
    pub(crate) fn column_in_view(&self, view: &Arc<DataChunkView>) -> Option<usize> {
        Arc::ptr_eq(&self.0.view, view).then_some(self.0.column)
    }

    pub(crate) fn to_view_column(&self, expected_rows: usize) -> Result<ViewColumn> {
        if self.0.view.num_rows() != expected_rows {
            return Err(VeinError::internal(format!(
                "VM derived view has {} rows, expected {expected_rows}",
                self.0.view.num_rows()
            )));
        }
        self.0.view.column_view(self.0.column)
    }

    pub(crate) fn materialize_with_report(&self) -> Result<(ArrayRef, MaterializationReport)> {
        if let Some(array) = self.0.materialized.get() {
            return Ok((Arc::clone(array), MaterializationReport::default()));
        }
        let (array, report) = self
            .0
            .view
            .materialize_column_with_report(self.0.column)?;
        match self.0.materialized.set(Arc::clone(&array)) {
            Ok(()) => Ok((array, report)),
            Err(_) => Ok((
                Arc::clone(self.0.materialized.get().ok_or_else(|| {
                    VeinError::internal("view register materialization cache was not initialized")
                })?),
                MaterializationReport::default(),
            )),
        }
    }
}

impl<'a> RegisterDatum<'a> {
    pub(crate) fn from_array(array: &'a ArrayRef, scalar: bool) -> Self {
        Self { array, scalar }
    }
}

impl Datum for RegisterDatum<'_> {
    fn get(&self) -> (&dyn Array, bool) {
        (self.array.as_ref(), self.scalar)
    }
}
