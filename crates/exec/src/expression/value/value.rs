use std::sync::Arc;

use arrow::array::ArrayRef;
use arrow::datatypes::DataType;
use common::Result;
use error::VeinError;

use super::ExpressionDatum;
use super::SegmentedArray;
use crate::pipeline::view::ViewSourceRow;
use crate::pipeline::DataChunkView;
#[cfg(test)]
use crate::pipeline::MaterializationReport;

/// Reference-backed or materialized value produced by expression evaluation.
#[derive(Debug, Clone)]
pub enum ExpressionValue {
    /// Dense Arrow array produced by a scan or an expression kernel.
    Array(ArrayRef),
    /// One-value Arrow array interpreted as a broadcast scalar.
    Scalar(ArrayRef),
    /// Logical column backed by source arrays and a view row mapping.
    ViewColumn(ViewColumnValue),
    /// Computed column backed by several ordered Arrow result arrays.
    Segments(SegmentedArray),
}

/// Reference-backed column metadata shared by expression aliases.
#[derive(Debug, Clone)]
pub struct ViewColumnValue(pub(super) Arc<ViewColumnInner>);

#[derive(Debug)]
pub(super) struct ViewColumnInner {
    pub(super) view: Arc<DataChunkView>,
    pub(super) column: usize,
    pub(super) data_type: DataType,
}

impl ExpressionValue {
    /// Whether this value is represented as a broadcast scalar.
    pub fn is_scalar(&self) -> bool {
        matches!(self, Self::Scalar(_))
    }

    /// Returns a dense Arrow array when one is present.
    ///
    /// Reference-backed and segmented values require explicit materialization.
    pub fn array(&self) -> Result<&ArrayRef> {
        match self {
            Self::Array(array) | Self::Scalar(array) => Ok(array),
            Self::ViewColumn(_) => Err(VeinError::internal(
                "view-backed expression value must be materialized before Arrow array access",
            )),
            Self::Segments(_) => Err(VeinError::internal(
                "segmented expression value must be materialized before Arrow array access",
            )),
        }
    }

    /// Whether this value can be consumed through zero-copy row segments.
    pub(crate) fn is_mapped(&self) -> bool {
        matches!(self, Self::ViewColumn(_) | Self::Segments(_))
    }

    /// Materializes an expression result, but never implicitly gathers a source view.
    #[cfg(test)]
    pub(crate) fn materialized_array(&self) -> Result<ArrayRef> {
        match self {
            Self::Array(array) | Self::Scalar(array) => Ok(Arc::clone(array)),
            Self::Segments(segments) => segments.materialized_array(),
            Self::ViewColumn(_) => Err(VeinError::internal(
                "view-backed expression value requires view materialization",
            )),
        }
    }

    /// Logical row count represented by this expression value.
    pub fn physical_len(&self) -> usize {
        match self {
            Self::Array(array) | Self::Scalar(array) => array.len(),
            Self::ViewColumn(column) => column.0.view.num_rows(),
            Self::Segments(segments) => segments.len(),
        }
    }

    /// Arrow type of this expression value.
    pub fn data_type(&self) -> &DataType {
        match self {
            Self::Array(array) | Self::Scalar(array) => array.data_type(),
            Self::ViewColumn(column) => &column.0.data_type,
            Self::Segments(segments) => segments.data_type(),
        }
    }

    /// Wraps an already dense value for APIs that accept Arrow's `Datum` trait.
    pub fn dense_datum(&self) -> Result<ExpressionDatum<'_>> {
        Ok(ExpressionDatum::from_array(self.array()?, self.is_scalar()))
    }

    pub(crate) fn view_column(view: Arc<DataChunkView>, column: usize) -> Result<Self> {
        let data_type = view.column_data_type(column)?.clone();
        Ok(Self::ViewColumn(ViewColumnValue(Arc::new(
            ViewColumnInner {
                view,
                column,
                data_type,
            },
        ))))
    }

    pub(crate) fn source_row(
        &self,
        row: usize,
        expected_rows: usize,
    ) -> Result<Option<ViewSourceRow<'_>>> {
        if row >= expected_rows {
            return Err(VeinError::internal(format!(
                "expression source row {row} is outside {expected_rows} rows"
            )));
        }
        match self {
            Self::Array(array) => {
                if array.len() != expected_rows {
                    return Err(VeinError::internal(format!(
                        "expression array has {} rows, expected {expected_rows}",
                        array.len()
                    )));
                }
                Ok(Some((array, row, None)))
            }
            Self::Scalar(array) => {
                if array.len() != 1 {
                    return Err(VeinError::internal(format!(
                        "expression scalar has {} values, expected one",
                        array.len()
                    )));
                }
                Ok(Some((array, 0, None)))
            }
            Self::ViewColumn(column) => {
                if column.0.view.num_rows() != expected_rows {
                    return Err(VeinError::internal(format!(
                        "expression view has {} rows, expected {expected_rows}",
                        column.0.view.num_rows()
                    )));
                }
                column.0.view.resolve_with_lease(column.0.column, row)
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

    #[cfg(test)]
    pub(crate) fn materialize_with_report(&self) -> Result<(ArrayRef, MaterializationReport)> {
        self.0.view.materialize_column_with_report(self.0.column)
    }
}
