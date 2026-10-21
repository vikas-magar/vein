use std::sync::Arc;

use arrow::array::{Array, ArrayRef, BooleanArray, Int64Array, builder::BooleanBuilder};
use common::Result;
use common::arrow::SelectionVector;
use error::VeinError;
use types::{ColumnId, ResolvedColumn, VeinDataType, VeinSchema};

use crate::pipeline::DataChunkView;
use crate::runtime::filter::RuntimeFilter;

#[derive(Debug)]
pub(super) struct KeepValues {
    pub(super) columns: Vec<ColumnId>,
    pub(super) expected: i64,
}

#[derive(Debug)]
pub(super) struct MetadataOnlyRange {
    pub(super) columns: Vec<ColumnId>,
}

impl RuntimeFilter for MetadataOnlyRange {
    fn name(&self) -> &'static str {
        "test_metadata_range"
    }

    fn required_columns(&self) -> &[ColumnId] {
        &self.columns
    }

    fn memory_bytes(&self) -> usize {
        0
    }

    fn evaluate(
        &self,
        _view: &DataChunkView,
        _column_positions: &[usize],
    ) -> Result<BooleanArray> {
        unreachable!("metadata-only filter is not evaluated on rows")
    }

    fn supports_row_evaluation(&self) -> bool {
        false
    }

    fn statistics_predicates(&self, _schema: &VeinSchema) -> Vec<catalog::StatisticsPredicate> {
        vec![catalog::StatisticsPredicate::Comparison {
            column: "id".into(),
            comparison: catalog::StatisticsComparison::GreaterOrEqual,
            literal: types::LogicalLiteral::Int64(1),
        }]
    }
}

impl RuntimeFilter for KeepValues {
    fn name(&self) -> &'static str {
        "test_keep_values"
    }

    fn required_columns(&self) -> &[ColumnId] {
        &self.columns
    }

    fn memory_bytes(&self) -> usize {
        0
    }

    fn evaluate(
        &self,
        view: &DataChunkView,
        column_positions: &[usize],
    ) -> Result<BooleanArray> {
        let column = *column_positions
            .first()
            .ok_or_else(|| VeinError::internal("test filter has no bound key column"))?;
        let mut output = BooleanBuilder::with_capacity(view.num_rows());
        for row in 0..view.num_rows() {
            let keep = match view.resolve(column, row)? {
                Some((array, index)) => {
                    let values = array
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .ok_or_else(|| VeinError::internal("test key must be Int64"))?;
                    values.is_valid(index) && values.value(index) == self.expected
                }
                None => false,
            };
            output.append_value(keep);
        }
        Ok(output.finish())
    }
}

pub(super) fn int64_view(columns: Vec<Vec<i64>>) -> DataChunkView {
    let arrays = columns
        .into_iter()
        .map(|values| Arc::new(Int64Array::from(values)) as ArrayRef)
        .collect::<Vec<_>>();
    let row_count = arrays.first().map_or(0, |array| array.len());
    DataChunkView::from_arrays(arrays, row_count, SelectionVector::new_dense(row_count))
        .expect("valid dense test view")
}

pub(super) fn int64_view_with_selection(
    columns: Vec<Vec<i64>>,
    selection: SelectionVector,
) -> DataChunkView {
    let arrays = columns
        .into_iter()
        .map(|values| Arc::new(Int64Array::from(values)) as ArrayRef)
        .collect::<Vec<_>>();
    let row_count = arrays.first().map_or(0, |array| array.len());
    DataChunkView::from_arrays(arrays, row_count, selection).expect("valid selected test view")
}

pub(super) fn schema() -> VeinSchema {
    VeinSchema::new(vec![
        Arc::new(ResolvedColumn::new(
            ColumnId(7),
            "id",
            None,
            VeinDataType::Int64,
        )),
        Arc::new(ResolvedColumn::new(
            ColumnId(8),
            "label",
            None,
            VeinDataType::Utf8,
        )),
    ])
}
