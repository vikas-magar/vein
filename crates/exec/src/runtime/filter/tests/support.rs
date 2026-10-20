use super::*;

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
        _batch: &RecordBatch,
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
        batch: &RecordBatch,
        column_positions: &[usize],
    ) -> Result<BooleanArray> {
        let values = batch
            .column(column_positions[0])
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| VeinError::internal("test key must be Int64"))?;
        let mut output = BooleanBuilder::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            output.append_value(values.is_valid(row) && values.value(row) == self.expected);
        }
        Ok(output.finish())
    }
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
