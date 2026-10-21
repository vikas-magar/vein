use std::sync::Arc;

use common::Result;
use common::arrow::SelectionVector;
use common::protocol::PhysicalExpr;
use functions::FunctionRegistry;

use crate::expression::eval::{ExpressionEvaluator, ExpressionKernelProvider};
use crate::expression::value::ExpressionValue;
use crate::pipeline::view::DerivedColumnBuilder;
use crate::pipeline::{DataChunkView, ViewColumn};

pub(super) fn apply<K>(
    input: &Arc<DataChunkView>,
    expressions: &[PhysicalExpr],
    kernels: &K,
    functions: &FunctionRegistry,
) -> Result<DataChunkView>
where
    K: ExpressionKernelProvider + ?Sized,
{
    let active = Arc::new(input.compact_active_rows()?);
    let evaluator = ExpressionEvaluator::new(kernels, functions);
    let values = expressions
        .iter()
        .map(|expression| evaluator.evaluate(expression, &active))
        .collect::<Result<Vec<_>>>()?;
    let (columns, computed) = projected_columns(&active, &values)?;
    active.project_with_columns(
        &columns,
        computed,
        SelectionVector::new_dense(active.num_rows()),
    )
}

fn projected_columns(
    input: &Arc<DataChunkView>,
    values: &[ExpressionValue],
) -> Result<(Vec<Option<usize>>, Vec<ViewColumn>)> {
    let mut columns = Vec::with_capacity(values.len());
    let mut computed = Vec::new();
    for value in values {
        if let ExpressionValue::ViewColumn(column) = value
            && let Some(index) = column.column_in_view(input)
        {
            columns.push(Some(index));
            continue;
        }
        columns.push(None);
        computed.push(derived_column(value, input.num_rows())?);
    }
    Ok((columns, computed))
}

fn derived_column(value: &ExpressionValue, rows: usize) -> Result<ViewColumn> {
    let mut builder = DerivedColumnBuilder::new(value.data_type().clone(), rows);
    for row in 0..rows {
        builder.push(value.source_row(row, rows)?)?;
    }
    builder.finish()?.column_view(0)
}
