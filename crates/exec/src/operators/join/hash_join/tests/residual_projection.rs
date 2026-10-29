use std::sync::Arc;

use arrow::array::StringArray;
use arrow::datatypes::DataType;
use common::protocol::{ExprKind, PhysicalExpr};

use super::*;

#[tokio::test]
async fn not_like_residual_reads_declared_utf8_slots_independent_of_join_output() {
    let mut table = HashTable::new(JoinKeys::new(vec![0]), JoinKeys::new(vec![0]));
    table
        .insert(crate::pipeline::test_support::dense_view(vec![
            ints(&[1, 2]),
            Arc::new(StringArray::from(vec![Some("%blocked%"), Some("%blocked%")])),
        ]))
        .expect("insert build keys and residual patterns");

    let output = JoinFilterCollector::default();
    let predicate = PhysicalExpr {
        return_type: types::VeinDataType::Boolean,
        kind: ExprKind::Like {
            expr: Box::new(expression_column(0, types::VeinDataType::Utf8)),
            pattern: Box::new(expression_column(1, types::VeinDataType::Utf8)),
            negated: true,
        },
    };
    let executor = expression_filter(predicate, output.clone());
    let sink = Collect::default();
    let mut join = HashJoin::new(
        Arc::new(table),
        JoinType::Left,
        vec![OutputColumn::Probe(0), OutputColumn::Build(0)],
        vec![DataType::Int64, DataType::Utf8],
        vec![DataType::Int64, DataType::Utf8],
        Box::new(sink.clone()),
    )
        .with_residual_filter(
            vec![OutputColumn::Probe(1), OutputColumn::Build(1)],
            Box::new(executor),
            output,
        );

    join.push(crate::pipeline::test_support::dense_view(vec![
        ints(&[1, 2]),
        Arc::new(StringArray::from(vec![Some("visible"), Some("blocked item")])),
    ]))
        .await.expect("probe rows through the residual predicate");
    join.flush().await.expect("flush left join");

    let chunks = sink.0.lock().expect("collect lock");
    let mut result = chunks
        .iter()
        .flat_map(|chunk| selected_cells(chunk, 0).into_iter().zip(selected_cells(chunk, 1)))
        .collect::<Vec<_>>();
    result.sort_unstable();
    assert_eq!(result, vec![(Some(1), Some(1)), (Some(2), None)]);
}
