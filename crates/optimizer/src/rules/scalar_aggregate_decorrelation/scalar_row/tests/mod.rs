//! Hoisting must not change how often a side-effecting relation executes.
use super::*;
use plan::ColumnId;

#[test]
fn correlated_scalar_does_not_hoist_writes_or_effectful_calls() {
    let inner = Arc::new(ResolvedColumn::new(ColumnId(1), "inner_key", None, VeinDataType::Int64));
    let outer = Arc::new(ResolvedColumn::new(ColumnId(2), "outer_key", None, VeinDataType::Int64));
    let schema = Box::new(VeinSchema::new(vec![Arc::clone(&inner)]));
    let source = LogicalPlan::Intermediate { source_id: 1, row_count: 2, schema: schema.clone() };
    let effectful = [
        LogicalPlan::Write {
            input: Box::new(source.clone()), target: "test://sink".into(),
            options: Vec::new(), schema: schema.clone(),
        },
        LogicalPlan::Call {
            input: Box::new(source), name: "effectful".into(),
            args: Vec::new(), schema: schema.clone(),
        },
    ];
    for base in effectful {
        let subquery = LogicalPlan::Project {
            input: Box::new(LogicalPlan::Filter {
                input: Box::new(base),
                predicate: LogicalExpr::BinaryOp {
                    left: Box::new(LogicalExpr::Column(Arc::clone(&inner))),
                    op: BinaryOp::Lt,
                    right: Box::new(LogicalExpr::OuterColumn(Arc::clone(&outer))),
                    return_type: VeinDataType::Boolean,
                },
            }),
            exprs: vec![LogicalExpr::Column(Arc::clone(&inner))],
            schema: schema.clone(),
        };
        assert!(decorrelate(&subquery, &VeinSchema::new(vec![Arc::clone(&outer)])).is_none());
    }
}
