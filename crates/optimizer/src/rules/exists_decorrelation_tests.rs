use std::sync::Arc;

use parser::ast::expr::BinaryOp;
use parser::ast::pipe_op::JoinType;
use plan::{
    ColumnId, LogicalExpr, LogicalPlan, PlanTraversal, ResolvedColumn, TableMetadata, VeinDataType,
    VeinSchema,
};

use super::ExistsDecorrelationRule;
use crate::context::OptimizerContext;
use crate::rule::OptimizerRule;

fn column(id: usize, name: &str, relation: &str) -> Arc<ResolvedColumn> {
    Arc::new(ResolvedColumn::new(
        ColumnId(id),
        name,
        Some(relation.to_string()),
        VeinDataType::Int64,
    ))
}

fn scan(name: &str, columns: Vec<Arc<ResolvedColumn>>) -> LogicalPlan {
    let schema = VeinSchema::new(columns);
    LogicalPlan::Scan {
        table: Arc::new(TableMetadata::new(name, Arc::new(schema.clone()))),
        projection: None,
        filters: Vec::new(),
        schema: Box::new(schema),
    }
}

fn compare(left: LogicalExpr, op: BinaryOp, right: LogicalExpr) -> LogicalExpr {
    LogicalExpr::BinaryOp {
        left: Box::new(left),
        op,
        right: Box::new(right),
        return_type: VeinDataType::Boolean,
    }
}

fn contains_outer(expr: &LogicalExpr) -> bool {
    matches!(expr, LogicalExpr::OuterColumn(_))
        || expr.children().into_iter().any(contains_outer)
}

fn conjuncts<'a>(expr: &'a LogicalExpr, terms: &mut Vec<&'a LogicalExpr>) {
    if let LogicalExpr::BinaryOp {
        left,
        op: BinaryOp::And,
        right,
        ..
    } = expr
    {
        conjuncts(left, terms);
        conjuncts(right, terms);
    } else {
        terms.push(expr);
    }
}

#[test]
fn equality_key_and_inequality_residual_lower_for_exists_and_not_exists() {
    for (negated, expected_type) in [(false, JoinType::Semi), (true, JoinType::Anti)] {
        let outer_order = column(1, "o_orderkey", "orders");
        let outer_supplier = column(2, "l_suppkey", "lineitem");
        let inner_order = column(3, "l_orderkey", "lineitem");
        let inner_supplier = column(4, "l2_suppkey", "lineitem");
        let outer = scan(
            "orders",
            vec![Arc::clone(&outer_order), Arc::clone(&outer_supplier)],
        );
        let inner = scan(
            "lineitem",
            vec![Arc::clone(&inner_order), Arc::clone(&inner_supplier)],
        );
        let correlation = compare(
            LogicalExpr::Column(Arc::clone(&inner_order)),
            BinaryOp::Eq,
            LogicalExpr::OuterColumn(Arc::clone(&outer_order)),
        );
        let inequality = compare(
            LogicalExpr::Column(inner_supplier),
            BinaryOp::Neq,
            LogicalExpr::OuterColumn(outer_supplier),
        );
        let predicate = compare(correlation, BinaryOp::And, inequality);
        let plan = LogicalPlan::Filter {
            input: Box::new(outer),
            predicate: LogicalExpr::Exists {
                subquery: Box::new(LogicalPlan::Filter {
                    input: Box::new(inner),
                    predicate,
                }),
                negated,
            },
        };

        let rewritten = ExistsDecorrelationRule
            .apply(&plan, &OptimizerContext::default())
            .expect("decorrelation should succeed")
            .expect("EXISTS should lower to a join");
        let LogicalPlan::Join {
            join_type,
            left,
            on: Some(on),
            ..
        } = rewritten
        else {
            panic!("expected semi/anti join");
        };

        assert_eq!(join_type, expected_type);
        assert_eq!(left.as_ref().schema().fields.len(), 2);
        let mut terms = Vec::new();
        conjuncts(&on, &mut terms);
        assert_eq!(terms.len(), 2, "retain one hash key and one residual");
        assert!(terms.iter().any(|term| matches!(
            term,
            LogicalExpr::BinaryOp { op: BinaryOp::Eq, .. }
        )));
        assert!(terms.iter().any(|term| matches!(
            term,
            LogicalExpr::BinaryOp { op: BinaryOp::Neq, .. }
        )));
        assert!(!contains_outer(&on), "outer refs must resolve to join inputs");
    }
}

#[test]
fn duplicate_correlated_equality_keys_are_emitted_once() {
    let outer_key = column(80, "o_orderkey", "orders");
    let inner_key = column(81, "l_orderkey", "lineitem");
    let outer = scan("orders", vec![Arc::clone(&outer_key)]);
    let inner = scan("lineitem", vec![Arc::clone(&inner_key)]);
    let correlation = || {
        compare(
            LogicalExpr::Column(Arc::clone(&inner_key)),
            BinaryOp::Eq,
            LogicalExpr::OuterColumn(Arc::clone(&outer_key)),
        )
    };
    let predicate = compare(correlation(), BinaryOp::And, correlation());
    let filter = LogicalPlan::Filter {
        input: Box::new(outer),
        predicate: LogicalExpr::Exists {
            subquery: Box::new(LogicalPlan::Filter {
                input: Box::new(inner),
                predicate,
            }),
            negated: false,
        },
    };

    let rewritten = ExistsDecorrelationRule
        .apply(&filter, &OptimizerContext::default())
        .expect("duplicate equality keys are valid")
        .expect("EXISTS should lower to a semi join");
    let LogicalPlan::Join { on: Some(on), .. } = rewritten else {
        panic!("expected semi join with a key predicate");
    };
    let mut terms = Vec::new();
    conjuncts(&on, &mut terms);
    assert_eq!(
        terms
            .iter()
            .filter(|term| matches!(term, LogicalExpr::BinaryOp { op: BinaryOp::Eq, .. }))
            .count(),
        1,
        "the same correlation pair must not create duplicate join keys"
    );
}
