use std::sync::Arc;

use parser::ast::expr::BinaryOp;
use parser::ast::pipe_op::JoinType;
use plan::{
    ColumnId, LogicalExpr, LogicalLiteral, LogicalPlan, ResolvedColumn, TableMetadata,
    VeinDataType, VeinSchema,
};

use crate::context::OptimizerContext;
use crate::rule::OptimizerRule;
use crate::rules::ExistsDecorrelationRule;

fn field(id: usize, name: &str, table: &str, data_type: VeinDataType) -> Arc<ResolvedColumn> {
    Arc::new(ResolvedColumn::new(
        ColumnId(id),
        name,
        Some(table.to_string()),
        data_type,
    ))
}

fn scan(name: &str, fields: Vec<Arc<ResolvedColumn>>) -> LogicalPlan {
    let schema = VeinSchema::new(fields);
    LogicalPlan::Scan {
        table: Arc::new(TableMetadata::new(name, Arc::new(schema.clone()))),
        projection: None,
        filters: Vec::new(),
        schema: Box::new(schema),
    }
}

fn binary(left: LogicalExpr, op: BinaryOp, right: LogicalExpr) -> LogicalExpr {
    LogicalExpr::BinaryOp {
        left: Box::new(left),
        op,
        right: Box::new(right),
        return_type: VeinDataType::Boolean,
    }
}

fn avg_subquery(volatile: bool) -> LogicalPlan {
    let value = field(4, "c_acctbal", "avg_customer", VeinDataType::Int64);
    let scan = scan("avg_customer", vec![Arc::clone(&value)]);
    let input = if volatile {
        LogicalPlan::Filter {
            input: Box::new(scan),
            predicate: binary(
                LogicalExpr::FunctionCall {
                    name: "random".to_string(),
                    args: Vec::new(),
                    is_aggregate: false,
                    distinct: false,
                    return_type: VeinDataType::Float64,
                },
                BinaryOp::Gt,
                LogicalExpr::Literal(LogicalLiteral::Float64(0.5)),
            ),
        }
    } else {
        scan
    };
    let average = field(5, "avg_acctbal", "", VeinDataType::Float64);
    LogicalPlan::Aggregate {
        input: Box::new(input),
        grouping_sets: vec![Vec::new()],
        aggs: vec![LogicalExpr::FunctionCall {
            name: "avg".to_string(),
            args: vec![LogicalExpr::Column(value)],
            is_aggregate: true,
            distinct: false,
            return_type: VeinDataType::Float64,
        }],
        schema: Box::new(VeinSchema::new(vec![average])),
    }
}

fn q22_filter(volatile_avg: bool) -> LogicalPlan {
    let customer_key = field(1, "c_custkey", "customer", VeinDataType::Int64);
    let account_balance = field(2, "c_acctbal", "customer", VeinDataType::Int64);
    let order_customer = field(3, "o_custkey", "orders", VeinDataType::Int64);
    let outer = scan(
        "customer",
        vec![Arc::clone(&customer_key), Arc::clone(&account_balance)],
    );
    let not_exists = LogicalExpr::Exists {
        subquery: Box::new(LogicalPlan::Filter {
            input: Box::new(scan("orders", vec![Arc::clone(&order_customer)])),
            predicate: binary(
                LogicalExpr::Column(order_customer),
                BinaryOp::Eq,
                LogicalExpr::OuterColumn(customer_key),
            ),
        }),
        negated: true,
    };
    let above_average = binary(
        LogicalExpr::Column(account_balance),
        BinaryOp::Gt,
        LogicalExpr::ScalarSubquery(Box::new(avg_subquery(volatile_avg))),
    );
    LogicalPlan::Filter {
        input: Box::new(outer),
        predicate: binary(above_average, BinaryOp::And, not_exists),
    }
}

#[test]
fn deterministic_scalar_aggregate_residual_allows_q22_exists_decorrelation() {
    let rewritten = ExistsDecorrelationRule
        .apply(&q22_filter(false), &OptimizerContext::default())
        .expect("deterministic AVG subquery can be reordered")
        .expect("NOT EXISTS should decorrelate");
    let LogicalPlan::Filter { input, predicate } = rewritten else {
        panic!("the scalar average predicate must remain above the join");
    };
    assert!(matches!(input.as_ref(), LogicalPlan::Join { join_type: JoinType::Anti, .. }));
    assert!(matches!(predicate, LogicalExpr::BinaryOp { right, op: BinaryOp::Gt, .. }
        if matches!(right.as_ref(), LogicalExpr::ScalarSubquery(_))));
}

#[test]
fn volatile_work_inside_scalar_subquery_still_blocks_exists_reordering() {
    let error = ExistsDecorrelationRule
        .apply(&q22_filter(true), &OptimizerContext::default())
        .expect_err("volatile scalar subquery must not move across NOT EXISTS");
    assert!(error.to_string().contains("volatile expressions"));
}
