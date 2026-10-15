use super::*;
use plan::{ColumnId, ResolvedColumn, VeinDataType};

fn scan() -> LogicalPlan {
    let schema = Box::new(plan::VeinSchema { fields: vec![] });
    let table = std::sync::Arc::new(plan::TableMetadata::new(
        "t",
        std::sync::Arc::new(*schema.clone()),
    ));
    LogicalPlan::Scan {
        table,
        projection: None,
        filters: vec![],
        schema,
    }
}

fn bool_column(id: usize, name: &str) -> LogicalExpr {
    LogicalExpr::Column(std::sync::Arc::new(ResolvedColumn {
        id: ColumnId(id),
        name: name.to_string(),
        qualifier: None,
        data_type: VeinDataType::Boolean,
        is_factorized: false,
        nullable: true,
    }))
}

fn and(left: LogicalExpr, right: LogicalExpr) -> LogicalExpr {
    LogicalExpr::BinaryOp {
        left: Box::new(left),
        op: BinaryOp::And,
        right: Box::new(right),
        return_type: VeinDataType::Boolean,
    }
}

fn filter(input: LogicalPlan, predicate: LogicalExpr) -> LogicalPlan {
    LogicalPlan::Filter {
        input: Box::new(input),
        predicate,
    }
}

fn predicates(plan: &LogicalPlan) -> Vec<&LogicalExpr> {
    let mut result = Vec::new();
    let mut current = plan;
    while let LogicalPlan::Filter { input, predicate } = current {
        result.push(predicate);
        current = input;
    }
    result
}

#[test]
fn splits_and_and_removes_true() {
    let a = bool_column(1, "a");
    let b = bool_column(2, "b");
    let plan = filter(
        scan(),
        and(
            a.clone(),
            and(
                LogicalExpr::Literal(LogicalLiteral::Boolean(true)),
                b.clone(),
            ),
        ),
    );

    let normalized = normalize_filter(
        match &plan {
            LogicalPlan::Filter { input, .. } => input,
            _ => unreachable!(),
        },
        match &plan {
            LogicalPlan::Filter { predicate, .. } => predicate,
            _ => unreachable!(),
        },
    );
    // The helper walks from the root toward the input.
    assert_eq!(predicates(&normalized), vec![&b, &a]);
}

#[test]
fn removes_true_filter_and_keeps_false_canonical() {
    assert_eq!(
        normalize_filter(
            &scan(),
            &LogicalExpr::Literal(LogicalLiteral::Boolean(true))
        ),
        scan()
    );
    let normalized = normalize_filter(
        &scan(),
        &and(
            bool_column(1, "a"),
            LogicalExpr::Literal(LogicalLiteral::Boolean(false)),
        ),
    );
    let false_predicate = boolean_literal(false);
    assert_eq!(predicates(&normalized), vec![&false_predicate]);
}

#[test]
fn removes_duplicate_deterministic_conjuncts() {
    let a = bool_column(1, "a");
    let normalized = normalize_filter(&scan(), &and(a.clone(), a.clone()));
    assert_eq!(predicates(&normalized), vec![&a]);
}

#[test]
fn repeated_normalization_preserves_filter_order_and_converges() {
    let a = bool_column(1, "a");
    let b = bool_column(2, "b");
    let c = bool_column(3, "c");
    let plan = filter(filter(filter(scan(), a.clone()), b.clone()), c.clone());

    let normalize = |plan: &LogicalPlan| match plan {
        LogicalPlan::Filter { input, predicate } => normalize_filter(input, predicate),
        _ => unreachable!("fixture has a filter at the root"),
    };
    let once = normalize(&plan);
    let twice = normalize(&once);

    // `predicates` walks from the root toward the input, so its order is
    // the reverse of the evaluation order in the filter chain.
    assert_eq!(predicates(&once), vec![&c, &b, &a]);
    assert_eq!(twice, once);
}

#[test]
fn preserves_volatile_predicate_as_atomic() {
    let random = LogicalExpr::FunctionCall {
        name: "random".to_string(),
        args: vec![],
        is_aggregate: false,
        distinct: false,
        return_type: VeinDataType::Boolean,
    };
    let predicate = and(bool_column(1, "a"), random.clone());
    let normalized = normalize_filter(&scan(), &predicate);
    assert_eq!(predicates(&normalized), vec![&predicate]);
}

#[test]
fn normalizes_scan_filter_list() {
    let a = bool_column(1, "a");
    let b = bool_column(2, "b");
    assert_eq!(
        normalize_filter_list(&[and(a.clone(), b.clone()), a.clone()]),
        vec![a, b]
    );
}
