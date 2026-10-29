//! Small bag-semantics oracle for the two relational operations being moved.
use super::*;
use std::collections::BTreeMap;

type Row = BTreeMap<usize, Option<i64>>;
pub(super) type Sources = BTreeMap<u128, Vec<Option<i64>>>;

pub(super) fn rows(plan: &LogicalPlan, sources: &Sources) -> Vec<Row> {
    match plan {
        LogicalPlan::Intermediate { source_id, schema, .. } => {
            let column = schema.fields[0].id.0;
            sources[source_id].iter().map(|&v| BTreeMap::from([(column, v)])).collect()
        }
        LogicalPlan::Join { join_type, left, right, on, .. } => {
            let build = rows(right, sources);
            let mut output = Vec::new();
            for probe in rows(left, sources) {
                for build in &build {
                    let mut row = probe.clone();
                    row.extend(build.iter().map(|(&key, &value)| (key, value)));
                    if truth(on.as_ref().unwrap(), &row) {
                        if *join_type == JoinType::Semi {
                            output.push(probe.clone());
                            break;
                        }
                        assert_eq!(*join_type, JoinType::Inner);
                        output.push(row);
                    }
                }
            }
            output.sort();
            output
        }
        _ => panic!("oracle supports only tested relational operations"),
    }
}

fn truth(expr: &LogicalExpr, row: &Row) -> bool {
    let LogicalExpr::BinaryOp { left, op, right, .. } = expr else { panic!("binary predicate") };
    if *op == BinaryOp::And {
        return truth(left, row) && truth(right, row);
    }
    let value = |expr: &LogicalExpr| {
        let LogicalExpr::Column(column) = expr else { panic!("column") };
        row[&column.id.0]
    };
    match (value(left), value(right)) {
        (Some(left), Some(right)) => match op {
            BinaryOp::Eq => left == right,
            BinaryOp::Lt => left < right,
            BinaryOp::Leq => left <= right,
            _ => panic!("unsupported oracle comparison"),
        },
        _ => false,
    }
}
