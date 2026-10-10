//! Decorrelation of equality-correlated EXISTS predicates into semi/anti joins.
//!
//! Keeping this rewrite in the optimizer makes existence checks set based: the
//! inner relation is built once and each outer row is emitted at most once.
use error::{Result, VeinError};
use parser::ast::expr::BinaryOp;
use parser::ast::pipe_op::JoinType;
use plan::{LogicalExpr, LogicalPlan, PlanTraversal, ResolvedColumnRef, VeinDataType};

use crate::context::{OptimizerContext, OptimizerPhase};
use crate::rule::{OptimizerRule, RuleType};

/// Rewrites supported correlated existence filters into semi/anti joins.
pub struct ExistsDecorrelationRule;

impl OptimizerRule for ExistsDecorrelationRule {
    fn name(&self) -> &str {
        "ExistsDecorrelationRule"
    }

    fn rule_type(&self) -> RuleType {
        RuleType::Heuristic
    }

    fn phase(&self) -> OptimizerPhase {
        OptimizerPhase::Heuristic
    }

    fn apply(&self, plan: &LogicalPlan, _context: &OptimizerContext) -> Result<Option<LogicalPlan>> {
        let LogicalPlan::Filter { input, predicate } = plan else {
            return Ok(None);
        };
        let mut terms = Vec::new();
        split_conjunction(predicate, &mut terms);
        let exists_terms = terms
            .iter()
            .filter_map(|term| match term {
                LogicalExpr::Exists { subquery, negated } => Some((subquery.as_ref(), *negated)),
                _ => None,
            })
            .collect::<Vec<_>>();
        if exists_terms.is_empty() {
            if contains_exists(predicate) {
                return Err(VeinError::planning(
                    "EXISTS decorrelation requires EXISTS to be a top-level conjunct; EXISTS under OR or another expression is not supported",
                    None,
                ));
            }
            return Ok(None);
        }

        let residuals = terms
            .into_iter()
            .filter(|term| !matches!(term, LogicalExpr::Exists { .. }))
            .map(|term| {
                if term.is_volatile() {
                    Err(VeinError::planning(
                        "EXISTS decorrelation cannot move a filter containing volatile expressions across the existence check",
                        None,
                    ))
                } else {
                    Ok(term.clone())
                }
            })
            .collect::<Result<Vec<_>>>()?;

        let mut rewritten = (**input).clone();
        for (subquery, negated) in exists_terms {
            rewritten = decorrelate_one(rewritten, subquery, negated)?;
        }
        let Some(predicate) = make_conjunction(residuals) else {
            return Ok(Some(rewritten));
        };
        Ok(Some(LogicalPlan::Filter {
            input: Box::new(rewritten),
            predicate,
        }))
    }
}

fn decorrelate_one(input: LogicalPlan, subquery: &LogicalPlan, negated: bool) -> Result<LogicalPlan> {
    let mut filters = Vec::new();
    let inner_base = collect_filter_chain(subquery, &mut filters);
    if !matches!(inner_base, LogicalPlan::Scan { .. }) {
        return Err(VeinError::planning(
            "EXISTS decorrelation currently supports filter chains directly over a table scan",
            None,
        ));
    }
    let mut keys = Vec::new();
    let mut local_filters = Vec::new();

    for predicate in &filters {
        let mut terms = Vec::new();
        split_conjunction(&predicate, &mut terms);
        for term in terms {
            if let Some((inner, outer)) = equality_correlation(&term) {
                if inner_base.schema().position_of_id(inner.id).is_none()
                    || input.schema().position_of_id(outer.id).is_none()
                {
                    return Err(VeinError::planning(
                        "correlated EXISTS equality references a column outside its input schema",
                        None,
                    ));
                }
                keys.push((inner, outer));
            } else if contains_outer_column(&term) {
                return Err(VeinError::planning(
                    "correlated EXISTS currently requires outer references to be direct equality keys between inner and outer columns",
                    None,
                ));
            } else {
                if term.is_volatile() {
                    return Err(VeinError::planning(
                        "EXISTS decorrelation cannot move volatile inner predicates into a shared semi/anti join input",
                        None,
                    ));
                }
                local_filters.push(term.clone());
            }
        }
    }

    if keys.is_empty() {
        if filters.iter().any(predicate_contains_outer_column) {
            return Err(VeinError::planning(
                "correlated EXISTS requires at least one inner-to-outer equality key",
                None,
            ));
        }
        return Err(VeinError::planning(
            "uncorrelated EXISTS is not supported by the current semi/anti join lowering",
            None,
        ));
    }

    let mut right = inner_base.clone();
    // Restore filter order from the original chain while keeping only
    // predicates local to the inner relation.
    for predicate in local_filters.into_iter().rev() {
        right = LogicalPlan::Filter {
            input: Box::new(right),
            predicate,
        };
    }

    let key_exprs = keys
        .into_iter()
        .map(|(inner, outer)| LogicalExpr::BinaryOp {
            left: Box::new(LogicalExpr::Column(outer)),
            op: BinaryOp::Eq,
            right: Box::new(LogicalExpr::Column(inner)),
            return_type: VeinDataType::Boolean,
        })
        .collect::<Vec<_>>();
    let mut key_exprs = key_exprs.into_iter();
    let mut on = key_exprs
        .next()
        .ok_or_else(|| VeinError::planning("correlated EXISTS requires an equality key", None))?;
    for right in key_exprs {
        on = LogicalExpr::BinaryOp {
            left: Box::new(on),
            op: BinaryOp::And,
            right: Box::new(right),
            return_type: VeinDataType::Boolean,
        };
    }

    let join = LogicalPlan::Join {
        join_type: if negated { JoinType::Anti } else { JoinType::Semi },
        left: Box::new(input.clone()),
        right: Box::new(right),
        on: Some(on),
        using: Vec::new(),
        // Semi/anti joins preserve only the outer side and its cardinality.
        schema: Box::new(input.schema().clone()),
    };

    tracing::debug!(
            target: "vein.audit",
            event = "exists_decorrelated",
            negated,
            key_count = match &join {
                LogicalPlan::Join { on: Some(on), .. } => conjunction_count(on),
                _ => 0,
            },
            "exists_decorrelated"
        );
    Ok(join)
}

fn make_conjunction(mut terms: Vec<LogicalExpr>) -> Option<LogicalExpr> {
    let mut predicate = terms.drain(..).next()?;
    for term in terms {
        predicate = LogicalExpr::BinaryOp {
            left: Box::new(predicate),
            op: BinaryOp::And,
            right: Box::new(term),
            return_type: VeinDataType::Boolean,
        };
    }
    Some(predicate)
}

fn contains_exists(expr: &LogicalExpr) -> bool {
    matches!(expr, LogicalExpr::Exists { .. }) || expr.children().into_iter().any(contains_exists)
}

/// Returns the first non-filter node below a consecutive filter chain, while
/// collecting each filter predicate from outermost to innermost.
fn collect_filter_chain<'a>(plan: &'a LogicalPlan, predicates: &mut Vec<LogicalExpr>) -> &'a LogicalPlan {
    match plan {
        LogicalPlan::Filter { input, predicate } => {
            predicates.push(predicate.clone());
            collect_filter_chain(input, predicates)
        }
        _ => plan,
    }
}

fn split_conjunction<'a>(expr: &'a LogicalExpr, terms: &mut Vec<&'a LogicalExpr>) {
    if let LogicalExpr::BinaryOp {
        left,
        op: BinaryOp::And,
        right,
        ..
    } = expr
    {
        split_conjunction(left, terms);
        split_conjunction(right, terms);
    } else {
        terms.push(expr);
    }
}

fn equality_correlation(expr: &LogicalExpr) -> Option<(ResolvedColumnRef, ResolvedColumnRef)> {
    let LogicalExpr::BinaryOp {
        left,
        op: BinaryOp::Eq,
        right,
        ..
    } = expr
    else {
        return None;
    };
    match (left.as_ref(), right.as_ref()) {
        (LogicalExpr::Column(inner), LogicalExpr::OuterColumn(outer))
        | (LogicalExpr::OuterColumn(outer), LogicalExpr::Column(inner)) => {
            Some((ResolvedColumnRef::clone(inner), ResolvedColumnRef::clone(outer)))
        }
        _ => None,
    }
}

fn contains_outer_column(expr: &LogicalExpr) -> bool {
    matches!(expr, LogicalExpr::OuterColumn(_))
        || expr.children().into_iter().any(contains_outer_column)
}

fn predicate_contains_outer_column(expr: &LogicalExpr) -> bool {
    contains_outer_column(expr)
}

fn conjunction_count(expr: &LogicalExpr) -> usize {
    let mut terms = Vec::new();
    split_conjunction(expr, &mut terms);
    terms.len()
}

#[cfg(test)]
mod tests {
    use super::*;
    use plan::{ColumnId, LogicalLiteral, ResolvedColumn, TableMetadata, VeinSchema};
    use std::sync::Arc;

    fn field(id: usize, name: &str, table: &str, data_type: VeinDataType) -> ResolvedColumnRef {
        Arc::new(ResolvedColumn::new(
            ColumnId(id),
            name,
            Some(table.to_string()),
            data_type,
        ))
    }

    fn scan(name: &str, fields: Vec<ResolvedColumnRef>) -> LogicalPlan {
        let schema = VeinSchema::new(fields);
        LogicalPlan::Scan {
            table: Arc::new(TableMetadata::new(name, Arc::new(schema.clone()))),
            projection: None,
            filters: Vec::new(),
            schema: Box::new(schema),
        }
    }

    fn eq(left: LogicalExpr, right: LogicalExpr) -> LogicalExpr {
        LogicalExpr::BinaryOp {
            left: Box::new(left),
            op: BinaryOp::Eq,
            right: Box::new(right),
            return_type: VeinDataType::Boolean,
        }
    }

    #[test]
    fn decorrelates_exists_preserving_inner_predicates_and_left_schema() {
        let outer_key = field(1, "o_orderkey", "orders", VeinDataType::Int64);
        let output = field(2, "o_priority", "orders", VeinDataType::Utf8);
        let inner_key = field(3, "l_orderkey", "lineitem", VeinDataType::Int64);
        let ship_date = field(4, "l_shipdate", "lineitem", VeinDataType::Date32);
        let commit_date = field(5, "l_commitdate", "lineitem", VeinDataType::Date32);
        let receipt_date = field(6, "l_receiptdate", "lineitem", VeinDataType::Date32);
        let outer = scan("orders", vec![Arc::clone(&outer_key), Arc::clone(&output)]);
        let inner = scan(
            "lineitem",
            vec![Arc::clone(&inner_key), Arc::clone(&ship_date), Arc::clone(&commit_date), Arc::clone(&receipt_date)],
        );
        let inner_filter = LogicalPlan::Filter {
            input: Box::new(inner),
            predicate: LogicalExpr::BinaryOp {
                left: Box::new(LogicalExpr::Column(Arc::clone(&commit_date))),
                op: BinaryOp::Lt,
                right: Box::new(LogicalExpr::Column(Arc::clone(&receipt_date))),
                return_type: VeinDataType::Boolean,
            },
        };
        let correlated_filter = LogicalPlan::Filter {
            input: Box::new(inner_filter),
            predicate: eq(
                LogicalExpr::Column(Arc::clone(&inner_key)),
                LogicalExpr::OuterColumn(Arc::clone(&outer_key)),
            ),
        };
        let exists = LogicalPlan::Filter {
            input: Box::new(outer),
            predicate: LogicalExpr::Exists {
                subquery: Box::new(correlated_filter),
                negated: false,
            },
        };

        let rewritten = ExistsDecorrelationRule
            .apply(&exists, &OptimizerContext::default())
            .expect("rewrite should succeed")
            .expect("correlated EXISTS should decorrelate");
        let LogicalPlan::Join { join_type, left, right, on, schema, .. } = rewritten else {
            panic!("expected semi join");
        };
        assert_eq!(join_type, JoinType::Semi);
        assert_eq!(schema.fields, left.schema().fields);
        assert!(matches!(on, Some(LogicalExpr::BinaryOp { op: BinaryOp::Eq, .. })));
        let LogicalPlan::Filter { input: right_input, predicate } = right.as_ref() else {
            panic!("inner-only filter must remain on the inner input");
        };
        assert!(matches!(right_input.as_ref(), LogicalPlan::Scan { .. }));
        assert!(matches!(predicate, LogicalExpr::BinaryOp { op: BinaryOp::Lt, .. }));
    }

    #[test]
    fn decorrelates_not_exists_to_anti_join_and_supports_multiple_keys() {
        let outer_key = field(10, "outer_key", "outer", VeinDataType::Int64);
        let outer_tenant = field(11, "outer_tenant", "outer", VeinDataType::Int64);
        let inner_key = field(12, "inner_key", "inner", VeinDataType::Int64);
        let inner_tenant = field(13, "inner_tenant", "inner", VeinDataType::Int64);
        let outer = scan("outer", vec![Arc::clone(&outer_key), Arc::clone(&outer_tenant)]);
        let inner = scan("inner", vec![Arc::clone(&inner_key), Arc::clone(&inner_tenant)]);
        let correlation = LogicalExpr::BinaryOp {
            left: Box::new(eq(
                LogicalExpr::Column(Arc::clone(&inner_key)),
                LogicalExpr::OuterColumn(Arc::clone(&outer_key)),
            )),
            op: BinaryOp::And,
            right: Box::new(eq(
                LogicalExpr::OuterColumn(Arc::clone(&outer_tenant)),
                LogicalExpr::Column(Arc::clone(&inner_tenant)),
            )),
            return_type: VeinDataType::Boolean,
        };
        let exists = LogicalPlan::Filter {
            input: Box::new(outer),
            predicate: LogicalExpr::Exists {
                subquery: Box::new(LogicalPlan::Filter {
                    input: Box::new(inner),
                    predicate: correlation,
                }),
                negated: true,
            },
        };
        let rewritten = ExistsDecorrelationRule
            .apply(&exists, &OptimizerContext::default())
            .expect("rewrite should succeed")
            .expect("correlated NOT EXISTS should decorrelate");
        assert!(matches!(rewritten, LogicalPlan::Join { join_type: JoinType::Anti, on: Some(_), .. }));
    }

    #[test]
    fn extracts_exists_from_top_level_conjunction_and_preserves_other_filter_terms() {
        let outer_key = field(40, "o_orderkey", "orders", VeinDataType::Int64);
        let inner_key = field(41, "l_orderkey", "lineitem", VeinDataType::Int64);
        let outer = scan("orders", vec![Arc::clone(&outer_key)]);
        let inner = scan("lineitem", vec![Arc::clone(&inner_key)]);
        let exists = LogicalExpr::Exists {
            subquery: Box::new(LogicalPlan::Filter {
                input: Box::new(inner),
                predicate: eq(
                    LogicalExpr::Column(inner_key),
                    LogicalExpr::OuterColumn(Arc::clone(&outer_key)),
                ),
            }),
            negated: false,
        };
        let residual = LogicalExpr::BinaryOp {
            left: Box::new(LogicalExpr::Column(outer_key)),
            op: BinaryOp::Gt,
            right: Box::new(LogicalExpr::Literal(LogicalLiteral::Int64(0))),
            return_type: VeinDataType::Boolean,
        };
        let filter = LogicalPlan::Filter {
            input: Box::new(outer),
            predicate: LogicalExpr::BinaryOp {
                left: Box::new(residual),
                op: BinaryOp::And,
                right: Box::new(exists),
                return_type: VeinDataType::Boolean,
            },
        };

        let rewritten = ExistsDecorrelationRule
            .apply(&filter, &OptimizerContext::default())
            .expect("rewrite should succeed")
            .expect("top-level EXISTS conjunct should decorrelate");
        let LogicalPlan::Filter { input, predicate } = rewritten else {
            panic!("the other conjunct must remain as a filter");
        };
        assert!(matches!(predicate, LogicalExpr::BinaryOp { op: BinaryOp::Gt, .. }));
        assert!(matches!(input.as_ref(), LogicalPlan::Join { join_type: JoinType::Semi, .. }));
    }

    #[test]
    fn decorrelates_multiple_top_level_exists_terms_as_sequential_semijoins() {
        let outer_key = field(60, "outer_key", "outer", VeinDataType::Int64);
        let first_key = field(61, "first_key", "first_inner", VeinDataType::Int64);
        let second_key = field(62, "second_key", "second_inner", VeinDataType::Int64);
        let outer = scan("outer", vec![Arc::clone(&outer_key)]);
        let first_exists = LogicalExpr::Exists {
            subquery: Box::new(LogicalPlan::Filter {
                input: Box::new(scan("first_inner", vec![Arc::clone(&first_key)])),
                predicate: eq(
                    LogicalExpr::Column(first_key),
                    LogicalExpr::OuterColumn(Arc::clone(&outer_key)),
                ),
            }),
            negated: false,
        };
        let second_exists = LogicalExpr::Exists {
            subquery: Box::new(LogicalPlan::Filter {
                input: Box::new(scan("second_inner", vec![Arc::clone(&second_key)])),
                predicate: eq(
                    LogicalExpr::Column(second_key),
                    LogicalExpr::OuterColumn(Arc::clone(&outer_key)),
                ),
            }),
            negated: false,
        };
        let filter = LogicalPlan::Filter {
            input: Box::new(outer),
            predicate: LogicalExpr::BinaryOp {
                left: Box::new(first_exists),
                op: BinaryOp::And,
                right: Box::new(second_exists),
                return_type: VeinDataType::Boolean,
            },
        };

        let rewritten = ExistsDecorrelationRule
            .apply(&filter, &OptimizerContext::default())
            .expect("rewrite should succeed")
            .expect("both top-level EXISTS terms should decorrelate");
        let LogicalPlan::Join { join_type: JoinType::Semi, left, .. } = rewritten else {
            panic!("the outermost existence check should be a semi join");
        };
        assert!(matches!(left.as_ref(), LogicalPlan::Join { join_type: JoinType::Semi, .. }));
    }

    #[test]
    fn rejects_volatile_inner_predicates_that_cannot_be_shared_across_outer_rows() {
        let outer_key = field(70, "outer_key", "outer", VeinDataType::Int64);
        let inner_key = field(71, "inner_key", "inner", VeinDataType::Int64);
        let outer = scan("outer", vec![Arc::clone(&outer_key)]);
        let inner = scan("inner", vec![Arc::clone(&inner_key)]);
        let correlation = eq(
            LogicalExpr::Column(inner_key),
            LogicalExpr::OuterColumn(Arc::clone(&outer_key)),
        );
        let random_check = LogicalExpr::BinaryOp {
            left: Box::new(LogicalExpr::FunctionCall {
                name: "random".to_string(),
                args: Vec::new(),
                is_aggregate: false,
                distinct: false,
                return_type: VeinDataType::Float64,
            }),
            op: BinaryOp::Gt,
            right: Box::new(LogicalExpr::Literal(LogicalLiteral::Float64(0.5))),
            return_type: VeinDataType::Boolean,
        };
        let exists = LogicalPlan::Filter {
            input: Box::new(outer),
            predicate: LogicalExpr::Exists {
                subquery: Box::new(LogicalPlan::Filter {
                    input: Box::new(LogicalPlan::Filter {
                        input: Box::new(inner),
                        predicate: random_check,
                    }),
                    predicate: correlation,
                }),
                negated: false,
            },
        };

        let error = ExistsDecorrelationRule
            .apply(&exists, &OptimizerContext::default())
            .expect_err("volatile inner predicates must not become globally shared build filters");
        assert!(error.to_string().contains("volatile inner predicates"));
    }

    #[test]
    fn rejects_exists_nested_under_or_instead_of_leaving_it_for_physical_lowering() {
        let outer_key = field(50, "o_orderkey", "orders", VeinDataType::Int64);
        let inner_key = field(51, "l_orderkey", "lineitem", VeinDataType::Int64);
        let outer = scan("orders", vec![Arc::clone(&outer_key)]);
        let inner = scan("lineitem", vec![Arc::clone(&inner_key)]);
        let exists = LogicalExpr::Exists {
            subquery: Box::new(LogicalPlan::Filter {
                input: Box::new(inner),
                predicate: eq(
                    LogicalExpr::Column(inner_key),
                    LogicalExpr::OuterColumn(Arc::clone(&outer_key)),
                ),
            }),
            negated: false,
        };
        let predicate = LogicalExpr::BinaryOp {
            left: Box::new(LogicalExpr::BinaryOp {
                left: Box::new(LogicalExpr::Column(outer_key)),
                op: BinaryOp::Gt,
                right: Box::new(LogicalExpr::Literal(LogicalLiteral::Int64(0))),
                return_type: VeinDataType::Boolean,
            }),
            op: BinaryOp::Or,
            right: Box::new(exists),
            return_type: VeinDataType::Boolean,
        };
        let filter = LogicalPlan::Filter {
            input: Box::new(outer),
            predicate,
        };

        let error = ExistsDecorrelationRule
            .apply(&filter, &OptimizerContext::default())
            .expect_err("EXISTS nested under OR cannot be transformed as an independent semi join");
        assert!(error.to_string().contains("top-level conjunct"));
    }

    #[test]
    fn rejects_correlated_exists_without_an_equi_key() {
        let outer_key = field(20, "outer_key", "outer", VeinDataType::Int64);
        let inner_key = field(21, "inner_key", "inner", VeinDataType::Int64);
        let outer = scan("outer", vec![Arc::clone(&outer_key)]);
        let inner = scan("inner", vec![Arc::clone(&inner_key)]);
        let exists = LogicalPlan::Filter {
            input: Box::new(outer),
            predicate: LogicalExpr::Exists {
                subquery: Box::new(LogicalPlan::Filter {
                    input: Box::new(inner),
                    predicate: LogicalExpr::BinaryOp {
                        left: Box::new(LogicalExpr::Column(inner_key)),
                        op: BinaryOp::Gt,
                        right: Box::new(LogicalExpr::OuterColumn(outer_key)),
                        return_type: VeinDataType::Boolean,
                    },
                }),
                negated: false,
            },
        };
        let error = ExistsDecorrelationRule
            .apply(&exists, &OptimizerContext::default())
            .expect_err("range correlation must not be silently changed");
        assert!(error.to_string().contains("direct equality keys"));
    }

    #[test]
    fn reports_uncorrelated_exists_as_an_explicit_unsupported_shape() {
        let value = field(30, "value", "inner", VeinDataType::Int64);
        let outer = scan("outer", Vec::new());
        let inner = scan("inner", vec![value]);
        let exists = LogicalPlan::Filter {
            input: Box::new(outer),
            predicate: LogicalExpr::Exists {
                subquery: Box::new(inner),
                negated: false,
            },
        };
        let error = ExistsDecorrelationRule
            .apply(&exists, &OptimizerContext::default())
            .expect_err("uncorrelated EXISTS must not survive as an unlowered expression");
        assert!(error.to_string().contains("uncorrelated EXISTS"));
    }
}
