//! Infers safe ordered bounds from deterministic filter predicates.
//!
//! Given `a < b` and `b < 10`, the rule adds the implied filter `a < 10`.
//! Original predicates remain in place. The rule handles strict and inclusive
//! comparisons in either operand order and only reasons over direct, same-type
//! column and scalar-literal comparisons. Expressions involving casts,
//! correlated columns, disjunctions, or volatile calls are left untouched.

use std::collections::{HashMap, HashSet, VecDeque};

use error::Result;
use parser::ast::expr::BinaryOp;
use plan::{ColumnId, LogicalExpr, LogicalLiteral, LogicalPlan, ResolvedColumnRef, VeinDataType};
use types::literal_type;

use crate::context::OptimizerContext;
use crate::rule::{OptimizerRule, RuleType};

/// Adds filters entailed by transitive ordered comparisons.
pub struct FilterTransitivityRule;

impl OptimizerRule for FilterTransitivityRule {
    fn name(&self) -> &str {
        "FilterTransitivityRule"
    }

    fn rule_type(&self) -> RuleType {
        RuleType::Heuristic
    }

    fn apply(
        &self,
        plan: &LogicalPlan,
        _context: &OptimizerContext,
    ) -> Result<Option<LogicalPlan>> {
        match plan {
            LogicalPlan::Filter { .. } => {
                let Some(predicates) = filter_chain_predicates(plan) else {
                    return Ok(None);
                };
                let inferred = infer_implied_bounds(&predicates);
                if inferred.is_empty() {
                    return Ok(None);
                }

                tracing::debug!(
                    target: "vein.optimizer",
                    event = "optimizer_ordered_filters_inferred",
                    input_predicate_count = predicates.len(),
                    inferred_filter_count = inferred.len(),
                    inferred_filters = ?inferred,
                    "optimizer_ordered_filters_inferred"
                );

                let mut rewritten = plan.clone();
                for predicate in inferred {
                    rewritten = LogicalPlan::Filter {
                        input: Box::new(rewritten),
                        predicate,
                    };
                }
                Ok(Some(rewritten))
            }
            LogicalPlan::Scan {
                table,
                projection,
                filters,
                schema,
            } => {
                if filters.iter().any(LogicalExpr::is_volatile) {
                    return Ok(None);
                }
                let inferred = infer_implied_bounds(filters);
                if inferred.is_empty() {
                    return Ok(None);
                }

                tracing::debug!(
                    target: "vein.optimizer",
                    event = "optimizer_ordered_filters_inferred",
                    table = %table.name,
                    input_predicate_count = filters.len(),
                    inferred_filter_count = inferred.len(),
                    inferred_filters = ?inferred,
                    "optimizer_ordered_filters_inferred"
                );

                let mut rewritten_filters = filters.clone();
                rewritten_filters.extend(inferred);
                Ok(Some(LogicalPlan::Scan {
                    table: table.clone(),
                    projection: projection.clone(),
                    filters: rewritten_filters,
                    schema: schema.clone(),
                }))
            }
            _ => Ok(None),
        }
    }
}

/// Collects deterministic predicates in adjacent filters and a directly
/// underlying scan. Crossing another relational operator would cross a scope
/// or operator boundary, so collection stops there.
fn filter_chain_predicates(plan: &LogicalPlan) -> Option<Vec<LogicalExpr>> {
    let mut predicates = Vec::new();
    let mut current = plan;
    while let LogicalPlan::Filter { input, predicate } = current {
        if predicate.is_volatile() || !append_conjuncts(predicate, &mut predicates) {
            return None;
        }
        current = input;
    }

    if let LogicalPlan::Scan { filters, .. } = current {
        for predicate in filters {
            if predicate.is_volatile() || !append_conjuncts(predicate, &mut predicates) {
                return None;
            }
        }
    }
    Some(predicates)
}

/// Appends deterministic AND terms, returning false for volatile expressions.
fn append_conjuncts(expression: &LogicalExpr, output: &mut Vec<LogicalExpr>) -> bool {
    if expression.is_volatile() {
        return false;
    }
    match expression {
        LogicalExpr::BinaryOp {
            left,
            op: BinaryOp::And,
            right,
            ..
        } => append_conjuncts(left, output) && append_conjuncts(right, output),
        _ => {
            output.push(expression.clone());
            true
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BoundSide {
    Upper,
    Lower,
}

#[derive(Debug, Clone)]
struct LiteralBound {
    column: ResolvedColumnRef,
    value: LogicalLiteral,
    side: BoundSide,
    strict: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ColumnRelation {
    lower: ColumnId,
    upper: ColumnId,
    strict: bool,
}

#[derive(Default)]
struct PredicateFacts {
    columns: HashMap<ColumnId, ResolvedColumnRef>,
    relations: Vec<ColumnRelation>,
    bounds: Vec<LiteralBound>,
}

/// Builds the supported comparison graph and derives column-to-literal bounds.
fn infer_implied_bounds(predicates: &[LogicalExpr]) -> Vec<LogicalExpr> {
    let mut facts = PredicateFacts::default();
    for predicate in predicates {
        if !append_facts(predicate, &mut facts) {
            return Vec::new();
        }
    }

    let graph = RelationGraph::new(&facts.relations);
    let mut candidates = Vec::new();
    for bound in &facts.bounds {
        let traversal = match bound.side {
            // If `column <= literal`, every predecessor in the relation graph
            // receives the same upper bound.
            BoundSide::Upper => graph.reachable(bound.column.id, Traversal::Predecessors),
            // If `literal <= column`, every successor receives that lower bound.
            BoundSide::Lower => graph.reachable(bound.column.id, Traversal::Successors),
        };

        for (column_id, relation_is_strict) in traversal {
            if column_id == bound.column.id {
                continue;
            }
            let Some(column) = facts.columns.get(&column_id) else {
                continue;
            };
            let candidate = LiteralBound {
                column: column.clone(),
                value: bound.value.clone(),
                side: bound.side,
                strict: bound.strict || relation_is_strict,
            };
            retain_strongest_candidate(&mut candidates, candidate);
        }
    }

    candidates
        .into_iter()
        .filter(|candidate| {
            !facts
                .bounds
                .iter()
                .any(|bound| bound_implies(bound, candidate))
        })
        .map(bound_expression)
        .collect()
}

/// Adds one direct ordered comparison to the transitivity facts.
///
/// `false` means the expression is volatile and the entire derivation must be
/// abandoned. Unsupported deterministic expressions are safely ignored.
fn append_facts(expression: &LogicalExpr, facts: &mut PredicateFacts) -> bool {
    if expression.is_volatile() {
        return false;
    }
    if let LogicalExpr::BinaryOp {
        left,
        op: BinaryOp::And,
        right,
        ..
    } = expression
    {
        return append_facts(left, facts) && append_facts(right, facts);
    }

    let LogicalExpr::BinaryOp {
        left, op, right, ..
    } = expression
    else {
        return true;
    };

    match (left.as_ref(), right.as_ref()) {
        (LogicalExpr::Column(left_column), LogicalExpr::Column(right_column)) => {
            if left_column.data_type != right_column.data_type {
                return true;
            }
            let relation = match op {
                BinaryOp::Lt => Some(ColumnRelation {
                    lower: left_column.id,
                    upper: right_column.id,
                    strict: true,
                }),
                BinaryOp::Leq => Some(ColumnRelation {
                    lower: left_column.id,
                    upper: right_column.id,
                    strict: false,
                }),
                BinaryOp::Gt => Some(ColumnRelation {
                    lower: right_column.id,
                    upper: left_column.id,
                    strict: true,
                }),
                BinaryOp::Geq => Some(ColumnRelation {
                    lower: right_column.id,
                    upper: left_column.id,
                    strict: false,
                }),
                _ => None,
            };
            if let Some(relation) = relation {
                facts.columns.insert(left_column.id, left_column.clone());
                facts.columns.insert(right_column.id, right_column.clone());
                if relation.lower != relation.upper && !facts.relations.contains(&relation) {
                    facts.relations.push(relation);
                }
            }
        }
        (LogicalExpr::Column(column), LogicalExpr::Literal(value)) => {
            add_literal_bound(column, value, *op, false, facts);
        }
        (LogicalExpr::Literal(value), LogicalExpr::Column(column)) => {
            add_literal_bound(column, value, *op, true, facts);
        }
        // Outer references, casts, function calls, and compound expressions
        // are not graph terms; keeping them out avoids changing scope or cast
        // semantics during implication reasoning.
        _ => {}
    }
    true
}

fn add_literal_bound(
    column: &ResolvedColumnRef,
    value: &LogicalLiteral,
    op: BinaryOp,
    literal_is_left: bool,
    facts: &mut PredicateFacts,
) {
    if matches!(value, LogicalLiteral::Null(_))
        || literal_type(value) != column.data_type
        || value.try_cmp(value).is_none()
    {
        return;
    }

    let normalized = if literal_is_left {
        reverse_comparison(op)
    } else {
        Some(op)
    };
    let Some(normalized) = normalized else {
        return;
    };
    let (side, strict) = match normalized {
        BinaryOp::Lt => (BoundSide::Upper, true),
        BinaryOp::Leq => (BoundSide::Upper, false),
        BinaryOp::Gt => (BoundSide::Lower, true),
        BinaryOp::Geq => (BoundSide::Lower, false),
        _ => return,
    };

    facts.columns.insert(column.id, column.clone());
    let bound = LiteralBound {
        column: column.clone(),
        value: value.clone(),
        side,
        strict,
    };
    if !facts.bounds.iter().any(|existing| {
        existing.column.id == bound.column.id
            && existing.side == bound.side
            && existing.value == bound.value
            && (existing.strict || !bound.strict)
    }) {
        facts.bounds.push(bound);
    }
}

fn reverse_comparison(op: BinaryOp) -> Option<BinaryOp> {
    match op {
        BinaryOp::Lt => Some(BinaryOp::Gt),
        BinaryOp::Leq => Some(BinaryOp::Geq),
        BinaryOp::Gt => Some(BinaryOp::Lt),
        BinaryOp::Geq => Some(BinaryOp::Leq),
        _ => None,
    }
}

#[derive(Default)]
struct RelationGraph {
    successors: HashMap<ColumnId, Vec<(ColumnId, bool)>>,
    predecessors: HashMap<ColumnId, Vec<(ColumnId, bool)>>,
}

impl RelationGraph {
    fn new(relations: &[ColumnRelation]) -> Self {
        let mut graph = Self::default();
        for relation in relations {
            graph
                .successors
                .entry(relation.lower)
                .or_default()
                .push((relation.upper, relation.strict));
            graph
                .predecessors
                .entry(relation.upper)
                .or_default()
                .push((relation.lower, relation.strict));
        }
        graph
    }

    fn reachable(&self, start: ColumnId, traversal: Traversal) -> HashMap<ColumnId, bool> {
        let adjacency = match traversal {
            Traversal::Successors => &self.successors,
            Traversal::Predecessors => &self.predecessors,
        };
        let mut reached = HashMap::new();
        let mut visited = HashSet::new();
        let mut pending = VecDeque::from([(start, false)]);
        visited.insert((start, false));
        reached.insert(start, false);

        while let Some((column_id, path_is_strict)) = pending.pop_front() {
            let Some(neighbors) = adjacency.get(&column_id) else {
                continue;
            };
            for &(neighbor, edge_is_strict) in neighbors {
                let is_strict = path_is_strict || edge_is_strict;
                reached
                    .entry(neighbor)
                    .and_modify(|reached_strictly| *reached_strictly |= is_strict)
                    .or_insert(is_strict);
                if visited.insert((neighbor, is_strict)) {
                    pending.push_back((neighbor, is_strict));
                }
            }
        }
        reached
    }
}

#[derive(Clone, Copy)]
enum Traversal {
    Successors,
    Predecessors,
}

/// Returns true when `stronger` entails `weaker` for the same column bound.
fn bound_implies(stronger: &LiteralBound, weaker: &LiteralBound) -> bool {
    if stronger.column.id != weaker.column.id || stronger.side != weaker.side {
        return false;
    }
    let ordering = stronger.value.try_cmp(&weaker.value);
    match (stronger.side, ordering) {
        (BoundSide::Upper, Some(std::cmp::Ordering::Less))
        | (BoundSide::Lower, Some(std::cmp::Ordering::Greater)) => true,
        (_, Some(std::cmp::Ordering::Equal)) | (_, None) if stronger.value == weaker.value => {
            stronger.strict || !weaker.strict
        }
        _ => false,
    }
}

fn retain_strongest_candidate(candidates: &mut Vec<LiteralBound>, candidate: LiteralBound) {
    for existing in candidates.iter_mut().filter(|existing| {
        existing.column.id == candidate.column.id && existing.side == candidate.side
    }) {
        if bound_implies(existing, &candidate) {
            return;
        }
        if bound_implies(&candidate, existing) {
            *existing = candidate;
            return;
        }
    }
    candidates.push(candidate);
}

fn bound_expression(bound: LiteralBound) -> LogicalExpr {
    let op = match (bound.side, bound.strict) {
        (BoundSide::Upper, true) => BinaryOp::Lt,
        (BoundSide::Upper, false) => BinaryOp::Leq,
        (BoundSide::Lower, true) => BinaryOp::Gt,
        (BoundSide::Lower, false) => BinaryOp::Geq,
    };
    LogicalExpr::BinaryOp {
        left: Box::new(LogicalExpr::Column(bound.column)),
        op,
        right: Box::new(LogicalExpr::Literal(bound.value)),
        return_type: VeinDataType::Boolean,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Optimizer;
    use plan::{ResolvedColumn, TableMetadata, VeinSchema};

    fn field(id: usize, name: &str, data_type: VeinDataType) -> ResolvedColumnRef {
        std::sync::Arc::new(ResolvedColumn::new(ColumnId(id), name, None, data_type))
    }

    fn column(field: &ResolvedColumnRef) -> LogicalExpr {
        LogicalExpr::Column(field.clone())
    }

    fn literal(value: LogicalLiteral) -> LogicalExpr {
        LogicalExpr::Literal(value)
    }

    fn comparison(left: LogicalExpr, op: BinaryOp, right: LogicalExpr) -> LogicalExpr {
        LogicalExpr::BinaryOp {
            left: Box::new(left),
            op,
            right: Box::new(right),
            return_type: VeinDataType::Boolean,
        }
    }

    fn scan(fields: &[ResolvedColumnRef], filters: Vec<LogicalExpr>) -> LogicalPlan {
        let schema = VeinSchema {
            fields: fields.to_vec(),
        };
        LogicalPlan::Scan {
            table: std::sync::Arc::new(TableMetadata::new(
                "t",
                std::sync::Arc::new(schema.clone()),
            )),
            projection: None,
            filters,
            schema: Box::new(schema),
        }
    }

    fn filter(input: LogicalPlan, predicate: LogicalExpr) -> LogicalPlan {
        LogicalPlan::Filter {
            input: Box::new(input),
            predicate,
        }
    }

    fn scan_filters(plan: &LogicalPlan) -> &[LogicalExpr] {
        let LogicalPlan::Scan { filters, .. } = plan else {
            panic!("expected scan, got {}", plan.operator_name());
        };
        filters
    }

    #[test]
    fn infers_q12_date_bounds_and_preserves_original_predicates() {
        let ship = field(1, "shipdate", VeinDataType::Date32);
        let commit = field(2, "commitdate", VeinDataType::Date32);
        let receipt = field(3, "receiptdate", VeinDataType::Date32);
        let cutoff = literal(LogicalLiteral::Date(9_131));
        let original = vec![
            comparison(column(&ship), BinaryOp::Lt, column(&commit)),
            comparison(column(&commit), BinaryOp::Lt, column(&receipt)),
            comparison(column(&receipt), BinaryOp::Lt, cutoff.clone()),
        ];
        let plan = filter(
            filter(
                filter(
                    scan(&[ship.clone(), commit.clone(), receipt.clone()], vec![]),
                    original[0].clone(),
                ),
                original[1].clone(),
            ),
            original[2].clone(),
        );

        let rewritten = FilterTransitivityRule
            .apply(&plan, &OptimizerContext::default())
            .unwrap()
            .expect("transitive date bounds should be added");
        let Some(predicates) = filter_chain_predicates(&rewritten) else {
            panic!("rewritten filters should remain deterministic");
        };
        assert!(
            original
                .iter()
                .all(|predicate| predicates.contains(predicate))
        );
        assert!(predicates.contains(&comparison(column(&commit), BinaryOp::Lt, cutoff.clone())));
        assert!(predicates.contains(&comparison(column(&ship), BinaryOp::Lt, cutoff)));
        assert_eq!(predicates.len(), 5);
    }

    #[test]
    fn propagates_strictness_in_both_directions_and_operand_orders() {
        let a = field(1, "a", VeinDataType::Int64);
        let b = field(2, "b", VeinDataType::Int64);
        let inclusive_relation = comparison(column(&b), BinaryOp::Geq, column(&a));
        let strict_upper = comparison(column(&b), BinaryOp::Lt, literal(LogicalLiteral::Int64(10)));
        let inferred = infer_implied_bounds(&[inclusive_relation, strict_upper]);
        assert!(inferred.contains(&comparison(
            column(&a),
            BinaryOp::Lt,
            literal(LogicalLiteral::Int64(10))
        )));

        let lower_relation = comparison(column(&a), BinaryOp::Leq, column(&b));
        let lower_bound = comparison(literal(LogicalLiteral::Int64(1)), BinaryOp::Lt, column(&a));
        let inferred = infer_implied_bounds(&[lower_relation, lower_bound]);
        assert!(inferred.contains(&comparison(
            column(&b),
            BinaryOp::Gt,
            literal(LogicalLiteral::Int64(1))
        )));
    }

    #[test]
    fn inclusive_paths_stay_inclusive_unless_a_path_is_strict() {
        let a = field(1, "a", VeinDataType::Int64);
        let b = field(2, "b", VeinDataType::Int64);
        let c = field(3, "c", VeinDataType::Int64);
        let inclusive = infer_implied_bounds(&[
            comparison(column(&a), BinaryOp::Leq, column(&b)),
            comparison(column(&b), BinaryOp::Leq, column(&c)),
            comparison(
                column(&c),
                BinaryOp::Leq,
                literal(LogicalLiteral::Int64(10)),
            ),
        ]);
        assert!(inclusive.contains(&comparison(
            column(&a),
            BinaryOp::Leq,
            literal(LogicalLiteral::Int64(10))
        )));

        let mixed = infer_implied_bounds(&[
            comparison(column(&a), BinaryOp::Leq, column(&b)),
            comparison(column(&b), BinaryOp::Lt, column(&c)),
            comparison(
                column(&c),
                BinaryOp::Leq,
                literal(LogicalLiteral::Int64(10)),
            ),
        ]);
        assert!(mixed.contains(&comparison(
            column(&a),
            BinaryOp::Lt,
            literal(LogicalLiteral::Int64(10))
        )));
    }

    #[test]
    fn scan_predicates_receive_bounds_once() {
        let a = field(1, "a", VeinDataType::Int64);
        let b = field(2, "b", VeinDataType::Int64);
        let predicates = vec![
            comparison(column(&a), BinaryOp::Lt, column(&b)),
            comparison(column(&b), BinaryOp::Lt, literal(LogicalLiteral::Int64(10))),
        ];
        let plan = scan(&[a.clone(), b.clone()], predicates.clone());
        let rewritten = FilterTransitivityRule
            .apply(&plan, &OptimizerContext::default())
            .unwrap()
            .expect("scan filters should gain an implied predicate");
        let expected = comparison(column(&a), BinaryOp::Lt, literal(LogicalLiteral::Int64(10)));
        assert!(scan_filters(&rewritten).contains(&expected));
        assert_eq!(scan_filters(&rewritten).len(), 3);
        assert!(
            FilterTransitivityRule
                .apply(&rewritten, &OptimizerContext::default())
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn production_pipeline_pushes_inferred_bounds_to_the_scan() {
        let ship = field(1, "shipdate", VeinDataType::Date32);
        let commit = field(2, "commitdate", VeinDataType::Date32);
        let receipt = field(3, "receiptdate", VeinDataType::Date32);
        let cutoff = literal(LogicalLiteral::Date(9_131));
        let plan = filter(
            filter(
                filter(
                    scan(&[ship.clone(), commit.clone(), receipt.clone()], vec![]),
                    comparison(column(&ship), BinaryOp::Lt, column(&commit)),
                ),
                comparison(column(&commit), BinaryOp::Lt, column(&receipt)),
            ),
            comparison(column(&receipt), BinaryOp::Lt, cutoff.clone()),
        );

        let optimized = crate::OptimizerPipeline::production()
            .optimize(plan)
            .expect("production optimizer should infer and push date bounds");
        assert!(scan_filters(&optimized).contains(&comparison(
            column(&commit),
            BinaryOp::Lt,
            cutoff.clone()
        )));
        assert!(scan_filters(&optimized).contains(&comparison(
            column(&ship),
            BinaryOp::Lt,
            cutoff
        )));
    }

    #[test]
    fn does_not_infer_across_outer_columns_or_disjunctions() {
        let local_a = field(1, "a", VeinDataType::Int64);
        let local_b = field(2, "b", VeinDataType::Int64);
        let outer = field(3, "outer", VeinDataType::Int64);
        let correlated = comparison(
            column(&local_a),
            BinaryOp::Lt,
            LogicalExpr::OuterColumn(outer),
        );
        let bound = comparison(
            column(&local_a),
            BinaryOp::Lt,
            literal(LogicalLiteral::Int64(10)),
        );
        assert!(infer_implied_bounds(&[correlated, bound.clone()]).is_empty());

        let disjunction = comparison(
            comparison(column(&local_a), BinaryOp::Lt, column(&local_b)),
            BinaryOp::Or,
            LogicalExpr::Literal(LogicalLiteral::Boolean(false)),
        );
        let other_bound = comparison(
            column(&local_b),
            BinaryOp::Lt,
            literal(LogicalLiteral::Int64(10)),
        );
        assert!(infer_implied_bounds(&[disjunction, other_bound]).is_empty());
    }

    #[test]
    fn ignores_incompatible_types_null_bounds_and_volatile_filter_chains() {
        let date = field(1, "date_value", VeinDataType::Date32);
        let number = field(2, "number_value", VeinDataType::Int64);
        let incompatible = comparison(column(&date), BinaryOp::Lt, column(&number));
        let date_bound = comparison(
            column(&date),
            BinaryOp::Lt,
            literal(LogicalLiteral::Date(10)),
        );
        assert!(infer_implied_bounds(&[incompatible, date_bound]).is_empty());

        let null_bound = comparison(
            column(&number),
            BinaryOp::Lt,
            literal(LogicalLiteral::Null(VeinDataType::Int64)),
        );
        let relation = comparison(
            column(&number),
            BinaryOp::Lt,
            column(&field(4, "other", VeinDataType::Int64)),
        );
        assert!(infer_implied_bounds(&[relation, null_bound]).is_empty());

        let float = field(5, "float_value", VeinDataType::Float64);
        let other_float = field(6, "other_float", VeinDataType::Float64);
        let unordered_bound = comparison(
            column(&other_float),
            BinaryOp::Lt,
            literal(LogicalLiteral::Float64(f64::NAN)),
        );
        let float_relation = comparison(column(&float), BinaryOp::Lt, column(&other_float));
        assert!(infer_implied_bounds(&[float_relation, unordered_bound]).is_empty());

        let volatile = LogicalExpr::FunctionCall {
            name: "random".to_string(),
            args: vec![],
            is_aggregate: false,
            distinct: false,
            return_type: VeinDataType::Boolean,
        };
        let chain = filter(
            filter(
                scan(
                    &[number.clone(), field(4, "other", VeinDataType::Int64)],
                    vec![],
                ),
                comparison(
                    column(&number),
                    BinaryOp::Lt,
                    column(&field(4, "other", VeinDataType::Int64)),
                ),
            ),
            volatile,
        );
        assert!(
            FilterTransitivityRule
                .apply(&chain, &OptimizerContext::default())
                .unwrap()
                .is_none()
        );
    }
}
