//! Pushes safe logical filters into physical table-scan filter lists.

use error::Result;
use plan::LogicalPlan;

use crate::context::OptimizerContext;
use crate::rule::{OptimizerRule, RuleType};

use super::predicate_utils::referenced_columns;

/// A heuristic rule that lowers safe filters into a scan's filter list.
///
/// The scan retains the logical predicate in its `filters` list. Physical
/// planners can use that list both for source-native pruning and for a
/// residual vectorized filter when native pruning is unavailable.
pub struct ScanFilterPushdownRule;

impl OptimizerRule for ScanFilterPushdownRule {
    fn name(&self) -> &str {
        "ScanFilterPushdownRule"
    }

    fn rule_type(&self) -> RuleType {
        RuleType::Heuristic
    }

    fn apply(
        &self,
        plan: &LogicalPlan,
        _context: &OptimizerContext,
    ) -> Result<Option<LogicalPlan>> {
        let LogicalPlan::Filter { input, predicate } = plan else {
            return Ok(None);
        };

        if predicate.is_volatile() {
            return Ok(None);
        }

        let LogicalPlan::Scan {
            table,
            projection,
            filters,
            schema,
        } = input.as_ref()
        else {
            return Ok(None);
        };

        let Some(referenced) = referenced_columns(predicate) else {
            return Ok(None);
        };
        if !referenced
            .iter()
            .all(|column| schema.fields.iter().any(|field| field.id == *column))
        {
            return Ok(None);
        }

        let mut scan_filters = filters.clone();
        if !scan_filters.contains(predicate) {
            scan_filters.push(predicate.clone());
            tracing::info!(
                target: "vein.optimizer",
                event = "optimizer_scan_filter_pushed",
                table = %table.name,
                filter = ?predicate,
                referenced_columns = ?referenced,
                scan_filter_count = scan_filters.len(),
                "optimizer_scan_filter_pushed"
            );
        }

        Ok(Some(LogicalPlan::Scan {
            table: table.clone(),
            projection: projection.clone(),
            filters: scan_filters,
            schema: schema.clone(),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use parser::ast::expr::BinaryOp;
    use plan::{
        ColumnId, LogicalExpr, LogicalLiteral, ResolvedColumn, ResolvedColumnRef, TableMetadata,
        VeinDataType, VeinSchema,
    };

    fn field(id: usize, name: &str) -> ResolvedColumnRef {
        std::sync::Arc::new(ResolvedColumn::new(ColumnId(id), name, None, VeinDataType::Int64))
    }

    fn scan(field: ResolvedColumnRef) -> LogicalPlan {
        let schema = VeinSchema {
            fields: vec![field.clone()],
        };
        LogicalPlan::Scan {
            table: std::sync::Arc::new(TableMetadata::new(
                "t",
                std::sync::Arc::new(schema.clone()),
            )),
            projection: None,
            filters: vec![],
            schema: Box::new(schema),
        }
    }

    fn predicate(field: &ResolvedColumnRef) -> LogicalExpr {
        LogicalExpr::BinaryOp {
            left: Box::new(LogicalExpr::Column(field.clone())),
            op: BinaryOp::Gt,
            right: Box::new(LogicalExpr::Literal(LogicalLiteral::Int64(0))),
            return_type: VeinDataType::Boolean,
        }
    }

    #[test]
    fn pushes_filter_into_scan_filters() {
        let id = field(1, "id");
        let plan = LogicalPlan::Filter {
            input: Box::new(scan(id.clone())),
            predicate: predicate(&id),
        };

        let rewritten = ScanFilterPushdownRule
            .apply(&plan, &OptimizerContext::default())
            .unwrap()
            .expect("filter should reach scan");

        let LogicalPlan::Scan { filters, .. } = rewritten else {
            panic!("expected a scan");
        };
        assert_eq!(filters.len(), 1);
        assert_eq!(filters[0], predicate(&id));
    }

    #[test]
    fn does_not_push_filter_with_unknown_column() {
        let id = field(1, "id");
        let other = field(2, "other");
        let plan = LogicalPlan::Filter {
            input: Box::new(scan(id)),
            predicate: predicate(&other),
        };

        assert!(
            ScanFilterPushdownRule
                .apply(&plan, &OptimizerContext::default())
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn does_not_duplicate_existing_scan_filter() {
        let id = field(1, "id");
        let filter = predicate(&id);
        let mut input = scan(id);
        let LogicalPlan::Scan { filters, .. } = &mut input else {
            unreachable!();
        };
        filters.push(filter.clone());

        let plan = LogicalPlan::Filter {
            input: Box::new(input),
            predicate: filter,
        };
        let rewritten = ScanFilterPushdownRule
            .apply(&plan, &OptimizerContext::default())
            .unwrap()
            .expect("duplicate filter should be absorbed");

        let LogicalPlan::Scan { filters, .. } = rewritten else {
            panic!("expected a scan");
        };
        assert_eq!(filters.len(), 1);
    }
}
