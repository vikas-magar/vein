use crate::plan::LogicalPlan;
use crate::expr::{LogicalExpr, LogicalLiteral};
use crate::optimizer::rule::{OptimizerRule, RuleType};
use parser::ast::expr::BinaryOp;
use error::Result;

/// A heuristic optimizer rule that evaluates constant mathematical and logical
/// expressions at compile time to save CPU cycles during execution.
///
/// Example: `WHERE age > 10 + 8` -> `WHERE age > 18`
pub struct ConstantFoldingRule;

impl OptimizerRule for ConstantFoldingRule {
    fn name(&self) -> &str {
        "ConstantFoldingRule"
    }

    fn rule_type(&self) -> RuleType {
        RuleType::Heuristic
    }

    fn apply(&self, plan: &LogicalPlan) -> Result<Option<LogicalPlan>> {
        let optimized_plan = self.optimize_node(plan)?;
        
        // In a real framework, we would check if `optimized_plan` is structurally 
        // different from `plan`. If so, return `Some`, else `None`.
        // For simplicity, we assume we always return a new node if changes occurred.
        Ok(Some(optimized_plan))
    }
}

impl ConstantFoldingRule {
    fn optimize_node(&self, plan: &LogicalPlan) -> Result<LogicalPlan> {
        match plan {
            LogicalPlan::Filter { input, predicate } => {
                let new_input = self.optimize_node(input)?;
                let new_predicate = self.optimize_expr(predicate)?;
                Ok(LogicalPlan::Filter {
                    input: Box::new(new_input),
                    predicate: new_predicate,
                })
            }
            LogicalPlan::Project { input, exprs, schema } => {
                let new_input = self.optimize_node(input)?;
                let new_exprs: Result<Vec<_>> = exprs.iter().map(|e| self.optimize_expr(e)).collect();
                Ok(LogicalPlan::Project {
                    input: Box::new(new_input),
                    exprs: new_exprs?,
                    schema: schema.clone(),
                })
            }
            // For other nodes, simply traverse down (omitted for brevity)
            _ => Ok(plan.clone()),
        }
    }

    fn optimize_expr(&self, expr: &LogicalExpr) -> Result<LogicalExpr> {
        match expr {
            LogicalExpr::BinaryOp { left, op, right, return_type } => {
                // Recursively optimize children first
                let opt_left = self.optimize_expr(left)?;
                let opt_right = self.optimize_expr(right)?;

                // If both children are now literals, fold them!
                if let (LogicalExpr::Literal(l_lit), LogicalExpr::Literal(r_lit)) = (&opt_left, &opt_right)
                    && let Some(folded) = Self::fold_literals(l_lit, *op, r_lit) {
                        return Ok(LogicalExpr::Literal(folded));
                    }
                
                // Couldn't fold, return the operation with (potentially) optimized children
                Ok(LogicalExpr::BinaryOp {
                    left: Box::new(opt_left),
                    op: *op,
                    right: Box::new(opt_right),
                    return_type: return_type.clone(),
                })
            }
            _ => Ok(expr.clone()),
        }
    }

    fn fold_literals(left: &LogicalLiteral, op: BinaryOp, right: &LogicalLiteral) -> Option<LogicalLiteral> {
        match (left, op, right) {
            (LogicalLiteral::Int64(l), BinaryOp::Plus, LogicalLiteral::Int64(r)) => {
                Some(LogicalLiteral::Int64(l + r))
            }
            (LogicalLiteral::Int64(l), BinaryOp::Multiply, LogicalLiteral::Int64(r)) => {
                Some(LogicalLiteral::Int64(l * r))
            }
            (LogicalLiteral::Float64(l), BinaryOp::Plus, LogicalLiteral::Float64(r)) => {
                Some(LogicalLiteral::Float64(l + r))
            }
            (LogicalLiteral::Boolean(l), BinaryOp::And, LogicalLiteral::Boolean(r)) => {
                Some(LogicalLiteral::Boolean(*l && *r))
            }
            // ... lots of other combinations
            _ => None, // Cannot fold dynamically yet
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::VeinDataType;

    #[test]
    fn test_constant_folding() {
        let rule = ConstantFoldingRule;
        
        // Expression: (10 + 5) * 2
        let expr = LogicalExpr::BinaryOp {
            left: Box::new(LogicalExpr::BinaryOp {
                left: Box::new(LogicalExpr::Literal(LogicalLiteral::Int64(10))),
                op: BinaryOp::Plus,
                right: Box::new(LogicalExpr::Literal(LogicalLiteral::Int64(5))),
                return_type: VeinDataType::Int64,
            }),
            op: BinaryOp::Multiply,
            right: Box::new(LogicalExpr::Literal(LogicalLiteral::Int64(2))),
            return_type: VeinDataType::Int64,
        };

        let folded = rule.optimize_expr(&expr).unwrap();
        
        assert_eq!(folded, LogicalExpr::Literal(LogicalLiteral::Int64(30)));
    }
}
