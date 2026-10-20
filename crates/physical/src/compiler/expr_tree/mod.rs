//! Typed logical-expression lowering for the canonical physical plan.

mod compile;
#[cfg(test)]
mod tests;

use std::collections::HashMap;
use common::{Result, VeinError};
use common::protocol::{BinaryExprOp, ExprKind, ExpressionStep, PhysicalExpr};
use parser::ast::expr::BinaryOp;
use plan::expr::logical::LogicalExpr;
use plan::schema::{ColumnId, VeinDataType, VeinSchema};

/// Lowers logical scalar expressions into the typed execution protocol.
pub struct ExpressionCompiler<'a> {
    schema: &'a VeinSchema,
    columns: HashMap<ColumnId, u32>,
}

impl<'a> ExpressionCompiler<'a> {
    /// Creates a compiler whose column ordinals address `schema` in field order.
    pub fn new(schema: &'a VeinSchema) -> Result<Self> {
        let columns = schema.fields.iter().enumerate()
            .map(|(i, field)| u32::try_from(i)
                .map(|index| (field.id, index))
                .map_err(|_| VeinError::planning("input schema has too many columns", None)))
            .collect::<Result<HashMap<_, _>>>()?;
        Ok(Self { schema, columns })
    }

    /// Compiles a logical predicate into a filter pipeline step.
    pub fn filter(&self, predicate: &LogicalExpr) -> Result<ExpressionStep> {
        Ok(ExpressionStep::Filter(self.compile(predicate)?))
    }

    /// Compiles conjuncts into one typed predicate expression.
    pub fn predicate(&self, predicates: &[LogicalExpr]) -> Result<PhysicalExpr> {
        let mut expressions = predicates.iter().map(|predicate| self.compile(predicate));
        let mut combined = expressions.next()
            .ok_or_else(|| VeinError::planning("scan predicate has no expressions", None))??;
        for expression in expressions {
            combined = PhysicalExpr {
                return_type: VeinDataType::Boolean,
                kind: ExprKind::Binary {
                    op: BinaryExprOp::And,
                    left: Box::new(combined),
                    right: Box::new(expression?),
                },
            };
        }
        Ok(combined)
    }

    /// Compiles projected expressions into a projection pipeline step.
    pub fn projection(&self, expressions: &[LogicalExpr]) -> Result<ExpressionStep> {
        Ok(ExpressionStep::Projection(
            expressions.iter().map(|expression| self.compile(expression))
                .collect::<Result<Vec<_>>>()?,
        ))
    }
}

fn binary_op(op: BinaryOp) -> BinaryExprOp {
    match op {
        BinaryOp::Plus => BinaryExprOp::Add,
        BinaryOp::Minus => BinaryExprOp::Subtract,
        BinaryOp::Multiply => BinaryExprOp::Multiply,
        BinaryOp::Divide => BinaryExprOp::Divide,
        BinaryOp::Modulo => BinaryExprOp::Modulo,
        BinaryOp::Concat => BinaryExprOp::Concat,
        BinaryOp::Eq => BinaryExprOp::Equal,
        BinaryOp::Neq => BinaryExprOp::NotEqual,
        BinaryOp::Lt => BinaryExprOp::Less,
        BinaryOp::Leq => BinaryExprOp::LessEqual,
        BinaryOp::Gt => BinaryExprOp::Greater,
        BinaryOp::Geq => BinaryExprOp::GreaterEqual,
        BinaryOp::And => BinaryExprOp::And,
        BinaryOp::Or => BinaryExprOp::Or,
    }
}

fn unsupported(expression: &LogicalExpr) -> common::VeinError {
    VeinError::unsupported(
        format!("expression kind {} has no physical scalar lowering", expression.kind_name()),
        None,
    )
}
