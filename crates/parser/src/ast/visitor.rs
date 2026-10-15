use crate::ast::expr::Expr;
use crate::parser::ParseError;

/// A visitor that walks every node of an [`Expr`] tree.
pub trait ExprVisitor<'a>: Sized {
    /// Visits the expression itself.
    fn visit_expr(&mut self, expr: &Expr<'a>) -> Result<(), ParseError>;
    /// Visits the expression itself, with mutable access.
    fn visit_expr_mut(&mut self, expr: &mut Expr<'a>) -> Result<(), ParseError>;
}

impl<'a> Expr<'a> {
    /// Visits this expression and then its children.
    pub fn accept<V: ExprVisitor<'a>>(&self, visitor: &mut V) -> Result<(), ParseError> {
        visitor.visit_expr(self)?;
        self.accept_children(visitor)
    }

    /// Visits this expression and then its children, with mutable access.
    pub fn accept_mut<V: ExprVisitor<'a>>(&mut self, visitor: &mut V) -> Result<(), ParseError> {
        visitor.visit_expr_mut(self)?;
        self.accept_children_mut(visitor)
    }

    fn accept_children<V: ExprVisitor<'a>>(&self, visitor: &mut V) -> Result<(), ParseError> {
        match self {
            Expr::BinaryOp { left, right, .. } => {
                left.node.accept(visitor)?;
                right.node.accept(visitor)
            }
            Expr::UnaryOp { expr, .. } => expr.node.accept(visitor),
            Expr::Function { args, .. } => {
                for arg in args {
                    arg.node.accept(visitor)?;
                }
                Ok(())
            }
            Expr::WindowFunction {
                args,
                partition_by,
                order_by,
                ..
            } => {
                for arg in args {
                    arg.node.accept(visitor)?;
                }
                for expr in partition_by {
                    expr.node.accept(visitor)?;
                }
                for ob in order_by {
                    ob.expr.node.accept(visitor)?;
                }
                Ok(())
            }
            Expr::Case {
                operands,
                results,
                else_result,
                ..
            } => {
                for op in operands {
                    op.node.accept(visitor)?;
                }
                for res in results {
                    res.node.accept(visitor)?;
                }
                if let Some(else_expr) = else_result {
                    else_expr.node.accept(visitor)?;
                }
                Ok(())
            }
            Expr::Cast { expr, .. } => expr.node.accept(visitor),
            Expr::Alias { expr, .. } => expr.node.accept(visitor),
            Expr::Between {
                expr, low, high, ..
            } => {
                expr.node.accept(visitor)?;
                low.node.accept(visitor)?;
                high.node.accept(visitor)
            }
            Expr::InList { expr, list, .. } => {
                expr.node.accept(visitor)?;
                for item in list {
                    item.node.accept(visitor)?;
                }
                Ok(())
            }
            Expr::Like { expr, pattern, .. } => {
                expr.node.accept(visitor)?;
                pattern.node.accept(visitor)
            }
            _ => Ok(()),
        }
    }

    fn accept_children_mut<V: ExprVisitor<'a>>(
        &mut self,
        visitor: &mut V,
    ) -> Result<(), ParseError> {
        match self {
            Expr::BinaryOp { left, right, .. } => {
                left.node.accept_mut(visitor)?;
                right.node.accept_mut(visitor)
            }
            Expr::UnaryOp { expr, .. } => expr.node.accept_mut(visitor),
            Expr::Function { args, .. } => {
                for arg in args {
                    arg.node.accept_mut(visitor)?;
                }
                Ok(())
            }
            Expr::WindowFunction {
                args,
                partition_by,
                order_by,
                ..
            } => {
                for arg in args {
                    arg.node.accept_mut(visitor)?;
                }
                for expr in partition_by {
                    expr.node.accept_mut(visitor)?;
                }
                for ob in order_by {
                    ob.expr.node.accept_mut(visitor)?;
                }
                Ok(())
            }
            Expr::Case {
                operands,
                results,
                else_result,
                ..
            } => {
                for op in operands {
                    op.node.accept_mut(visitor)?;
                }
                for res in results {
                    res.node.accept_mut(visitor)?;
                }
                if let Some(else_expr) = else_result {
                    else_expr.node.accept_mut(visitor)?;
                }
                Ok(())
            }
            Expr::Cast { expr, .. } => expr.node.accept_mut(visitor),
            Expr::Alias { expr, .. } => expr.node.accept_mut(visitor),
            Expr::Between {
                expr, low, high, ..
            } => {
                expr.node.accept_mut(visitor)?;
                low.node.accept_mut(visitor)?;
                high.node.accept_mut(visitor)
            }
            Expr::InList { expr, list, .. } => {
                expr.node.accept_mut(visitor)?;
                for item in list {
                    item.node.accept_mut(visitor)?;
                }
                Ok(())
            }
            Expr::Like { expr, pattern, .. } => {
                expr.node.accept_mut(visitor)?;
                pattern.node.accept_mut(visitor)
            }
            _ => Ok(()),
        }
    }
}

/// A test visitor that collects every column reference it walks.

pub struct ColumnCollector<'a> {
    /// The collected `(table, column)` references.
    pub columns: Vec<(Option<&'a str>, &'a str)>,
}

impl<'a> ExprVisitor<'a> for ColumnCollector<'a> {
    fn visit_expr(&mut self, expr: &Expr<'a>) -> Result<(), ParseError> {
        if let Expr::Column { table, column, .. } = expr {
            self.columns.push((*table, *column));
        }
        Ok(())
    }

    fn visit_expr_mut(&mut self, _expr: &mut Expr<'a>) -> Result<(), ParseError> {
        Ok(())
    }
}

#[cfg(test)]
mod tests;
