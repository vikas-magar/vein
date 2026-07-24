use crate::ast::expr::Expr;
use crate::logical_plan::PlanError;

pub trait ExprVisitor: Sized {
    fn visit_expr(&mut self, expr: &Expr) -> Result<(), PlanError>;
    fn visit_expr_mut(&mut self, expr: &mut Expr) -> Result<(), PlanError>;
}

impl Expr {
    pub fn accept<V: ExprVisitor>(&self, visitor: &mut V) -> Result<(), PlanError> {
        visitor.visit_expr(self)?;
        self.accept_children(visitor)
    }

    pub fn accept_mut<V: ExprVisitor>(&mut self, visitor: &mut V) -> Result<(), PlanError> {
        visitor.visit_expr_mut(self)?;
        self.accept_children_mut(visitor)
    }

    fn accept_children<V: ExprVisitor>(&self, visitor: &mut V) -> Result<(), PlanError> {
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
            Expr::WindowFunction { args, partition_by, order_by, .. } => {
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
            Expr::Case { operands, results, else_result, .. } => {
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
            Expr::Between { expr, low, high, .. } => {
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
            _ => Ok(()),
        }
    }

    fn accept_children_mut<V: ExprVisitor>(&mut self, visitor: &mut V) -> Result<(), PlanError> {
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
            Expr::WindowFunction { args, partition_by, order_by, .. } => {
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
            Expr::Case { operands, results, else_result, .. } => {
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
            Expr::Between { expr, low, high, .. } => {
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
            _ => Ok(()),
        }
    }
}

pub struct ColumnCollector {
    pub columns: Vec<(Option<String>, String)>,
}

impl ExprVisitor for ColumnCollector {
    fn visit_expr(&mut self, expr: &Expr) -> Result<(), PlanError> {
        if let Expr::Column { table, column, .. } = expr {
            self.columns.push((table.clone(), column.clone()));
        }
        Ok(())
    }

    fn visit_expr_mut(&mut self, _expr: &mut Expr) -> Result<(), PlanError> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::expr::{BinaryOp, Literal};

    #[test]
    fn test_column_collector_recursive() {
        use crate::ast::expr::SpannedExpr;
        let c = SpannedExpr { span: Default::default(), node: Expr::Column { table: None, column: "c".into() } };
        let b = SpannedExpr { span: Default::default(), node: Expr::Column { table: None, column: "b".into() } };
        let a = SpannedExpr { span: Default::default(), node: Expr::Column { table: None, column: "a".into() } };
        let ab = SpannedExpr { span: Default::default(), node: Expr::BinaryOp {
            left: Box::new(a),
            op: BinaryOp::Plus,
            right: Box::new(b),
        }};
        let abc = SpannedExpr { span: Default::default(), node: Expr::BinaryOp {
            left: Box::new(ab),
            op: BinaryOp::Plus,
            right: Box::new(c),
        }};

        let mut collector = ColumnCollector { columns: vec![] };
        abc.node.accept(&mut collector).unwrap();

        assert_eq!(collector.columns.len(), 3);
        assert_eq!(collector.columns[0].1, "a");
        assert_eq!(collector.columns[1].1, "b");
        assert_eq!(collector.columns[2].1, "c");
    }

    #[test]
    fn test_inlist_visits_children() {
        use crate::ast::expr::SpannedExpr;
        let x = SpannedExpr { span: Default::default(), node: Expr::Column { table: None, column: "x".into() } };
        let y = SpannedExpr { span: Default::default(), node: Expr::Column { table: None, column: "y".into() } };
        let one = SpannedExpr { span: Default::default(), node: Expr::Literal(Literal::Int(1)) };
        let inlist = SpannedExpr { span: Default::default(), node: Expr::InList {
            expr: Box::new(x),
            list: vec![one, y],
            negated: false,
        }};

        let mut collector = ColumnCollector { columns: vec![] };
        inlist.node.accept(&mut collector).unwrap();

        assert_eq!(collector.columns.len(), 2);
        assert_eq!(collector.columns[0].1, "x");
        assert_eq!(collector.columns[1].1, "y");
    }
}
