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
                left.accept(visitor)?;
                right.accept(visitor)
            }
            Expr::UnaryOp { expr, .. } => expr.accept(visitor),
            Expr::Function { args, .. } => {
                for arg in args {
                    arg.accept(visitor)?;
                }
                Ok(())
            }
            Expr::WindowFunction { args, partition_by, order_by, .. } => {
                for arg in args {
                    arg.accept(visitor)?;
                }
                for expr in partition_by {
                    expr.accept(visitor)?;
                }
                for ob in order_by {
                    ob.expr.accept(visitor)?;
                }
                Ok(())
            }
            Expr::Case { operands, results, else_result, .. } => {
                for op in operands {
                    op.accept(visitor)?;
                }
                for res in results {
                    res.accept(visitor)?;
                }
                if let Some(else_expr) = else_result {
                    else_expr.accept(visitor)?;
                }
                Ok(())
            }
            Expr::Cast { expr, .. } => expr.accept(visitor),
            Expr::Alias { expr, .. } => expr.accept(visitor),
            Expr::Between { expr, low, high, .. } => {
                expr.accept(visitor)?;
                low.accept(visitor)?;
                high.accept(visitor)
            }
            Expr::InList { expr, list, .. } => {
                expr.accept(visitor)?;
                for item in list {
                    item.accept(visitor)?;
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }

    fn accept_children_mut<V: ExprVisitor>(&mut self, visitor: &mut V) -> Result<(), PlanError> {
        match self {
            Expr::BinaryOp { left, right, .. } => {
                left.accept_mut(visitor)?;
                right.accept_mut(visitor)
            }
            Expr::UnaryOp { expr, .. } => expr.accept_mut(visitor),
            Expr::Function { args, .. } => {
                for arg in args {
                    arg.accept_mut(visitor)?;
                }
                Ok(())
            }
            Expr::WindowFunction { args, partition_by, order_by, .. } => {
                for arg in args {
                    arg.accept_mut(visitor)?;
                }
                for expr in partition_by {
                    expr.accept_mut(visitor)?;
                }
                for ob in order_by {
                    ob.expr.accept_mut(visitor)?;
                }
                Ok(())
            }
            Expr::Case { operands, results, else_result, .. } => {
                for op in operands {
                    op.accept_mut(visitor)?;
                }
                for res in results {
                    res.accept_mut(visitor)?;
                }
                if let Some(else_expr) = else_result {
                    else_expr.accept_mut(visitor)?;
                }
                Ok(())
            }
            Expr::Cast { expr, .. } => expr.accept_mut(visitor),
            Expr::Alias { expr, .. } => expr.accept_mut(visitor),
            Expr::Between { expr, low, high, .. } => {
                expr.accept_mut(visitor)?;
                low.accept_mut(visitor)?;
                high.accept_mut(visitor)
            }
            Expr::InList { expr, list, .. } => {
                expr.accept_mut(visitor)?;
                for item in list {
                    item.accept_mut(visitor)?;
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
        // a + b + c  =>  BinaryOp(Plus, BinaryOp(Plus, Column(a), Column(b)), Column(c))
        let c = Expr::Column { table: None, column: "c".into(), span: Default::default() };
        let b = Expr::Column { table: None, column: "b".into(), span: Default::default() };
        let a = Expr::Column { table: None, column: "a".into(), span: Default::default() };
        let ab = Expr::BinaryOp {
            left: Box::new(a),
            op: BinaryOp::Plus,
            right: Box::new(b),
            span: Default::default(),
        };
        let abc = Expr::BinaryOp {
            left: Box::new(ab),
            op: BinaryOp::Plus,
            right: Box::new(c),
            span: Default::default(),
        };

        let mut collector = ColumnCollector { columns: vec![] };
        abc.accept(&mut collector).unwrap();

        assert_eq!(collector.columns.len(), 3);
        assert_eq!(collector.columns[0].1, "a");
        assert_eq!(collector.columns[1].1, "b");
        assert_eq!(collector.columns[2].1, "c");
    }

    #[test]
    fn test_inlist_visits_children() {
        // x IN (1, y)  =>  InList { expr: Column(x), list: [Literal(1), Column(y)] }
        let x = Expr::Column { table: None, column: "x".into(), span: Default::default() };
        let y = Expr::Column { table: None, column: "y".into(), span: Default::default() };
        let one = Expr::Literal(Literal::Int(1), Default::default());
        let inlist = Expr::InList {
            expr: Box::new(x),
            list: vec![one, y],
            negated: false,
            span: Default::default(),
        };

        let mut collector = ColumnCollector { columns: vec![] };
        inlist.accept(&mut collector).unwrap();

        assert_eq!(collector.columns.len(), 2);
        assert_eq!(collector.columns[0].1, "x");
        assert_eq!(collector.columns[1].1, "y");
    }
}
