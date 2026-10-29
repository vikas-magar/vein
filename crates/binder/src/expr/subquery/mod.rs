//! Single-column subquery binding with an explicit expression-use context.
use super::ExprBinder;
use error::{Result, VeinError};
use parser::ast::query::Query;
use parser::token::Span;
use plan::{LogicalExpr, PlanTraversal};

pub(super) enum SubqueryUse {
    Scalar,
    Membership { negated: bool },
}

impl SubqueryUse {
    fn label(&self) -> &'static str {
        match self {
            Self::Scalar => "scalar",
            Self::Membership { negated: false } => "IN",
            Self::Membership { negated: true } => "NOT IN",
        }
    }
}

impl ExprBinder<'_, '_> {
    pub(super) async fn bind_single_column_subquery(
        &mut self, query: &Query<'_>, span: Span, usage: SubqueryUse,
    ) -> Result<LogicalExpr> {
        let plan = self.bind_subquery(query).await?;
        let columns = plan.schema().fields.len();
        if columns != 1 {
            return Err(VeinError::semantic(
                format!("{} subquery must return exactly one column, but returned {columns}. Add an explicit SELECT inside the subquery.", usage.label()),
                Some(span),
            ));
        }
        Ok(LogicalExpr::ScalarSubquery(Box::new(plan)))
    }
}
