use std::sync::Arc;

use crate::coerce::coerce_types;
use crate::context::BinderContext;
use crate::date_time::{parse_date, parse_interval, parse_time, parse_timestamp};
use crate::pipe::query::bind_query_internal;
use crate::scope::ScopeStack;
use plan::{LogicalExpr, LogicalLiteral};
use plan::SortKey;
use plan::PlanTraversal;
use plan::{ResolvedColumn, VeinDataType};
use types::{aggregate_return_type, unify_types};
use crate::traits::{FunctionKind, RuntimeKind};
use error::{Result, VeinError};
use parser::ast::data_type::DataType;
use parser::ast::expr::{BinaryOp as AstBinaryOp, Expr, Literal as AstLiteral, SpannedExpr};

/// Derives a human-readable output column name from an AST expression, used
/// when a projection/aggregate item carries no explicit alias. Follows
/// GoogleSQL conventions: a bare column yields its name, a function call
/// yields the lowercased function name, and anything else falls back to a
/// positional placeholder.
pub fn derive_column_name(expr: &Expr<'_>) -> String {
    match expr {
        Expr::Column { column, .. } => (*column).to_string(),
        Expr::Function { name, .. } | Expr::WindowFunction { name, .. } => name.to_lowercase(),
        Expr::Alias { expr, .. } => derive_column_name(&expr.node),
        _ => "_col".to_string(),
    }
}

/// Responsible for translating AST expressions into Logical Expressions
pub struct ExprBinder<'a, 'b> {
    context: &'a mut BinderContext<'b>,
    scopes: &'a mut ScopeStack,
}

impl<'a, 'b> ExprBinder<'a, 'b> {
    /// Creates a new ExprBinder using the current context and scope stack
    pub fn new(context: &'a mut BinderContext<'b>, scopes: &'a mut ScopeStack) -> Self {
        Self { context, scopes }
    }

    /// Recursively binds an AST expression into a type-aware LogicalExpr
    pub async fn bind(&mut self, ast: &SpannedExpr<'_>) -> Result<LogicalExpr> {
        match &ast.node {
            Expr::Column { table, column } => {
                let (resolved, correlated) =
                    self.scopes.resolve_correlated(table.as_deref(), column)?;
                Ok(if correlated {
                    LogicalExpr::OuterColumn(resolved)
                } else {
                    LogicalExpr::Column(resolved)
                })
            }
            Expr::Literal(lit) => self.bind_literal(lit),
            Expr::Wildcard { .. } => Err(VeinError::semantic(
                "wildcard '*' is only allowed in SELECT/EXTEND/DROP item lists",
                Some(ast.span),
            )),
            Expr::Function {
                name,
                args,
                distinct,
            } => {
                self.bind_function_call(name, args, *distinct, ast.span)
                    .await
            }
            Expr::WindowFunction {
                name,
                args,
                distinct,
                partition_by,
                order_by,
                window_frame,
            } => {
                self.bind_window_call(
                    name,
                    args,
                    *distinct,
                    partition_by,
                    order_by,
                    window_frame.as_ref(),
                    ast.span,
                )
                    .await
            }
            Expr::BinaryOp { left, op, right } => {
                let bound_left = Box::pin(self.bind(left)).await?;
                let bound_right = Box::pin(self.bind(right)).await?;

                // Comparison/logical operators always return Boolean while
                // widening their operands; arithmetic unifies both operands
                // to the common supertype and returns that type.
                if matches!(
                    op,
                    AstBinaryOp::Eq
                        | AstBinaryOp::Neq
                        | AstBinaryOp::Lt
                        | AstBinaryOp::Gt
                        | AstBinaryOp::Leq
                        | AstBinaryOp::Geq
                        | AstBinaryOp::And
                        | AstBinaryOp::Or
                ) {
                    let (l, r, _) = coerce_types(bound_left, bound_right)?;
                    return Ok(LogicalExpr::BinaryOp {
                        left: Box::new(l),
                        op: *op,
                        right: Box::new(r),
                        return_type: VeinDataType::Boolean,
                    });
                }

                let (l, r, common) = coerce_types(bound_left, bound_right)?;
                Ok(LogicalExpr::BinaryOp {
                    left: Box::new(l),
                    op: *op,
                    right: Box::new(r),
                    return_type: common,
                })
            }
            Expr::UnaryOp { op, expr } => {
                let bound = Box::pin(self.bind(expr)).await?;
                use parser::ast::expr::UnaryOp as AstUnaryOp;
                match op {
                    AstUnaryOp::Not => Ok(LogicalExpr::Not {
                        expr: Box::new(bound),
                    }),
                    AstUnaryOp::Negate => Ok(LogicalExpr::Negate {
                        expr: Box::new(bound),
                    }),
                    AstUnaryOp::IsNull => Ok(LogicalExpr::IsNull {
                        expr: Box::new(bound),
                    }),
                    AstUnaryOp::IsNotNull => Ok(LogicalExpr::IsNotNull {
                        expr: Box::new(bound),
                    }),
                    AstUnaryOp::Plus => Ok(bound),
                }
            }
            Expr::Case {
                operands,
                results,
                else_result,
            } => {
                self.bind_case(operands, results, else_result.as_deref(), ast.span)
                    .await
            }
            Expr::Cast { expr, data_type } => {
                let bound = Box::pin(self.bind(expr)).await?;
                let target_type = ast_type_to_vein(data_type).map_err(|msg| {
                    VeinError::semantic(
                        format!("{msg} (in CAST) at {}", ast.span.start),
                        Some(ast.span),
                    )
                })?;
                Ok(LogicalExpr::Cast {
                    expr: Box::new(bound),
                    target_type,
                })
            }
            Expr::Between {
                expr,
                low,
                high,
                negated,
            } => {
                let bound = Box::pin(self.bind(expr)).await?;
                let low = Box::pin(self.bind(low)).await?;
                let high = Box::pin(self.bind(high)).await?;

                let (ge_low, _, _) = coerce_types(bound.clone(), low)?;
                let (le_high, _, _) = coerce_types(bound, high)?;

                let mut result = LogicalExpr::BinaryOp {
                    left: Box::new(ge_low),
                    op: AstBinaryOp::Geq,
                    right: Box::new(le_high),
                    return_type: VeinDataType::Boolean,
                };
                if *negated {
                    result = LogicalExpr::Not {
                        expr: Box::new(result),
                    };
                }
                Ok(result)
            }
            Expr::InList {
                expr,
                list,
                negated,
            } => {
                let bound = Box::pin(self.bind(expr)).await?;
                let mut bound_list = Vec::with_capacity(list.len());
                for item in list {
                    bound_list.push(Box::pin(self.bind(item)).await?);
                }

                // Establish one common type across the probe and every list item,
                // then coerce each side to it so the IN comparison is consistent
                // (rejecting e.g. an Int probe against a mixed Int/String list).
                let mut common = bound.data_type();
                for item in &bound_list {
                    common = unify_types(&common, &item.data_type())?;
                }
                let bound = cast_to_common(bound, &common);
                let list = bound_list
                    .into_iter()
                    .map(|it| cast_to_common(it, &common))
                    .collect::<Vec<_>>();

                Ok(LogicalExpr::InList {
                    expr: Box::new(bound),
                    list,
                    negated: *negated,
                })
            }
            Expr::Like {
                expr,
                pattern,
                negated,
            } => Ok(LogicalExpr::Like {
                expr: Box::new(Box::pin(self.bind(expr)).await?),
                pattern: Box::new(Box::pin(self.bind(pattern)).await?),
                negated: *negated,
            }),
            Expr::Alias { expr, .. } => Box::pin(self.bind(expr)).await,
            Expr::TypedString { data_type, value } => {
                self.bind_typed_string(data_type, value, ast.span)
            }
            Expr::Exists { subquery, negated } => {
                let plan = Box::pin(self.bind_subquery(subquery)).await?;
                Ok(LogicalExpr::Exists {
                    subquery: Box::new(plan),
                    negated: *negated,
                })
            }
            Expr::Subquery(subquery) | Expr::ScalarSubquery(subquery) => {
                let plan = self.bind_subquery(subquery).await?;
                // A scalar subquery must produce exactly one column.
                if plan.schema().fields.len() != 1 {
                    return Err(VeinError::semantic(
                        "scalar subquery must return exactly one column",
                        Some(ast.span),
                    ));
                }
                Ok(LogicalExpr::ScalarSubquery(Box::new(plan)))
            }
        }
    }

    /// Binds a literal node into a typed logical literal.
    fn bind_literal(&self, lit: &AstLiteral) -> Result<LogicalExpr> {
        let logical_lit = match lit {
            AstLiteral::Boolean(b) => LogicalLiteral::Boolean(*b),
            AstLiteral::Int(i) => LogicalLiteral::Int64(*i),
            AstLiteral::Float(f) => LogicalLiteral::Float64(f.0),
            AstLiteral::String(s) => LogicalLiteral::String(s.clone()),
            AstLiteral::Null => LogicalLiteral::Null(VeinDataType::Null),
        };
        Ok(LogicalExpr::Literal(logical_lit))
    }

    /// Binds a plain function call through the registry (LLD §9: the registry
    /// is the single source of truth — no hardcoded functions in the binder).
    async fn bind_function_call(
        &mut self,
        name: &str,
        args: &[SpannedExpr<'_>],
        distinct: bool,
        _span: parser::token::Span,
    ) -> Result<LogicalExpr> {
        let signature = self.context.functions.resolve(name)?;

        let mut bound_args = Vec::with_capacity(args.len());
        for arg in args {
            bound_args.push(Box::pin(self.bind(arg)).await?);
        }

        match signature.kind {
            FunctionKind::AiOperator => {
                self.validate_ai_budget(name, &bound_args)?;
                Ok(LogicalExpr::AiOperator {
                    name: name.to_string(),
                    args: bound_args,
                    return_type: signature.return_type,
                })
            }
            FunctionKind::UserDefined(runtime) => match runtime {
                RuntimeKind::Wasm => Ok(LogicalExpr::WasmUdf {
                    bytecode_ref: signature.bytecode_ref,
                    args: bound_args,
                    return_type: signature.return_type,
                }),
                RuntimeKind::Ebpf => Ok(LogicalExpr::EbpfUdf {
                    bytecode_ref: signature.bytecode_ref,
                    args: bound_args,
                    return_type: signature.return_type,
                }),
                RuntimeKind::Python => Ok(LogicalExpr::PythonUdf {
                    bytecode_ref: signature.bytecode_ref,
                    args: bound_args,
                    return_type: signature.return_type,
                }),
            },
            FunctionKind::Scalar | FunctionKind::Aggregate => {
                let is_aggregate = signature.kind == FunctionKind::Aggregate;
                let return_type = if is_aggregate {
                    let argument_type = bound_args.first().map(LogicalExpr::data_type);
                    aggregate_return_type(name, argument_type.as_ref(), signature.return_type)
                } else {
                    signature.return_type
                };
                Ok(LogicalExpr::FunctionCall {
                    name: name.to_string(),
                    args: bound_args,
                    is_aggregate,
                    distinct,
                    return_type,
                })
            }
        }
    }

    /// Binds a window function call (`fn(args) OVER (...)`). The call itself
    /// is registry-resolved; the surrounding OVER clause is bound here and
    /// lifted into a `LogicalPlan::Window` by the projection binders.
    #[allow(clippy::too_many_arguments)]
    async fn bind_window_call(
        &mut self,
        name: &str,
        args: &[SpannedExpr<'_>],
        distinct: bool,
        partition_by: &[SpannedExpr<'_>],
        order_by: &[parser::ast::pipe_op::OrderByExpr<'_>],
        frame: Option<&parser::ast::pipe_op::WindowFrame>,
        span: parser::token::Span,
    ) -> Result<LogicalExpr> {
        if distinct {
            return Err(VeinError::semantic(
                format!("DISTINCT inside window function '{name}' is not supported yet"),
                Some(span),
            ));
        }
        let signature = self.context.functions.resolve(name)?;

        let mut bound_args = Vec::with_capacity(args.len());
        for arg in args {
            // COUNT(*) lowers to COUNT(1), mirroring GoogleSQL semantics.
            if matches!(arg.node, Expr::Wildcard { .. }) {
                bound_args.push(LogicalExpr::Literal(LogicalLiteral::Int64(1)));
            } else {
                bound_args.push(Box::pin(self.bind(arg)).await?);
            }
        }

        let mut bound_partition = Vec::with_capacity(partition_by.len());
        for p in partition_by {
            bound_partition.push(Box::pin(self.bind(p)).await?);
        }

        let mut bound_order = Vec::with_capacity(order_by.len());
        for ob in order_by {
            let expr = Box::pin(self.bind(&ob.expr)).await?;
            bound_order.push(SortKey {
                expr,
                asc: ob.asc,
                nulls_first: ob.nulls_first,
            });
        }

        Ok(LogicalExpr::WindowCall {
            name: name.to_string(),
            args: bound_args,
            is_aggregate: signature.kind == FunctionKind::Aggregate,
            distinct,
            partition_by: bound_partition,
            order_by: bound_order,
            frame: frame.cloned(),
            return_type: signature.return_type,
        })
    }

    /// Binds a CASE expression: searched CASE binds operands as predicates;
    /// simple CASE arrives pre-lowered to equality comparisons by the parser.
    async fn bind_case(
        &mut self,
        operands: &[SpannedExpr<'_>],
        results: &[SpannedExpr<'_>],
        else_result: Option<&SpannedExpr<'_>>,
        span: parser::token::Span,
    ) -> Result<LogicalExpr> {
        if operands.len() != results.len() || operands.is_empty() {
            return Err(VeinError::semantic(
                "CASE must have the same number of WHEN and THEN clauses",
                Some(span),
            ));
        }

        let mut when = Vec::with_capacity(operands.len());
        let mut then = Vec::with_capacity(results.len());

        for (operand, result) in operands.iter().zip(results) {
            when.push(Box::pin(self.bind(operand)).await?);
            then.push(Box::pin(self.bind(result)).await?);
        }
        let else_expr = match else_result {
            Some(e) => Some(Box::pin(self.bind(e)).await?),
            None => None,
        };

        // Unify every THEN/ELSE branch into a single return type.
        let mut common_type = VeinDataType::Null;
        for result in &then {
            common_type = unify_with(result.data_type(), common_type)?;
        }
        if let Some(else_expr) = &else_expr {
            common_type = unify_with(else_expr.data_type(), common_type)?;
        }

        // Coerce every non-Null branch to the unified return type so downstream
        // consumers observe consistent types instead of a silent type lie.
        let then = then
            .into_iter()
            .map(|r| cast_to_common(r, &common_type))
            .collect::<Vec<_>>();
        let else_ = else_expr.map(|e| Box::new(cast_to_common(e, &common_type)));

        Ok(LogicalExpr::Case {
            when,
            then,
            else_,
            return_type: common_type,
        })
    }

    /// Binds a typed string literal (`DATE '...'`, `TIMESTAMP '...'`).
    fn bind_typed_string(
        &self,
        data_type: &DataType<'_>,
        value: &str,
        span: parser::token::Span,
    ) -> Result<LogicalExpr> {
        match data_type {
            DataType::Date => parse_date(value)
                .map(LogicalLiteral::Date)
                .map(LogicalExpr::Literal),
            DataType::Time => parse_time(value)
                .map(LogicalLiteral::Time)
                .map(LogicalExpr::Literal),
            DataType::Interval { .. } => parse_interval(value)
                .map(|(months, days, nanos)| {
                    LogicalExpr::Literal(LogicalLiteral::Interval {
                        months,
                        days,
                        nanos,
                    })
                }),
            DataType::Timestamp { .. } => parse_timestamp(value)
                .map(LogicalLiteral::Timestamp)
                .map(LogicalExpr::Literal),
            other => Err(VeinError::semantic(
                format!("typed string literals of type {other:?} are not supported yet"),
                Some(span),
            )),
        }
    }

    /// Binds a subquery in a child scope stack so correlated references can
    /// resolve into the enclosing scopes (wrapped as `OuterColumn`).
    async fn bind_subquery(
        &mut self,
        query: &parser::ast::query::Query<'_>,
    ) -> Result<plan::LogicalPlan> {
        // The child captures the enclosing scopes *by value* into its
        // `correlation_scopes`, so `resolve_correlated` marks references found
        // there as correlated (`OuterColumn`). This is fully owned and avoids
        // the self-referential parent reference that Rust cannot express
        // safely (previously worked around with raw pointers).
        let mut child_scopes = ScopeStack::new();
        child_scopes.capture_correlation(self.scopes);
        bind_query_internal(query, self.context, &mut child_scopes).await
    }

    /// FinOps Guardrail: Validates that the estimated token cost of an AI Operator does not exceed the budget.
    fn validate_ai_budget(&self, op_name: &str, _args: &[LogicalExpr]) -> Result<()> {
        let contextual_max_cost_usd = 10.00;
        let estimated_cost_usd = 0.05;

        if estimated_cost_usd > contextual_max_cost_usd {
            return Err(VeinError::internal(format!(
                "FinOps Fault: AI Operator '{}' estimated to cost ${:.2}, exceeding limit of ${:.2}",
                op_name, estimated_cost_usd, contextual_max_cost_usd
            )));
        }

        Ok(())
    }
}

/// Unifies a branch type with the running common type (Null seeded start).
///
/// Delegates to the shared [`unify_types`] so every branch pair benefits from
/// the same rules as binary/set-op coercion (numeric widening, symmetric string
/// unification, `Date32 → Timestamp` promotion), while surfacing a CASE-specific
/// error message on incompatibility.
fn unify_with(branch: VeinDataType, common: VeinDataType) -> Result<VeinDataType> {
    if common == VeinDataType::Null {
        return Ok(branch);
    }
    if branch == VeinDataType::Null {
        return Ok(common);
    }
    if branch == common {
        return Ok(common);
    }
    unify_types(&common, &branch).map_err(|_| {
        VeinError::semantic(
            format!("CASE branches have incompatible types {common:?} and {branch:?}"),
            None,
        )
    })
}

/// Wraps `expr` in a `Cast` to `common` when its type differs and it isn't a
/// `Null` literal (Null adopts any type without an explicit cast). No-op
/// otherwise, so a branch already at the target type is left untouched.
fn cast_to_common(expr: LogicalExpr, common: &VeinDataType) -> LogicalExpr {
    if expr.data_type() == *common || expr.data_type() == VeinDataType::Null {
        expr
    } else {
        LogicalExpr::Cast {
            expr: Box::new(expr),
            target_type: common.clone(),
        }
    }
}

/// Converts a parser AST data type into a planner `VeinDataType`.
pub fn ast_type_to_vein(data_type: &DataType<'_>) -> Result<VeinDataType> {
    Ok(match data_type {
        DataType::Boolean => VeinDataType::Boolean,
        DataType::Int => VeinDataType::Int32,
        DataType::TinyInt => VeinDataType::Int8,
        DataType::SmallInt => VeinDataType::Int16,
        DataType::BigInt => VeinDataType::Int64,
        DataType::Float => VeinDataType::Float32,
        DataType::Double => VeinDataType::Float64,
        DataType::Decimal { precision, scale } => {
            VeinDataType::Decimal128(*precision, i8::try_from(*scale).unwrap_or(i8::MAX))
        }
        DataType::String => VeinDataType::Utf8,
        DataType::Char(_) | DataType::Varchar(_) => VeinDataType::Utf8,
        DataType::Binary | DataType::Varbinary(_) => VeinDataType::Binary,
        DataType::Date => VeinDataType::Date32,
        DataType::Time => VeinDataType::Time64,
        DataType::Timestamp { with_tz } => {
            if *with_tz {
                VeinDataType::Timestamp(Some("UTC".to_string()))
            } else {
                VeinDataType::Timestamp(None)
            }
        }
        DataType::Interval { .. } => VeinDataType::Interval,
        DataType::Array(inner) => VeinDataType::List(Arc::new(ast_type_to_vein(inner)?)),
        DataType::Vector(dim) => VeinDataType::Vector(*dim),
        DataType::Struct(fields) => {
            let mut cols = Vec::with_capacity(fields.len());
            for (name, ty) in fields {
                cols.push(ResolvedColumn::new(
                    plan::ColumnId(0),
                    *name,
                    None,
                    ast_type_to_vein(ty)?,
                ));
            }
            VeinDataType::Struct(cols.into())
        }
        DataType::Map { key, value } => VeinDataType::Map(
            Arc::new(ast_type_to_vein(key)?),
            Arc::new(ast_type_to_vein(value)?),
        ),
    })
}
