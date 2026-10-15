//! Pretty-printing for logical plans and expressions.
//!
//! [`format_plan`] renders a [`LogicalPlan`] as an indented tree, one line
//! per node, mirroring the output of `EXPLAIN` in other engines. It has no
//! external dependencies and never fails.

use crate::expr::LogicalExpr;
use crate::plan::logical::{LogicalPlan, WindowExpr};
use crate::plan::traversal::PlanTraversal;
use crate::schema::{ResolvedColumn, ResolvedColumnRef, VeinDataType};
use parser::ast::expr::BinaryOp;
use parser::ast::pipe_op::{WindowFrame, WindowFrameBound, WindowFrameKind};
use std::cell::Cell;
use std::fmt;

/// Maximum nesting depth for plan/expression rendering. Display recurses over
/// both plan children and expression trees (which interleave, e.g. through
/// `Exists`/subquery plans), so a pathologically deep tree would otherwise
/// overflow the stack. At the limit rendering emits a `...` marker instead.
const MAX_DISPLAY_DEPTH: usize = 100;

thread_local! {
    static DISPLAY_DEPTH: Cell<usize> = const { Cell::new(0) };
}

/// Enters a display recursion level, returning `false` once the budget is
/// exhausted (callers must not recurse further and must emit a truncation
/// marker instead). Every `true` return must be paired with [`exit_display`].
fn enter_display() -> bool {
    DISPLAY_DEPTH.with(|d| {
        let depth = d.get();
        if depth >= MAX_DISPLAY_DEPTH {
            false
        } else {
            d.set(depth + 1);
            true
        }
    })
}

fn exit_display() {
    DISPLAY_DEPTH.with(|d| d.set(d.get() - 1));
}

/// Renders a logical plan as a box-drawing tree.
///
/// Example:
///
/// ```text
/// Project [exprs: [id, name], schema: [id, name]]
/// └── Scan [table: users, projection: none, filters: 0]
/// ```
pub fn format_plan(plan: &LogicalPlan) -> String {
    let mut out = String::new();
    build_lines(plan, "", "", &mut out);
    out
}

/// Renders a logical expression in a compact, SQL-like form.
///
/// Columns render as `qualifier.name` (or just `name`), literals verbatim,
/// and function calls as `name(args...)`. Operators use symbolic forms
/// (`a + b`, `x = 10`) where one exists.
pub fn format_expr(expr: &LogicalExpr) -> String {
    if !enter_display() {
        return "...".to_string();
    }
    let out = format_expr_inner(expr);
    exit_display();
    out
}

fn format_expr_inner(expr: &LogicalExpr) -> String {
    match expr {
        LogicalExpr::Column(col) => format_col(col),
        LogicalExpr::OuterColumn(col) => format!("outer:{}", format_col(col)),
        LogicalExpr::Literal(lit) => format_literal(lit),
        LogicalExpr::BinaryOp { left, op, right, .. } => {
            format!("({} {} {})", format_expr(left), op_symbol(*op), format_expr(right))
        }
        LogicalExpr::Cast { expr, target_type } => {
            format!("CAST({} AS {})", format_expr(expr), type_str(target_type))
        }
        LogicalExpr::Not { expr } => format!("NOT({})", format_expr(expr)),
        LogicalExpr::Negate { expr } => format!("-{}", format_expr(expr)),
        LogicalExpr::IsNull { expr } => format!("{} IS NULL", format_expr(expr)),
        LogicalExpr::IsNotNull { expr } => format!("{} IS NOT NULL", format_expr(expr)),
        LogicalExpr::InList { expr, list, negated } => {
            let items = format_exprs(list, ", ");
            if *negated {
                format!("{} NOT IN ({items})", format_expr(expr))
            } else {
                format!("{} IN ({items})", format_expr(expr))
            }
        }
        LogicalExpr::Like { expr, pattern, negated } => {
            let op = if *negated { "NOT LIKE" } else { "LIKE" };
            format!("{} {op} {}", format_expr(expr), format_expr(pattern))
        }
        LogicalExpr::Case { when, then, else_, .. } => {
            let mut out = String::from("CASE");
            for (w, t) in when.iter().zip(then) {
                out.push_str(&format!(" WHEN {} THEN {}", format_expr(w), format_expr(t)));
            }
            if let Some(else_expr) = else_ {
                out.push_str(&format!(" ELSE {}", format_expr(else_expr)));
            }
            out.push_str(" END");
            out
        }
        LogicalExpr::FunctionCall {
            name,
            args,
            distinct,
            ..
        } => {
            if *distinct {
                format!("{name}(DISTINCT {})", format_exprs(args, ", "))
            } else {
                format!("{name}({})", format_exprs(args, ", "))
            }
        }
        LogicalExpr::WindowCall {
            name,
            args,
            distinct,
            order_by,
            partition_by,
            frame,
            ..
        } => {
            let inner = if *distinct {
                format!("DISTINCT {}", format_exprs(args, ", "))
            } else {
                format_exprs(args, ", ")
            };
            let mut out = format!("{name}({inner})");
            out.push_str(" OVER (");
            if !partition_by.is_empty() {
                out.push_str(&format!("PARTITION BY {}", format_exprs(partition_by, ", ")));
            }
            if !order_by.is_empty() {
                if !partition_by.is_empty() {
                    out.push(' ');
                }
                out.push_str(&format!("ORDER BY {}", format_sort_keys(order_by)));
            }
            if let Some(frame) = frame {
                if !partition_by.is_empty() || !order_by.is_empty() {
                    out.push(' ');
                }
                out.push_str(&format_window_frame(frame));
            }
            out.push(')');
            out
        }
        LogicalExpr::AiOperator { name, args, .. } => {
            format!("AI:{name}({})", format_exprs(args, ", "))
        }
        LogicalExpr::WasmUdf { bytecode_ref, args, .. } => {
            format_udf("WASM", bytecode_ref.as_deref(), args)
        }
        LogicalExpr::EbpfUdf { bytecode_ref, args, .. } => {
            format_udf("EBPF", bytecode_ref.as_deref(), args)
        }
        LogicalExpr::PythonUdf { bytecode_ref, args, .. } => {
            format_udf("PYTHON", bytecode_ref.as_deref(), args)
        }
        LogicalExpr::Lambda { params, body, .. } => {
            let params = params
                .iter()
                .map(|c| c.name.as_str())
                .collect::<Vec<_>>()
                .join(", ");
            format!("({params}) -> {}", format_expr(body))
        }
        LogicalExpr::ScalarSubquery(plan) => format!("(scalar subquery {})", format_plan_flat(plan)),
        LogicalExpr::Exists { subquery, negated } => {
            if *negated {
                format!("NOT EXISTS ({})", format_plan_flat(subquery))
            } else {
                format!("EXISTS ({})", format_plan_flat(subquery))
            }
        }
    }
}

/// Renders a schema as `[name:Type, ...]`.
pub fn format_schema(fields: &[ResolvedColumnRef]) -> String {
    let cols = fields
        .iter()
        .map(|f| format!("{}:{}", f.name, type_str(&f.data_type)))
        .collect::<Vec<_>>()
        .join(", ");
    if cols.is_empty() {
        "[]".to_string()
    } else {
        format!("[{cols}]")
    }
}

impl fmt::Display for LogicalPlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&format_plan(self))
    }
}

impl fmt::Display for LogicalExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&format_expr(self))
    }
}

// ── Internals ─────────────────────────────────────────────────────────────

fn build_lines(plan: &LogicalPlan, prefix: &str, child_prefix: &str, out: &mut String) {
    if !enter_display() {
        // Budget exhausted: emit a truncation marker instead of recursing
        // further (the tree is deeper than MAX_DISPLAY_DEPTH).
        if !prefix.is_empty() {
            out.push_str(prefix);
            out.push(' ');
        }
        out.push_str("...");
        out.push('\n');
        return;
    }

    if !prefix.is_empty() {
        out.push_str(prefix);
        out.push(' ');
    }
    out.push_str(&node_line(plan));
    out.push('\n');

    let children = plan.children();
    let last = children.len().saturating_sub(1);
    for (i, child) in children.into_iter().enumerate() {
        let (branch, next) = if i == last {
            ("└──", "    ")
        } else {
            ("├──", "│   ")
        };
        let full = format!("{child_prefix}{branch}");
        build_lines(child, &full, &format!("{child_prefix}{next}"), out);
    }

    exit_display();
}

fn node_line(plan: &LogicalPlan) -> String {
    let (kind, details, show_schema) = match plan {
        LogicalPlan::Scan {
            table,
            projection,
            filters,
            ..
        } => {
            let table_name = &table.name;
            let proj = match projection {
                Some(cols) => cols
                    .iter()
                    .map(|c| c.0.to_string())
                    .collect::<Vec<_>>()
                    .join(", "),
                None => "none".to_string(),
            };
            (
                "Scan".to_string(),
                format!("table: {table_name}, projection: [{proj}], filters: {}", filters.len()),
                false,
            )
        }
        LogicalPlan::ExternalScan { url, options, .. } => {
            (
                "ExternalScan".to_string(),
                format!("url: {url}, options: {:?}", options),
                false,
            )
        }
        LogicalPlan::Filter { predicate, .. } => {
            ("Filter".to_string(), format!("predicate: {}", format_expr(predicate)), false)
        }
        LogicalPlan::Project { exprs, .. } => (
            "Project".to_string(),
            format!("exprs: [{}]", format_exprs(exprs, ", ")),
            true,
        ),
        LogicalPlan::Aggregate {
            grouping_sets,
            aggs,
            ..
        } => {
            let groups = grouping_sets
                .first()
                .map(|set| format_exprs(set, ", "))
                .unwrap_or_default();
            (
                "Aggregate".to_string(),
                format!("groups: [{groups}], aggs: [{}]", format_exprs(aggs, ", ")),
                true,
            )
        }
        LogicalPlan::Window {
            partition_by,
            order_by,
            frame,
            exprs,
            ..
        } => {
            let mut parts = Vec::new();
            if !partition_by.is_empty() {
                parts.push(format!("partition_by: [{}]", format_exprs(partition_by, ", ")));
            }
            if !order_by.is_empty() {
                parts.push(format!("order_by: [{}]", format_sort_keys(order_by)));
            }
            if let Some(frame) = frame {
                parts.push(format!("frame: {}", format_window_frame(frame)));
            }
            (
                "Window".to_string(),
                format!("{} exprs: [{}]", parts.join(", "), format_window_exprs(exprs)),
                true,
            )
        }
        LogicalPlan::Sort { sort_keys, .. } => (
            "Sort".to_string(),
            format!("keys: [{}]", format_sort_keys(sort_keys)),
            true,
        ),
        LogicalPlan::Limit { offset, count, .. } => {
            ("Limit".to_string(), format!("offset: {offset}, count: {count}"), false)
        }
        LogicalPlan::Distinct { .. } => ("Distinct".to_string(), String::new(), false),
        LogicalPlan::SubqueryAlias { alias, .. } => {
            ("SubqueryAlias".to_string(), format!("as: {alias}"), false)
        }
        LogicalPlan::Explain { .. } => ("Explain".to_string(), String::new(), false),
        LogicalPlan::SetOp { kind, all, .. } => {
            let all = if *all { " ALL" } else { "" };
            ("SetOp".to_string(), format!("{kind:?}{all}"), false)
        }
        LogicalPlan::Join {
            join_type,
            on,
            using,
            ..
        } => {
            let on = match on {
                Some(e) => format_expr(e),
                None => "cross".to_string(),
            };
            let using = if using.is_empty() {
                String::new()
            } else {
                format!(
                    ", using: [{}]",
                    using
                        .iter()
                        .map(|c| c.name.as_str())
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            };
            (
                "Join".to_string(),
                format!("type: {join_type:?}, on: {on}{using}"),
                false,
            )
        }
        LogicalPlan::Sample { method, percent, .. } => {
            ("Sample".to_string(), format!("method: {method}, percent: {percent}"), false)
        }
        LogicalPlan::Assert {
            condition,
            message,
            ..
        } => {
            let msg = message.as_deref().unwrap_or("none");
            (
                "Assert".to_string(),
                format!("condition: {}, message: {msg}", format_expr(condition)),
                false,
            )
        }
        LogicalPlan::Pivot {
            aggregate,
            alias,
            value_column,
            values,
            ..
        } => (
            "Pivot".to_string(),
            format!(
                "aggregate: {}, alias: {:?}, value_column: {}, values: [{}]",
                format_expr(aggregate),
                alias,
                value_column.name,
                format_exprs(values, ", ")
            ),
            false,
        ),
        LogicalPlan::Unpivot {
            value_name,
            name_name,
            columns,
            include_nulls,
            ..
        } => (
            "Unpivot".to_string(),
            format!(
                "value: {value_name}, name: {name_name}, columns: [{}], include_nulls: {include_nulls}",
                columns
                    .iter()
                    .map(|c| c.name.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            false,
        ),
        LogicalPlan::Fork { branches, .. } => {
            ("Fork".to_string(), format!("branches: {}", branches.len()), false)
        }
        LogicalPlan::Tee { branches, .. } => {
            ("Tee".to_string(), format!("branches: {}", branches.len()), false)
        }
        LogicalPlan::SemanticFilter { predicate, alpha, .. } => {
            ("SemanticFilter".to_string(), format!("predicate: \"{predicate}\", alpha: {alpha}"), false)
        }
        LogicalPlan::SemanticJoin { condition, alpha, .. } => {
            ("SemanticJoin".to_string(), format!("condition: \"{condition}\", alpha: {alpha}"), false)
        }
        LogicalPlan::SemanticCalibrate { alpha, .. } => {
            ("SemanticCalibrate".to_string(), format!("alpha: {alpha}"), false)
        }
        LogicalPlan::Write { target, .. } => ("Write".to_string(), format!("target: {target}"), false),
        LogicalPlan::Call { name, args, .. } => (
            "Call".to_string(),
            format!("name: {name}, args: [{}]", format_exprs(args, ", ")),
            false,
        ),
    };

    let mut line = kind;
    if !details.is_empty() {
        line.push_str(&format!(" [{details}]"));
    }
    if show_schema {
        line.push_str(&format!(" schema: {}", format_schema(&plan.schema().fields)));
    }
    line
}

fn format_col(col: &ResolvedColumn) -> String {
    match &col.qualifier {
        Some(q) => format!("{q}.{}", col.name),
        None => col.name.clone(),
    }
}

fn format_literal(lit: &crate::LogicalLiteral) -> String {
    match lit {
        crate::LogicalLiteral::Null(_) => "NULL".to_string(),
        crate::LogicalLiteral::Boolean(b) => b.to_string(),
        crate::LogicalLiteral::Int64(v) => v.to_string(),
        crate::LogicalLiteral::UInt64(v) => v.to_string(),
        crate::LogicalLiteral::Float64(v) => v.to_string(),
        crate::LogicalLiteral::String(s) => format!("\"{}\"", s.escape_default()),
        crate::LogicalLiteral::Date(d) => format!("DATE {d}"),
        crate::LogicalLiteral::Timestamp(ts) => format!("TIMESTAMP {ts}"),
        crate::LogicalLiteral::Time(us) => format!("TIME {us}"),
        crate::LogicalLiteral::Interval { months, days, nanos } => {
            format!("INTERVAL {months}m {days}d {nanos}ns")
        }
        crate::LogicalLiteral::Decimal(v, _, _) => v.to_string(),
        crate::LogicalLiteral::List(items) => {
            let inner = items
                .iter()
                .map(format_literal)
                .collect::<Vec<_>>()
                .join(", ");
            format!("[{inner}]")
        }
        crate::LogicalLiteral::Struct(entries) => {
            let inner = entries
                .iter()
                .map(|(name, v)| format!("{name}: {}", format_literal(v)))
                .collect::<Vec<_>>()
                .join(", ");
            format!("STRUCT({inner})")
        }
        crate::LogicalLiteral::Map(entries) => {
            let inner = entries
                .iter()
                .map(|(k, v)| format!("{}: {}", format_literal(k), format_literal(v)))
                .collect::<Vec<_>>()
                .join(", ");
            format!("MAP({inner})")
        }
        crate::LogicalLiteral::Vector(v) => {
            let inner = v
                .iter()
                .map(|f| f.to_string())
                .collect::<Vec<_>>()
                .join(", ");
            format!("VECTOR[{inner}]")
        }
    }
}

fn op_symbol(op: BinaryOp) -> &'static str {
    match op {
        BinaryOp::Plus => "+",
        BinaryOp::Minus => "-",
        BinaryOp::Multiply => "*",
        BinaryOp::Divide => "/",
        BinaryOp::Modulo => "%",
        BinaryOp::Concat => "||",
        BinaryOp::Eq => "=",
        BinaryOp::Neq => "<>",
        BinaryOp::Lt => "<",
        BinaryOp::Gt => ">",
        BinaryOp::Leq => "<=",
        BinaryOp::Geq => ">=",
        BinaryOp::And => "AND",
        BinaryOp::Or => "OR",
    }
}

fn type_str(ty: &VeinDataType) -> String {
    match ty {
        VeinDataType::Decimal128(p, s) => format!("Decimal128({p}, {s})"),
        VeinDataType::Interval(unit) => format!("Interval({unit:?})"),
        VeinDataType::Timestamp(unit, Some(tz)) => format!("Timestamp({unit:?}, \"{tz}\")"),
        _ => format!("{ty:?}"),
    }
}

fn format_exprs(exprs: &[LogicalExpr], sep: &str) -> String {
    exprs.iter().map(format_expr).collect::<Vec<_>>().join(sep)
}

fn format_udf(kind: &str, bytecode: Option<&str>, args: &[LogicalExpr]) -> String {
    match bytecode {
        Some(ref r) => format!("{kind}(@{r}, {})", format_exprs(args, ", ")),
        None => format!("{kind}({})", format_exprs(args, ", ")),
    }
}

fn format_sort_keys(keys: &[crate::SortKey]) -> String {
    keys.iter()
        .map(|k| {
            let dir = if k.asc { "ASC" } else { "DESC" };
            let nulls = if k.nulls_first { "NULLS FIRST" } else { "NULLS LAST" };
            format!("{} {dir} {nulls}", format_expr(&k.expr))
        })
        .collect::<Vec<_>>()
        .join(", ")
}

fn format_window_exprs(exprs: &[WindowExpr]) -> String {
    exprs
        .iter()
        .map(|w| format!("{} -> {}", format_expr(&w.call), w.output_column.name))
        .collect::<Vec<_>>()
        .join(", ")
}

fn format_window_frame(frame: &WindowFrame) -> String {
    let unit = match frame.kind {
        WindowFrameKind::Rows => "ROWS",
        WindowFrameKind::Range => "RANGE",
        WindowFrameKind::Groups => "GROUPS",
    };
    let start = format_bound(&frame.start);
    let end = frame
        .end
        .as_ref()
        .map(|b| format!(" AND {}", format_bound(b)))
        .unwrap_or_default();
    format!("{unit} BETWEEN {start}{end}")
}

fn format_bound(bound: &WindowFrameBound) -> String {
    match bound {
        WindowFrameBound::UnboundedPreceding => "UNBOUNDED PRECEDING".to_string(),
        WindowFrameBound::Preceding(n) => format!("{n} PRECEDING"),
        WindowFrameBound::CurrentRow => "CURRENT ROW".to_string(),
        WindowFrameBound::Following(n) => format!("{n} FOLLOWING"),
        WindowFrameBound::UnboundedFollowing => "UNBOUNDED FOLLOWING".to_string(),
    }
}

fn format_plan_flat(plan: &LogicalPlan) -> String {
    let mut out = String::new();
    build_lines(plan, "", "", &mut out);
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::LogicalLiteral;
    use crate::{ColumnId, ResolvedColumn};
    use crate::VeinDataType;

    fn col(id: u64, name: &str) -> ResolvedColumnRef {
        std::sync::Arc::new(ResolvedColumn {
            id: ColumnId(id as usize),
            name: name.to_string(),
            qualifier: Some("users".to_string()),
            data_type: VeinDataType::Int64,
            is_factorized: false,
            nullable: false,
        })
    }

    fn scan_plan() -> LogicalPlan {
        let schema = std::sync::Arc::new(crate::VeinSchema {
            fields: vec![col(0, "id"), col(1, "name")],
        });
        LogicalPlan::Scan {
            table: std::sync::Arc::new(crate::TableMetadata::new("users", schema.clone())),
            projection: None,
            filters: vec![],
            schema: Box::new((*schema).clone()),
        }
    }

    #[test]
    fn test_scan_line() {
        let text = format_plan(&scan_plan());
        assert_eq!(text, "Scan [table: users, projection: [none], filters: 0]\n");
    }

    #[test]
    fn test_filter_project_tree() {
        let plan = LogicalPlan::Project {
            input: Box::new(LogicalPlan::Filter {
                input: Box::new(scan_plan()),
                predicate: LogicalExpr::BinaryOp {
                    left: Box::new(LogicalExpr::Column(col(3, "age"))),
                    op: BinaryOp::Gt,
                    right: Box::new(LogicalExpr::Literal(LogicalLiteral::Int64(18))),
                    return_type: VeinDataType::Boolean,
                },
            }),
            exprs: vec![LogicalExpr::Column(col(0, "id"))],
            schema: Box::new(crate::VeinSchema {
                fields: vec![col(0, "id")],
            }),
        };
        let text = format_plan(&plan);
        assert_eq!(
            text,
            "Project [exprs: [users.id]] schema: [id:Int64]\n\
             └── Filter [predicate: (users.age > 18)]\n\
             \x20   └── Scan [table: users, projection: [none], filters: 0]\n"
        );
    }

    #[test]
    fn test_aggregate_line() {
        let plan = LogicalPlan::Aggregate {
            input: Box::new(scan_plan()),
            grouping_sets: vec![vec![LogicalExpr::Column(col(1, "name"))]],
            aggs: vec![LogicalExpr::FunctionCall {
                name: "SUM".to_string(),
                args: vec![LogicalExpr::Column(col(3, "age"))],
                is_aggregate: true,
                distinct: false,
                return_type: VeinDataType::Int64,
            }],
            schema: Box::new(crate::VeinSchema {
                fields: vec![col(1, "name")],
            }),
        };
        let text = format_plan(&plan);
        assert_eq!(
            text,
            "Aggregate [groups: [users.name], aggs: [SUM(users.age)]] schema: [name:Int64]\n\
             └── Scan [table: users, projection: [none], filters: 0]\n"
        );
    }

    #[test]
    fn test_explain_wraps_input() {
        let plan = LogicalPlan::Explain {
            input: Box::new(scan_plan()),
            schema: Box::new(crate::VeinSchema {
                fields: vec![col(0, "id"), col(1, "name")],
            }),
        };
        let text = format_plan(&plan);
        assert_eq!(
            text,
            "Explain\n└── Scan [table: users, projection: [none], filters: 0]\n"
        );
    }

    #[test]
    fn test_window_frame_rendering() {
        let frame = WindowFrame {
            kind: WindowFrameKind::Rows,
            start: WindowFrameBound::UnboundedPreceding,
            end: Some(WindowFrameBound::CurrentRow),
        };
        assert_eq!(
            format_window_frame(&frame),
            "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"
        );
    }

    #[test]
    fn test_expr_display() {
        let e = LogicalExpr::BinaryOp {
            left: Box::new(LogicalExpr::Literal(LogicalLiteral::Int64(10))),
            op: BinaryOp::Plus,
            right: Box::new(LogicalExpr::Literal(LogicalLiteral::Int64(8))),
            return_type: VeinDataType::Int64,
        };
        assert_eq!(format_expr(&e), "(10 + 8)");
        assert_eq!(format_expr(&LogicalExpr::Literal(LogicalLiteral::String("hi".into()))), "\"hi\"");
    }
}

#[cfg(test)]
mod coverage {
    use super::*;
    use crate::{ColumnId, LogicalLiteral, SortKey};
    use parser::ast::pipe_op::JoinType;
    use parser::ast::query::SetOpKind;

    fn col(id: u64, name: &str) -> ResolvedColumnRef {
        std::sync::Arc::new(ResolvedColumn {
            id: ColumnId(id as usize),
            name: name.to_string(),
            qualifier: Some("users".to_string()),
            data_type: VeinDataType::Int64,
            is_factorized: false,
            nullable: false,
        })
    }

    fn lit(v: i64) -> LogicalExpr {
        LogicalExpr::Literal(LogicalLiteral::Int64(v))
    }

    fn scan_plan() -> LogicalPlan {
        let schema = std::sync::Arc::new(crate::VeinSchema {
            fields: vec![col(0, "id"), col(1, "name")],
        });
        LogicalPlan::Scan {
            table: std::sync::Arc::new(crate::TableMetadata::new("users", schema.clone())),
            projection: None,
            filters: vec![],
            schema: Box::new((*schema).clone()),
        }
    }

    fn schema(cols: Vec<ResolvedColumnRef>) -> Box<crate::VeinSchema> {
        Box::new(crate::VeinSchema { fields: cols })
    }

    /// Every `LogicalPlan` variant must render without panicking, starting
    /// with its canonical label and containing its scan. Guards the printer
    /// against new variants.
    #[test]
    fn every_plan_variant_renders() {
        for (plan, label) in all_plan_variants() {
            let text = format_plan(&plan);
            let first = text.lines().next().expect("plan renders at least one line");
            assert!(first.starts_with(&label), "{label}: got {text}");
            assert!(text.contains("Scan"), "{label} scan missing:\n{text}");
        }
    }

    fn all_plan_variants() -> Vec<(LogicalPlan, String)> {
        let scan = scan_plan();
        let base_schema = schema(scan.schema().fields.clone());
        let filter = LogicalPlan::Filter {
            predicate: LogicalExpr::BinaryOp {
                left: Box::new(LogicalExpr::Column(col(3, "age"))),
                op: BinaryOp::Gt,
                right: Box::new(lit(18)),
                return_type: VeinDataType::Boolean,
            },
            input: Box::new(scan_plan()),
        };

        vec![
            (
                LogicalPlan::Scan {
                    table: std::sync::Arc::new(crate::TableMetadata::new(
                        "users",
                        std::sync::Arc::new(*base_schema.clone()),
                    )),
                    projection: None,
                    filters: vec![],
                    schema: base_schema.clone(),
                },
                "Scan".to_string(),
            ),
            (
                LogicalPlan::Filter {
                    input: Box::new(scan_plan()),
                    predicate: LogicalExpr::Literal(LogicalLiteral::Boolean(true)),
                },
                "Filter".to_string(),
            ),
            (
                LogicalPlan::Project {
                    input: Box::new(scan_plan()),
                    exprs: vec![LogicalExpr::Column(col(0, "id"))],
                    schema: base_schema.clone(),
                },
                "Project".to_string(),
            ),
            (
                LogicalPlan::Aggregate {
                    input: Box::new(scan_plan()),
                    grouping_sets: vec![vec![LogicalExpr::Column(col(1, "name"))]],
                    aggs: vec![LogicalExpr::Column(col(0, "id"))],
                    schema: base_schema.clone(),
                },
                "Aggregate".to_string(),
            ),
            (
                LogicalPlan::Window {
                    input: Box::new(scan_plan()),
                    partition_by: vec![LogicalExpr::Column(col(1, "name"))],
                    order_by: vec![SortKey {
                        expr: LogicalExpr::Column(col(3, "age")),
                        asc: false,
                        nulls_first: true,
                    }],
                    frame: Some(WindowFrame {
                        kind: WindowFrameKind::Rows,
                        start: WindowFrameBound::UnboundedPreceding,
                        end: Some(WindowFrameBound::CurrentRow),
                    }),
                    exprs: vec![WindowExpr {
                        call: LogicalExpr::FunctionCall {
                            name: "SUM".to_string(),
                            args: vec![LogicalExpr::Column(col(3, "age"))],
                            is_aggregate: true,
                            distinct: false,
                            return_type: VeinDataType::Int64,
                        },
                        output_column: col(5, "total_age"),
                    }],
                    schema: schema(vec![col(1, "name"), col(5, "total_age")]),
                },
                "Window".to_string(),
            ),
            (
                LogicalPlan::Sort {
                    input: Box::new(scan_plan()),
                    sort_keys: vec![SortKey {
                        expr: LogicalExpr::Column(col(1, "name")),
                        asc: false,
                        nulls_first: true,
                    }],
                    schema: base_schema.clone(),
                },
                "Sort".to_string(),
            ),
            (
                LogicalPlan::Limit {
                    input: Box::new(scan_plan()),
                    offset: 5,
                    count: 10,
                    schema: base_schema.clone(),
                },
                "Limit".to_string(),
            ),
            (
                LogicalPlan::Distinct {
                    input: Box::new(scan_plan()),
                    schema: base_schema.clone(),
                },
                "Distinct".to_string(),
            ),
            (
                LogicalPlan::SubqueryAlias {
                    input: Box::new(scan_plan()),
                    alias: "aliased".to_string(),
                    schema: base_schema.clone(),
                },
                "SubqueryAlias".to_string(),
            ),
            (
                LogicalPlan::Explain {
                    input: Box::new(scan_plan()),
                    schema: base_schema.clone(),
                },
                "Explain".to_string(),
            ),
            (
                LogicalPlan::SetOp {
                    kind: SetOpKind::Union,
                    all: true,
                    left: Box::new(scan_plan()),
                    right: Box::new(scan_plan()),
                    schema: base_schema.clone(),
                },
                "SetOp".to_string(),
            ),
            (
                LogicalPlan::Sample {
                    input: Box::new(scan_plan()),
                    method: "BERNOULLI".to_string(),
                    percent: 1.0,
                    schema: base_schema.clone(),
                },
                "Sample".to_string(),
            ),
            (
                LogicalPlan::Assert {
                    input: Box::new(scan_plan()),
                    condition: lit(1),
                    message: Some("msg".to_string()),
                    schema: base_schema.clone(),
                },
                "Assert".to_string(),
            ),
            (
                LogicalPlan::Fork {
                    branches: vec![Box::new(scan_plan()), Box::new(filter.clone())],
                    schema: base_schema.clone(),
                },
                "Fork".to_string(),
            ),
            (
                LogicalPlan::Tee {
                    branches: vec![Box::new(scan_plan())],
                    schema: base_schema.clone(),
                },
                "Tee".to_string(),
            ),
            (
                LogicalPlan::SemanticFilter {
                    input: Box::new(scan_plan()),
                    predicate: "is vip".to_string(),
                    targets: vec![col(0, "vip")],
                    alpha: 0.05,
                    schema: base_schema.clone(),
                },
                "SemanticFilter".to_string(),
            ),
            (
                LogicalPlan::SemanticJoin {
                    left: Box::new(scan_plan()),
                    right: Box::new(scan_plan()),
                    condition: "a = b".to_string(),
                    bounding_equi: vec![],
                    depends_on: (vec![], vec![]),
                    alpha: 0.1,
                    schema: base_schema.clone(),
                },
                "SemanticJoin".to_string(),
            ),
            (
                LogicalPlan::SemanticCalibrate {
                    input: Box::new(scan_plan()),
                    target_features: vec![],
                    ground_truth_label: col(0, "label"),
                    alpha: 0.1,
                    schema: base_schema.clone(),
                },
                "SemanticCalibrate".to_string(),
            ),
            (
                LogicalPlan::Write {
                    input: Box::new(scan_plan()),
                    target: "kafka://analytics".to_string(),
                    options: vec![],
                    schema: base_schema.clone(),
                },
                "Write".to_string(),
            ),
            (
                LogicalPlan::Call {
                    input: Box::new(scan_plan()),
                    name: "my_udf".to_string(),
                    args: vec![LogicalExpr::Column(col(0, "id"))],
                    schema: base_schema.clone(),
                },
                "Call".to_string(),
            ),
            (
                LogicalPlan::ExternalScan {
                    url: "s3://bucket/data".to_string(),
                    options: vec![],
                    files: vec![],
                    schema: schema(vec![]),
                },
                "ExternalScan".to_string(),
            ),
            (
                LogicalPlan::Join {
                    join_type: JoinType::Inner,
                    left: Box::new(scan_plan()),
                    right: Box::new(scan_plan()),
                    on: None,
                    using: vec![],
                    schema: base_schema.clone(),
                },
                "Join".to_string(),
            ),
            (
                LogicalPlan::Pivot {
                    input: Box::new(scan_plan()),
                    aggregate: LogicalExpr::Column(col(0, "amount")),
                    alias: Some("total".to_string()),
                    value_column: col(0, "category"),
                    values: vec![LogicalExpr::Literal(LogicalLiteral::Int64(1))],
                    schema: base_schema.clone(),
                },
                "Pivot".to_string(),
            ),
            (
                LogicalPlan::Unpivot {
                    input: Box::new(scan_plan()),
                    value_name: "amount".to_string(),
                    name_name: "category".to_string(),
                    columns: vec![col(0, "a"), col(0, "b")],
                    include_nulls: true,
                    schema: base_schema.clone(),
                },
                "Unpivot".to_string(),
            ),
        ]
    }

    /// Spot-checks the detail strings for structural variants.
    #[test]
    fn structural_details_render() {
        let text = format_plan(&LogicalPlan::SetOp {
            kind: SetOpKind::Union,
            all: true,
            left: Box::new(scan_plan()),
            right: Box::new(scan_plan()),
            schema: schema(vec![col(0, "id")]),
        });
        assert!(text.contains("SetOp [Union ALL]"), "got {text}");

        let text = format_plan(&LogicalPlan::Sample {
            input: Box::new(scan_plan()),
            method: "BERNOULLI".to_string(),
            percent: 1.0,
            schema: schema(vec![col(0, "id")]),
        });
        assert!(text.contains("percent: 1"), "got {text}");

        let text = format_plan(&LogicalPlan::Window {
            input: Box::new(scan_plan()),
            partition_by: vec![LogicalExpr::Column(col(1, "name"))],
            order_by: vec![],
            frame: Some(WindowFrame {
                kind: WindowFrameKind::Range,
                start: WindowFrameBound::CurrentRow,
                end: None,
            }),
            exprs: vec![WindowExpr {
                call: LogicalExpr::WindowCall {
                    name: "SUM".to_string(),
                    args: vec![LogicalExpr::Column(col(3, "age"))],
                    is_aggregate: true,
                    distinct: false,
                    partition_by: vec![LogicalExpr::Column(col(1, "name"))],
                    order_by: vec![],
                    frame: None,
                    return_type: VeinDataType::Int64,
                },
                output_column: col(5, "total_age"),
            }],
            schema: schema(vec![col(1, "name"), col(5, "total_age")]),
        });
        assert!(text.contains("RANGE BETWEEN CURRENT ROW"), "got {text}");
        assert!(text.contains("SUM(users.age)"), "got {text}");
    }

    /// Every `LogicalExpr` variant renders without panicking with a
    /// recognizable shape.
    #[test]
    fn every_expr_variant_renders() {
        let col_expr = LogicalExpr::Column(col(0, "id"));
        let bind = |expr: LogicalExpr, needle: &str| {
            let text = format_expr(&expr);
            assert!(text.contains(needle), "got {text}");
        };

        bind(LogicalExpr::OuterColumn(col(0, "id")), "outer:users.id");
        bind(LogicalExpr::Literal(LogicalLiteral::Null(VeinDataType::Int64)), "NULL");
        bind(LogicalExpr::Literal(LogicalLiteral::Boolean(true)), "true");
        bind(LogicalExpr::Literal(LogicalLiteral::Float64(1.5)), "1.5");
        bind(LogicalExpr::Literal(LogicalLiteral::String("a\\b".into())), "\"a\\\\b\"");
        bind(LogicalExpr::Literal(LogicalLiteral::Date(19000)), "DATE 19000");
        bind(LogicalExpr::Literal(LogicalLiteral::Timestamp(123)), "TIMESTAMP 123");
        bind(LogicalExpr::Literal(LogicalLiteral::Time(9_000_000)), "TIME 9000000");
        bind(
            LogicalExpr::Literal(LogicalLiteral::Interval {
                months: 1,
                days: 2,
                nanos: 3,
            }),
            "INTERVAL 1m 2d 3ns",
        );
        bind(LogicalExpr::Literal(LogicalLiteral::Decimal(12345, 76, 2)), "12345");
        bind(
            LogicalExpr::Literal(LogicalLiteral::List(vec![
                LogicalLiteral::Int64(1),
                LogicalLiteral::Int64(2),
            ])),
            "[1, 2]",
        );
        bind(
            LogicalExpr::Literal(LogicalLiteral::Struct(vec![
                ("id".to_string(), LogicalLiteral::Int64(1)),
                ("name".to_string(), LogicalLiteral::String("x".into())),
            ])),
            r#"STRUCT(id: 1, name: "x")"#,
        );
        bind(
            LogicalExpr::Literal(LogicalLiteral::Map(vec![(
                LogicalLiteral::String("k".into()),
                LogicalLiteral::Boolean(true),
            )])),
            r#"MAP("k": true)"#,
        );
        bind(
            LogicalExpr::Literal(LogicalLiteral::Vector(vec![1.0, 2.0, 3.0])),
            "VECTOR[1, 2, 3]",
        );

        let binop = |op, r| LogicalExpr::BinaryOp {
            left: Box::new(lit(10)),
            op,
            right: r,
            return_type: VeinDataType::Int64,
        };
        bind(binop(BinaryOp::Concat, Box::new(LogicalExpr::Literal(LogicalLiteral::String("x".into())))), "||");
        bind(binop(BinaryOp::Leq, Box::new(lit(5))), "<=");
        bind(binop(BinaryOp::And, Box::new(LogicalExpr::Literal(LogicalLiteral::Boolean(true)))), "AND");

        bind(
            LogicalExpr::Cast {
                expr: Box::new(col_expr.clone()),
                target_type: VeinDataType::Utf8,
            },
            "CAST(users.id AS Utf8)",
        );
        bind(LogicalExpr::Not { expr: Box::new(col_expr.clone()) }, "NOT(users.id)");
        bind(LogicalExpr::Negate { expr: Box::new(col_expr.clone()) }, "-users.id");
        bind(LogicalExpr::IsNull { expr: Box::new(col_expr.clone()) }, "IS NULL");
        bind(LogicalExpr::IsNotNull { expr: Box::new(col_expr.clone()) }, "IS NOT NULL");

        bind(
            LogicalExpr::InList {
                expr: Box::new(col_expr.clone()),
                list: vec![lit(18), lit(20)],
                negated: true,
            },
            "users.id NOT IN (18, 20)",
        );
        bind(
            LogicalExpr::Like {
                expr: Box::new(col_expr.clone()),
                pattern: Box::new(LogicalExpr::Literal(LogicalLiteral::String("%a%".into()))),
                negated: true,
            },
            "NOT LIKE",
        );
        bind(
            LogicalExpr::Case {
                when: vec![binop(BinaryOp::Gt, Box::new(lit(18)))],
                then: vec![lit(1)],
                else_: Some(Box::new(lit(0))),
                return_type: VeinDataType::Int64,
            },
            "CASE WHEN (10 > 18) THEN 1 ELSE 0 END",
        );
        bind(
            LogicalExpr::FunctionCall {
                name: "LOWER".to_string(),
                args: vec![col_expr.clone()],
                is_aggregate: false,
                distinct: false,
                return_type: VeinDataType::Utf8,
            },
            "LOWER(users.id)",
        );
        bind(
            LogicalExpr::FunctionCall {
                name: "SUM".to_string(),
                args: vec![col_expr.clone()],
                is_aggregate: true,
                distinct: true,
                return_type: VeinDataType::Int64,
            },
            "SUM(DISTINCT users.id)",
        );
        bind(
            LogicalExpr::WindowCall {
                name: "COUNT".to_string(),
                args: vec![col_expr.clone()],
                is_aggregate: true,
                distinct: true,
                partition_by: vec![],
                order_by: vec![],
                frame: None,
                return_type: VeinDataType::Int64,
            },
            "COUNT(DISTINCT users.id) OVER ()",
        );
        bind(
            LogicalExpr::AiOperator {
                name: "LLM_CLASSIFY".to_string(),
                args: vec![col_expr.clone()],
                return_type: VeinDataType::Utf8,
            },
            "AI:LLM_CLASSIFY(users.id)",
        );
        bind(
            LogicalExpr::WasmUdf {
                bytecode_ref: Some("ref1".to_string()),
                args: vec![col_expr.clone()],
                return_type: VeinDataType::Int64,
            },
            "WASM(@ref1, users.id)",
        );
        bind(
            LogicalExpr::EbpfUdf {
                bytecode_ref: None,
                args: vec![col_expr.clone()],
                return_type: VeinDataType::Int64,
            },
            "EBPF(users.id)",
        );
        bind(
            LogicalExpr::PythonUdf {
                bytecode_ref: None,
                args: vec![col_expr.clone()],
                return_type: VeinDataType::Int64,
            },
            "PYTHON(users.id)",
        );
        bind(
            LogicalExpr::Lambda {
                params: vec![col(4, "x")],
                body: Box::new(LogicalExpr::Column(col(0, "id"))),
                captures: vec![],
            },
            "(x) -> users.id",
        );
        bind(
            LogicalExpr::ScalarSubquery(Box::new(scan_plan())),
            "(scalar subquery Scan [table: users",
        );
        bind(
            LogicalExpr::Exists {
                subquery: Box::new(scan_plan()),
                negated: true,
            },
            "NOT EXISTS",
        );
    }

    /// Rendering a pathologically deep expression must not overflow the stack;
    /// it should truncate with a `...` marker past MAX_DISPLAY_DEPTH. Run on a
    /// large-stack thread so dropping the 100k-deep owned chain is itself safe.
    #[test]
    fn test_deep_expression_does_not_overflow() {
        std::thread::Builder::new()
            .stack_size(256 * 1024 * 1024)
            .spawn(|| {
                // 100k-deep `NOT(NOT(...x))` chain.
                let mut e = LogicalExpr::Column(col(0, "id"));
                for _ in 0..100_000 {
                    e = LogicalExpr::Not {
                        expr: Box::new(e),
                    };
                }
                let text = format_expr(&e);
                assert!(!text.is_empty());
                assert!(
                    text.contains("..."),
                    "deep expression should render a truncation marker"
                );
            })
            .unwrap()
            .join()
            .unwrap();
    }

    /// Rendering a pathologically deep plan tree must not overflow the stack.
    #[test]
    fn test_deep_plan_does_not_overflow() {
        std::thread::Builder::new()
            .stack_size(256 * 1024 * 1024)
            .spawn(|| {
                // 100k-deep chain of single-child Filter nodes.
                let mut plan = scan_plan();
                let lit = LogicalExpr::Literal(LogicalLiteral::Boolean(true));
                for _ in 0..100_000 {
                    plan = LogicalPlan::Filter {
                        input: Box::new(plan),
                        predicate: lit.clone(),
                    };
                }
                let text = format_plan(&plan);
                assert!(!text.is_empty());
                assert!(
                    text.contains("..."),
                    "deep plan should render a truncation marker"
                );
            })
            .unwrap()
            .join()
            .unwrap();
    }
}