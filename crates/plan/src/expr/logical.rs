use crate::expr::literal::LogicalLiteral;
use crate::plan::LogicalPlan;
use crate::plan::types::SortKey;
use crate::schema::{ResolvedColumn, VeinDataType};
use parser::ast::expr::BinaryOp;
use parser::ast::pipe_op::WindowFrame;

/// A node in the logical expression tree
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum LogicalExpr {
    // ── Atoms ─────────────────────────────────────────────
    /// A reference to a column in the current scope
    Column(ResolvedColumn),
    /// A reference to a column in an outer scope (Correlated subquery)
    OuterColumn(ResolvedColumn),
    /// A literal value
    Literal(LogicalLiteral),

    // ── Operators ─────────────────────────────────────────
    /// A binary operation
    BinaryOp {
        /// The left operand
        left: Box<LogicalExpr>,
        /// The operator
        op: BinaryOp,
        /// The right operand
        right: Box<LogicalExpr>,
        /// The expected return type
        return_type: VeinDataType,
    },
    /// A type cast
    Cast {
        /// The expression to cast
        expr: Box<LogicalExpr>,
        /// The target type
        target_type: VeinDataType,
    },

    // ── Unary ─────────────────────────────────────────────
    /// Logical NOT
    Not {
        /// The expression to negate
        expr: Box<LogicalExpr>,
    },
    /// Mathematical negation
    Negate {
        /// The expression to negate
        expr: Box<LogicalExpr>,
    },
    /// IS NULL check
    IsNull {
        /// The expression to check
        expr: Box<LogicalExpr>,
    },
    /// IS NOT NULL check
    IsNotNull {
        /// The expression to check
        expr: Box<LogicalExpr>,
    },

    // ── Structured ────────────────────────────────────────
    /// IN list check
    InList {
        /// The expression to check
        expr: Box<LogicalExpr>,
        /// The list of expressions
        list: Vec<LogicalExpr>,
        /// Whether the check is negated (NOT IN)
        negated: bool,
    },
    /// LIKE pattern matching
    Like {
        /// The string expression
        expr: Box<LogicalExpr>,
        /// The pattern expression
        pattern: Box<LogicalExpr>,
        /// Whether the check is negated (NOT LIKE)
        negated: bool,
    },
    /// CASE WHEN statement
    Case {
        /// The WHEN conditions
        when: Vec<LogicalExpr>,
        /// The THEN results
        then: Vec<LogicalExpr>,
        /// The optional ELSE result
        else_: Option<Box<LogicalExpr>>,
        /// The unified return type
        return_type: VeinDataType,
    },

    // ── Unified Function Registry ─────────────────────────
    /// A standard scalar or aggregate function call
    FunctionCall {
        /// Name of the function
        name: String,
        /// Arguments provided to the function
        args: Vec<LogicalExpr>,
        /// Whether the function acts as an aggregate
        is_aggregate: bool,
        /// Whether `DISTINCT` was applied to the arguments (aggregates only)
        distinct: bool,
        /// Return type of the function
        return_type: VeinDataType,
    },
    /// A window function call (`fn(args) OVER (...)`), resolved through the
    /// function registry like any other call (LLD §9). The projection binder
    /// lifts it into a `LogicalPlan::Window` node.
    WindowCall {
        /// Name of the function
        name: String,
        /// Arguments provided to the function
        args: Vec<LogicalExpr>,
        /// Whether the function acts as an aggregate (SUM/AVG/... vs.
        /// navigation functions like ROW_NUMBER)
        is_aggregate: bool,
        /// Whether `DISTINCT` was applied to the arguments (aggregates only)
        distinct: bool,
        /// The PARTITION BY expressions
        partition_by: Vec<LogicalExpr>,
        /// The ORDER BY keys inside OVER
        order_by: Vec<SortKey>,
        /// The optional frame (reuses the parser's `WindowFrame`)
        frame: Option<WindowFrame>,
        /// Return type of the function
        return_type: VeinDataType,
    },
    /// A First-Class AI Operator (e.g. LLM_CLASSIFY)
    AiOperator {
        /// Name of the AI operator
        name: String,
        /// Arguments for the operator
        args: Vec<LogicalExpr>,
        /// Expected return type (cost estimated)
        return_type: VeinDataType,
    },
    /// A WebAssembly User-Defined Function (Safe, fast user-space logic)
    WasmUdf {
        /// Reference to the Wasm bytecode
        bytecode_ref: Option<String>,
        /// Arguments passed to the UDF
        args: Vec<LogicalExpr>,
        /// The explicitly cached return type of the function
        return_type: VeinDataType,
    },
    /// An eBPF User-Defined Function (Kernel-level packet extraction)
    EbpfUdf {
        /// Reference to the eBPF bytecode
        bytecode_ref: Option<String>,
        /// Arguments passed to the UDF
        args: Vec<LogicalExpr>,
        /// The explicitly cached return type of the function
        return_type: VeinDataType,
    },
    /// A Python User-Defined Function (Data science/legacy logic)
    PythonUdf {
        /// Reference to the Python bytecode/script
        bytecode_ref: Option<String>,
        /// Arguments passed to the UDF
        args: Vec<LogicalExpr>,
        /// The explicitly cached return type of the function
        return_type: VeinDataType,
    },

    // ── Lambdas ───────────────────────────────────────────
    /// A lambda expression for higher-order functions
    Lambda {
        /// The parameters bound by the lambda
        params: Vec<ResolvedColumn>,
        /// The body of the lambda
        body: Box<LogicalExpr>,
        /// The variables captured from the outer scope
        captures: Vec<ResolvedColumn>,
    },

    // ── Subqueries ────────────────────────────────────────
    /// A scalar subquery that returns a single value
    ScalarSubquery(Box<LogicalPlan>),
    /// An EXISTS subquery check
    Exists {
        /// The subquery to evaluate
        subquery: Box<LogicalPlan>,
        /// Whether the check is negated (NOT EXISTS)
        negated: bool,
    },
}

/// Maximum expression-nesting depth for recursive analysis (volatility, eBPF
/// safety, etc.). Beyond this the safe default is assumed so a deep tree cannot
/// overflow the stack.
const MAX_VOLATILITY_DEPTH: usize = 100;

/// True for built-in functions that are volatile (may differ between
/// evaluations) or stable (depend on the current transaction/clock).
fn is_volatile_builtin(name: &str) -> bool {
    match name.to_uppercase().as_str() {
        // Volatile: genuinely non-deterministic per evaluation.
        "RANDOM" | "RAND" | "UUID" | "GEN_RANDOM_UUID" | "GENERATE_UUID" | "SETVAL"
        | "PG_TYPEOF" => true,
        // Stable: fixed within a query/transaction, not foldable to a constant.
        "NOW" | "CURRENT_TIMESTAMP" | "CURRENT_DATE" | "CURRENT_TIME" | "LOCALTIMESTAMP"
        | "LOCALTIME" | "CURDATE" | "CURRENT_USER" | "SESSION_USER" | "USER" => true,
        _ => false,
    }
}

impl LogicalExpr {
    /// Returns a list of child expressions for traversal
    pub fn children(&self) -> Vec<&LogicalExpr> {
        match self {
            LogicalExpr::Column(_)
            | LogicalExpr::OuterColumn(_)
            | LogicalExpr::Literal(_)
            | LogicalExpr::ScalarSubquery(_)
            | LogicalExpr::Exists { .. } => vec![],
            LogicalExpr::BinaryOp { left, right, .. } => vec![left.as_ref(), right.as_ref()],
            LogicalExpr::Cast { expr, .. } => vec![expr.as_ref()],
            LogicalExpr::Not { expr }
            | LogicalExpr::Negate { expr }
            | LogicalExpr::IsNull { expr }
            | LogicalExpr::IsNotNull { expr } => vec![expr.as_ref()],
            LogicalExpr::InList { expr, list, .. } => {
                let mut v = vec![expr.as_ref()];
                v.extend(list.iter());
                v
            }
            LogicalExpr::Like { expr, pattern, .. } => vec![expr.as_ref(), pattern.as_ref()],
            LogicalExpr::Case {
                when, then, else_, ..
            } => {
                let mut v = Vec::new();
                v.extend(when.iter());
                v.extend(then.iter());
                if let Some(e) = else_ {
                    v.push(e.as_ref());
                }
                v
            }
            LogicalExpr::FunctionCall { args, .. }
            | LogicalExpr::WindowCall { args, .. }
            | LogicalExpr::AiOperator { args, .. }
            | LogicalExpr::WasmUdf { args, .. }
            | LogicalExpr::EbpfUdf { args, .. }
            | LogicalExpr::PythonUdf { args, .. } => {
                let mut v: Vec<&LogicalExpr> = args.iter().collect();
                if let LogicalExpr::WindowCall {
                    partition_by,
                    order_by,
                    ..
                } = self
                {
                    v.extend(partition_by.iter());
                    v.extend(order_by.iter().map(|k| &k.expr));
                }
                v
            }
            LogicalExpr::Lambda { body, .. } => vec![body.as_ref()],
        }
    }

    /// Maps children using a mutable function, consuming the expression.
    ///
    /// Taking `self` by value is what keeps recursion clone-free: a rewriter
    /// that owns each node moves it into the mapper instead of deep-cloning the
    /// whole subtree at every level.
    pub fn map_children<F>(self, f: &mut F) -> Result<LogicalExpr, error::VeinError>
    where
        F: FnMut(LogicalExpr) -> Result<LogicalExpr, error::VeinError>,
    {
        match self {
            other @ LogicalExpr::Column(_)
            | other @ LogicalExpr::OuterColumn(_)
            | other @ LogicalExpr::Literal(_)
            | other @ LogicalExpr::ScalarSubquery(_)
            | other @ LogicalExpr::Exists { .. } => Ok(other),
            LogicalExpr::BinaryOp {
                left,
                op,
                right,
                return_type,
            } => Ok(LogicalExpr::BinaryOp {
                left: Box::new(f(*left)?),
                op,
                right: Box::new(f(*right)?),
                return_type,
            }),
            LogicalExpr::Cast { expr, target_type } => Ok(LogicalExpr::Cast {
                expr: Box::new(f(*expr)?),
                target_type,
            }),
            LogicalExpr::Not { expr } => Ok(LogicalExpr::Not {
                expr: Box::new(f(*expr)?),
            }),
            LogicalExpr::Negate { expr } => Ok(LogicalExpr::Negate {
                expr: Box::new(f(*expr)?),
            }),
            LogicalExpr::IsNull { expr } => Ok(LogicalExpr::IsNull {
                expr: Box::new(f(*expr)?),
            }),
            LogicalExpr::IsNotNull { expr } => Ok(LogicalExpr::IsNotNull {
                expr: Box::new(f(*expr)?),
            }),
            LogicalExpr::InList {
                expr,
                list,
                negated,
            } => {
                let new_expr = Box::new(f(*expr)?);
                let mut new_list = Vec::with_capacity(list.len());
                for item in list {
                    new_list.push(f(item)?);
                }
                Ok(LogicalExpr::InList {
                    expr: new_expr,
                    list: new_list,
                    negated,
                })
            }
            LogicalExpr::Like {
                expr,
                pattern,
                negated,
            } => Ok(LogicalExpr::Like {
                expr: Box::new(f(*expr)?),
                pattern: Box::new(f(*pattern)?),
                negated,
            }),
            LogicalExpr::Case {
                when,
                then,
                else_,
                return_type,
            } => {
                let mut new_when = Vec::with_capacity(when.len());
                for w in when {
                    new_when.push(f(w)?);
                }
                let mut new_then = Vec::with_capacity(then.len());
                for t in then {
                    new_then.push(f(t)?);
                }
                let new_else = match else_ {
                    Some(e) => Some(Box::new(f(*e)?)),
                    None => None,
                };
                Ok(LogicalExpr::Case {
                    when: new_when,
                    then: new_then,
                    else_: new_else,
                    return_type,
                })
            }
            LogicalExpr::FunctionCall {
                name,
                args,
                is_aggregate,
                distinct,
                return_type,
            } => {
                let mut new_args = Vec::with_capacity(args.len());
                for a in args {
                    new_args.push(f(a)?);
                }
                Ok(LogicalExpr::FunctionCall {
                    name,
                    args: new_args,
                    is_aggregate,
                    distinct,
                    return_type,
                })
            }
            LogicalExpr::WindowCall {
                name,
                args,
                is_aggregate,
                distinct,
                partition_by,
                order_by,
                frame,
                return_type,
            } => {
                let mut new_args = Vec::with_capacity(args.len());
                for a in args {
                    new_args.push(f(a)?);
                }
                let mut new_partition = Vec::with_capacity(partition_by.len());
                for p in partition_by {
                    new_partition.push(f(p)?);
                }
                let mut new_order = Vec::with_capacity(order_by.len());
                for key in order_by {
                    new_order.push(SortKey {
                        expr: f(key.expr)?,
                        asc: key.asc,
                        nulls_first: key.nulls_first,
                    });
                }
                Ok(LogicalExpr::WindowCall {
                    name,
                    args: new_args,
                    is_aggregate,
                    distinct,
                    partition_by: new_partition,
                    order_by: new_order,
                    frame,
                    return_type,
                })
            }
            LogicalExpr::AiOperator {
                name,
                args,
                return_type,
            } => {
                let mut new_args = Vec::with_capacity(args.len());
                for a in args {
                    new_args.push(f(a)?);
                }
                Ok(LogicalExpr::AiOperator {
                    name,
                    args: new_args,
                    return_type,
                })
            }
            LogicalExpr::WasmUdf {
                bytecode_ref,
                args,
                return_type,
            } => {
                let mut new_args = Vec::with_capacity(args.len());
                for a in args {
                    new_args.push(f(a)?);
                }
                Ok(LogicalExpr::WasmUdf {
                    bytecode_ref,
                    args: new_args,
                    return_type,
                })
            }
            LogicalExpr::EbpfUdf {
                bytecode_ref,
                args,
                return_type,
            } => {
                let mut new_args = Vec::with_capacity(args.len());
                for a in args {
                    new_args.push(f(a)?);
                }
                Ok(LogicalExpr::EbpfUdf {
                    bytecode_ref,
                    args: new_args,
                    return_type,
                })
            }
            LogicalExpr::PythonUdf {
                bytecode_ref,
                args,
                return_type,
            } => {
                let mut new_args = Vec::with_capacity(args.len());
                for a in args {
                    new_args.push(f(a)?);
                }
                Ok(LogicalExpr::PythonUdf {
                    bytecode_ref,
                    args: new_args,
                    return_type,
                })
            }
            LogicalExpr::Lambda {
                params,
                body,
                captures,
            } => Ok(LogicalExpr::Lambda {
                params,
                body: Box::new(f(*body)?),
                captures,
            }),
        }
    }

    /// Collects the `LogicalPlan`s embedded in subquery expressions
    /// (`ScalarSubquery`/`Exists`) anywhere inside this expression tree.
    ///
    /// Subquery bodies are plans, not expressions, so they are deliberately
    /// opaque to [`children`]/[`map_children`]. This is the narrow, explicit
    /// accessor for reaching them (mirroring how DataFusion/RisingWave inspect
    /// `Subquery` plans from rules rather than descending through generic
    /// expression traversal).
    ///
    /// [`children`]: LogicalExpr::children
    /// [`map_children`]: LogicalExpr::map_children
    pub fn subquery_plans(&self) -> Vec<&LogicalPlan> {
        let mut out = Vec::new();
        self.collect_subquery_plans(&mut out);
        out
    }

    fn collect_subquery_plans<'a>(&'a self, out: &mut Vec<&'a LogicalPlan>) {
        match self {
            LogicalExpr::ScalarSubquery(plan) => out.push(plan.as_ref()),
            LogicalExpr::Exists { subquery, .. } => out.push(subquery.as_ref()),
            _ => {
                for child in self.children() {
                    child.collect_subquery_plans(out);
                }
            }
        }
    }

    /// Checks if the expression is volatile (prevents constant folding).
    ///
    /// Volatility is decided conservatively: anything that cannot be *proven*
    /// deterministic is treated as volatile so the optimizer never folds a
    /// value that may differ between evaluations. This covers the standard
    /// time/random builtins, window calls, AI operators, and user-defined
    /// functions (whose volatility is only known to the binder's registry).
    pub fn is_volatile(&self) -> bool {
        Self::is_volatile_depth(self, 0)
    }

    /// Depth-limited recursion so a pathologically deep expression tree cannot
    /// overflow the stack while checking volatility.
    fn is_volatile_depth(expr: &LogicalExpr, depth: usize) -> bool {
        if depth >= MAX_VOLATILITY_DEPTH {
            // Assume volatile past the limit: the safe default.
            return true;
        }
        match expr {
            LogicalExpr::FunctionCall { name, .. } => is_volatile_builtin(name),
            // Window calls span a frame/partition and are not constant-foldable.
            LogicalExpr::WindowCall { .. } => true,
            // AI operators and UDFs have unknown volatility without a registry.
            LogicalExpr::AiOperator { .. }
            | LogicalExpr::WasmUdf { .. }
            | LogicalExpr::EbpfUdf { .. }
            | LogicalExpr::PythonUdf { .. } => true,
            // Subquery bodies may themselves be volatile; be safe.
            LogicalExpr::ScalarSubquery(_) | LogicalExpr::Exists { .. } => true,
            _ => expr
                .children()
                .iter()
                .any(|c| Self::is_volatile_depth(c, depth + 1)),
        }
    }

    /// Checks if the expression can be pushed down to eBPF execution.
    pub fn is_ebpf_safe(&self) -> bool {
        Self::is_ebpf_safe_depth(self, 0)
    }

    /// Depth-limited recursion for eBPF safety checks.
    fn is_ebpf_safe_depth(expr: &LogicalExpr, depth: usize) -> bool {
        if depth >= MAX_VOLATILITY_DEPTH {
            return false;
        }
        match expr {
            LogicalExpr::PythonUdf { .. }
            | LogicalExpr::AiOperator { .. }
            | LogicalExpr::WasmUdf { .. } => false,
            LogicalExpr::ScalarSubquery(_) | LogicalExpr::Exists { .. } => false,
            _ => expr
                .children()
                .iter()
                .all(|c| Self::is_ebpf_safe_depth(c, depth + 1)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expr::LogicalLiteral;
    use crate::schema::VeinDataType;

    fn func(name: &str) -> LogicalExpr {
        LogicalExpr::FunctionCall {
            name: name.to_string(),
            args: vec![],
            is_aggregate: false,
            distinct: false,
            return_type: VeinDataType::Float64,
        }
    }

    #[test]
    fn volatile_and_stable_builtins_are_detected() {
        assert!(func("random").is_volatile());
        assert!(func("RAND").is_volatile());
        assert!(func("uuid").is_volatile());
        assert!(func("NOW").is_volatile());
        assert!(func("CURRENT_TIMESTAMP").is_volatile());
        assert!(func("curdate").is_volatile());
    }

    #[test]
    fn immutable_function_is_not_volatile() {
        assert!(!func("ABS").is_volatile());
        assert!(!func("length").is_volatile());
    }

    #[test]
    fn volatility_recurses_into_children() {
        // A volatile call buried inside a binary expression must be found.
        let inner = LogicalExpr::BinaryOp {
            left: Box::new(LogicalExpr::Literal(LogicalLiteral::Int64(1))),
            op: BinaryOp::Plus,
            right: Box::new(func("random")),
            return_type: VeinDataType::Float64,
        };
        assert!(inner.is_volatile());
    }

    #[test]
    fn udfs_window_and_ai_ops_are_conservatively_volatile() {
        assert!(LogicalExpr::WindowCall {
            name: "row_number".to_string(),
            args: vec![],
            is_aggregate: false,
            distinct: false,
            partition_by: vec![],
            order_by: vec![],
            frame: None,
            return_type: VeinDataType::Int64,
        }
            .is_volatile());
        assert!(LogicalExpr::AiOperator {
            name: "llm_classify".to_string(),
            args: vec![],
            return_type: VeinDataType::Utf8,
        }
            .is_volatile());
        assert!(LogicalExpr::WasmUdf {
            bytecode_ref: None,
            args: vec![],
            return_type: VeinDataType::Int64,
        }
            .is_volatile());
    }

    /// A pathologically deep expression must not overflow the stack while
    /// checking volatility; past MAX_VOLATILITY_DEPTH it assumes volatile.
    #[test]
    fn deep_expression_volatility_does_not_overflow() {
        std::thread::Builder::new()
            .stack_size(256 * 1024 * 1024)
            .spawn(|| {
                let mut e = LogicalExpr::Literal(LogicalLiteral::Int64(1));
                for _ in 0..200_000 {
                    e = LogicalExpr::Not {
                        expr: Box::new(e),
                    };
                }
                // Not is not volatile itself, so the only way this returns true
                // without recursing 200k deep is the depth guard's safe default.
                assert!(e.is_volatile());
            })
            .unwrap()
            .join()
            .unwrap();
    }
}
