use crate::expr::literal::LogicalLiteral;
use crate::plan::LogicalPlan;
use crate::schema::{ResolvedColumn, VeinDataType};
use parser::ast::expr::BinaryOp;

/// A node in the logical expression tree
#[derive(Debug, Clone, PartialEq)]
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
            | LogicalExpr::AiOperator { args, .. }
            | LogicalExpr::WasmUdf { args, .. }
            | LogicalExpr::EbpfUdf { args, .. }
            | LogicalExpr::PythonUdf { args, .. } => args.iter().collect(),
            LogicalExpr::Lambda { body, .. } => vec![body.as_ref()],
        }
    }

    /// Maps children using a mutable function
    pub fn map_children<F>(&self, f: &mut F) -> Result<LogicalExpr, error::VeinError>
    where
        F: FnMut(LogicalExpr) -> Result<LogicalExpr, error::VeinError>,
    {
        match self.clone() {
            LogicalExpr::Column(_)
            | LogicalExpr::OuterColumn(_)
            | LogicalExpr::Literal(_)
            | LogicalExpr::ScalarSubquery(_)
            | LogicalExpr::Exists { .. } => Ok(self.clone()),
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

    /// Checks if the expression is volatile (prevents constant folding)
    pub fn is_volatile(&self) -> bool {
        if let LogicalExpr::FunctionCall { name, .. } = self {
            let n = name.to_uppercase();
            if n == "RANDOM" || n == "NOW" {
                return true;
            }
        }
        self.children().iter().any(|c| c.is_volatile())
    }

    /// Checks if the expression can be pushed down to eBPF execution
    pub fn is_ebpf_safe(&self) -> bool {
        match self {
            LogicalExpr::PythonUdf { .. }
            | LogicalExpr::AiOperator { .. }
            | LogicalExpr::WasmUdf { .. } => false,
            LogicalExpr::ScalarSubquery(_) | LogicalExpr::Exists { .. } => false,
            _ => self.children().iter().all(|c| c.is_ebpf_safe()),
        }
    }
}
