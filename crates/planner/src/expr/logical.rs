use crate::schema::{ResolvedColumn, VeinDataType};
use crate::plan::LogicalPlan;
use parser::ast::expr::BinaryOp;
use crate::expr::literal::LogicalLiteral;

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
        expr: Box<LogicalExpr> 
    },
    /// Mathematical negation
    Negate { 
        /// The expression to negate
        expr: Box<LogicalExpr> 
    },
    /// IS NULL check
    IsNull { 
        /// The expression to check
        expr: Box<LogicalExpr> 
    },
    /// IS NOT NULL check
    IsNotNull { 
        /// The expression to check
        expr: Box<LogicalExpr> 
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
        todo!()
    }

    /// Maps children using a mutable function
    pub fn map_children<F>(&self, _f: &mut F) -> Result<LogicalExpr, error::VeinError>
    where
        F: FnMut(LogicalExpr) -> Result<LogicalExpr, error::VeinError>,
    {
        todo!()
    }

    /// Checks if the expression is volatile (prevents constant folding)
    pub fn is_volatile(&self) -> bool {
        todo!()
    }

    /// Checks if the expression can be pushed down to eBPF execution
    pub fn is_ebpf_safe(&self) -> bool {
        todo!()
    }
}
