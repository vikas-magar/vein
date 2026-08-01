use arrow::datatypes::DataType;
use parser::ast::expr::BinaryOp;
// We will need a way to represent literal values that are type-aware. 
// For now, we can redefine a logical literal or use an Arrow scalar.
// Let's define a simple enum for now.

/// A fully resolved column reference.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ResolvedColumn {
    /// The name of the column (e.g., "age")
    pub name: String,
    /// The index of the column in the physical schema
    pub index: usize,
    /// The resolved data type of the column
    pub data_type: DataType,
}

/// A resolved literal value.
#[derive(Debug, Clone, PartialEq)]
pub enum LogicalLiteral {
    /// Null value of a specific type.
    Null(DataType),
    /// Boolean value.
    Boolean(bool),
    /// 64-bit integer value.
    Int64(i64),
    /// 64-bit floating point value.
    Float64(f64),
    /// UTF-8 string value.
    String(String),
}

impl LogicalLiteral {
    /// Returns the resolved data type of this literal.
    pub fn data_type(&self) -> DataType {
        match self {
            LogicalLiteral::Null(dt) => dt.clone(),
            LogicalLiteral::Boolean(_) => DataType::Boolean,
            LogicalLiteral::Int64(_) => DataType::Int64,
            LogicalLiteral::Float64(_) => DataType::Float64,
            LogicalLiteral::String(_) => DataType::Utf8,
        }
    }
}

/// A fully resolved, type-aware scalar expression.
#[derive(Debug, Clone, PartialEq)]
pub enum LogicalExpr {
    /// A reference to a column in the input schema.
    Column(ResolvedColumn),
    
    /// A constant scalar value.
    Literal(LogicalLiteral),
    
    /// A binary mathematical or logical operation.
    BinaryOp {
        /// The left-hand side expression.
        left: Box<LogicalExpr>,
        /// The binary operator.
        op: BinaryOp,
        /// The right-hand side expression.
        right: Box<LogicalExpr>,
        /// The return type after resolving left and right.
        return_type: DataType,
    },
    
    /// An explicit type cast (often injected by the TypeCoercion optimizer rule).
    Cast {
        /// The expression to cast.
        expr: Box<LogicalExpr>,
        /// The target data type.
        target_type: DataType,
    },
    
    /// A scalar function invocation (e.g., UDF).
    ScalarFunction {
        /// The name of the function.
        name: String,
        /// The arguments to the function.
        args: Vec<LogicalExpr>,
        /// The return type of the function.
        return_type: DataType,
    },
}

impl LogicalExpr {
    /// Returns the resolved data type of this expression.
    pub fn data_type(&self) -> DataType {
        match self {
            LogicalExpr::Column(c) => c.data_type.clone(),
            LogicalExpr::Literal(l) => l.data_type(),
            LogicalExpr::BinaryOp { return_type, .. } => return_type.clone(),
            LogicalExpr::Cast { target_type, .. } => target_type.clone(),
            LogicalExpr::ScalarFunction { return_type, .. } => return_type.clone(),
        }
    }
}
