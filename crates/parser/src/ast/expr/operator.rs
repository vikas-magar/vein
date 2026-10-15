/// A binary operator.
#[derive(Debug, Clone, Copy, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum BinaryOp {
    /// `+`
    Plus,
    /// `-`
    Minus,
    /// `*`
    Multiply,
    /// `/`
    Divide,
    /// `%`
    Modulo,
    /// `||`
    Concat,
    /// `=`
    Eq,
    /// `<>`
    Neq,
    /// `<`
    Lt,
    /// `>`
    Gt,
    /// `<=`
    Leq,
    /// `>=`
    Geq,
    /// `AND`
    And,
    /// `OR`
    Or,
}

/// A unary operator.
#[derive(Debug, Clone, Copy, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum UnaryOp {
    /// `NOT`
    Not,
    /// unary `-`
    Negate,
    /// unary `+`
    Plus,
    /// `IS NULL`
    IsNull,
    /// `IS NOT NULL`
    IsNotNull,
}
