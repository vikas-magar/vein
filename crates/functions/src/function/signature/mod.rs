use types::VeinDataType;

use super::FunctionKind;

/// Whether a function can change its output for equal arguments.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Volatility {
    /// Always returns the same result for the same arguments.
    Immutable,
    /// Returns the same result within one query execution.
    Stable,
    /// May return a different result for equal arguments.
    Volatile,
}

/// Function signature used for semantic binding.
#[derive(Debug, Clone, PartialEq)]
pub struct FunctionSignature {
    /// Declared return type, or a placeholder for polymorphic functions.
    pub return_type: VeinDataType,
    /// Whether the function result can be reused or reordered.
    pub volatility: Volatility,
    /// Declared argument types, including optional trailing arguments.
    pub arg_types: Vec<VeinDataType>,
    /// Minimum argument count accepted by the definition.
    pub min_arg_count: usize,
    /// Semantic class used by binding and planning.
    pub kind: FunctionKind,
    /// External runtime module or bytecode reference, when applicable.
    pub bytecode_ref: Option<String>,
}
