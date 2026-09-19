use async_trait::async_trait;
use error::Result;
use parser::ast::query::QueryNode;
use plan::{LogicalPlan, VeinDataType, Volatility};

/// Supported runtimes for User-Defined Functions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RuntimeKind {
    /// WebAssembly runtime (Safe, fast user-space logic)
    Wasm,
    /// eBPF runtime (Ultra-fast kernel-space extraction)
    Ebpf,
    /// Python runtime (Legacy data-science support)
    Python,
}

/// The kind of function being resolved.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FunctionKind {
    /// Standard SQL scalar function
    Scalar,
    /// Standard SQL aggregate function
    Aggregate,
    /// First-Class AI Operator (requires cost estimation)
    AiOperator,
    /// Custom User Defined Function targeting a specific runtime
    UserDefined(RuntimeKind),
}

/// Represents a resolved function signature from the registry.
#[derive(Debug, Clone, PartialEq)]
pub struct FunctionSignature {
    /// The return type of the function
    pub return_type: VeinDataType,
    /// The volatility of the function
    pub volatility: Volatility,
    /// The expected argument types
    pub arg_types: Vec<VeinDataType>,
    /// The execution kind and runtime of the function
    pub kind: FunctionKind,
    /// A reference string/path to the compiled bytecode (if it's a UDF)
    pub bytecode_ref: Option<String>,
}

/// Registry for resolving scalar, window, and aggregate functions.
pub trait FunctionRegistry: Send + Sync {
    /// Resolves any function (UDF, AI, Scalar) by name.
    fn resolve(&self, name: &str) -> Result<FunctionSignature>;
}

/// The core logical binder trait.
///
/// Binds an AST [`QueryNode`] into a validated, type-coerced [`LogicalPlan`].
/// The binder is a semantic frontend only: it resolves columns, types, scopes
/// and functions; plan optimization happens in a later pipeline stage.
#[async_trait]
pub trait LogicalBinder: Send + Sync {
    /// Binds a parsed statement into a validated, type-coerced LogicalPlan.
    /// `EXPLAIN` statements yield a [`LogicalPlan::Explain`] wrapper over the
    /// bound inner query.
    async fn bind<'a>(&self, node: &'a QueryNode<'a>) -> Result<LogicalPlan>;

    /// Returns the underlying catalog, if available.
    fn catalog(&self) -> Option<&std::sync::Arc<dyn vcs::TableCatalog>> {
        None
    }
}