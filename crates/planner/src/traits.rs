use crate::plan::LogicalPlan;
use crate::schema::{VeinDataType, VeinSchema};
use crate::expr::Volatility;
use parser::ast::query::Query;
use std::sync::Arc;
use async_trait::async_trait;

/// Supported runtimes for User-Defined Functions
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RuntimeKind {
    /// WebAssembly runtime (Safe, fast user-space logic)
    Wasm,
    /// eBPF runtime (Ultra-fast kernel-space extraction)
    Ebpf,
    /// Python runtime (Legacy data-science support)
    Python,
}

/// The kind of function being resolved
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

/// Represents a resolved function signature from the registry
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

/// Registry for resolving scalar, window, and aggregate functions
pub trait FunctionRegistry: Send + Sync {
    /// Resolves any function (UDF, AI, Scalar) by name.
    fn resolve(&self, name: &str) -> Result<FunctionSignature, error::VeinError>;
}

/// Provider for resolving table schemas asynchronously
#[async_trait]
pub trait SchemaProvider: Send + Sync {
    /// Asynchronously fetches a table's schema (e.g. from Iceberg/S3).
    async fn get_table_schema(&self, name: &str) -> Result<Arc<VeinSchema>, error::VeinError>;
}

/// The core logical binder trait
#[async_trait]
pub trait LogicalBinder: Send + Sync {
    /// Binds an AST Query into a validated, type-coerced LogicalPlan.
    async fn bind<'a>(&self, query: &'a Query<'a>) -> Result<LogicalPlan, error::VeinError>;
}

/// Interface for the ADOPT reinforcement learning join optimizer
pub trait JoinOptimizer: Send + Sync {
    /// Optimizes the join ordering for a given plan
    fn optimize_join(&self, plan: &LogicalPlan) -> common::Result<LogicalPlan>;
    /// Reports runtime telemetry for RL agent training
    fn report_telemetry(&self) { todo!() }
}
