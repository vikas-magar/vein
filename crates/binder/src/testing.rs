use crate::pipe::query::QueryBinder;
use crate::traits::{
    FunctionKind, FunctionRegistry, FunctionSignature, LogicalBinder,
};
use error::VeinError;
use parser::parser::Parser;
use plan::{LogicalPlan, VeinDataType, Volatility};
use std::collections::HashMap;
use std::sync::Arc;
use vcs::InMemoryCatalog;

/// Defines the built-in functions a fresh [`MockFunctionRegistry`] knows.
fn builtin(
    return_type: VeinDataType,
    arg_types: &[VeinDataType],
    kind: FunctionKind,
) -> FunctionSignature {
    FunctionSignature {
        return_type,
        volatility: Volatility::Immutable,
        arg_types: arg_types.to_vec(),
        kind,
        bytecode_ref: None,
    }
}

/// A mock function registry for testing: a registration-driven map of
/// functions, pre-populated with the built-ins the test corpus uses.
///
/// Functions resolve by name (case-insensitive) through the map; unknown
/// names yield [`VeinError::NotFound`].
pub struct MockFunctionRegistry {
    functions: HashMap<String, FunctionSignature>,
}

impl MockFunctionRegistry {
    /// Creates a registry pre-populated with the test-corpus built-ins
    /// (`SUM`, `COUNT`, `AVG`, `MIN`, `MAX`, `LOWER`, `ROUND`, `EXTRACT`,
    /// plus the AI/embedding mocks `EMBED` and `VECTOR_DISTANCE`).
    pub fn new() -> Self {
        let mut functions = HashMap::new();
        for (name, sig) in [
            (
                "SUM".to_string(),
                builtin(VeinDataType::Float64, &[], FunctionKind::Aggregate),
            ),
            (
                "COUNT".to_string(),
                builtin(VeinDataType::Int64, &[], FunctionKind::Aggregate),
            ),
            (
                "AVG".to_string(),
                builtin(VeinDataType::Float64, &[], FunctionKind::Aggregate),
            ),
            (
                "MIN".to_string(),
                builtin(VeinDataType::Float64, &[], FunctionKind::Aggregate),
            ),
            (
                "MAX".to_string(),
                builtin(VeinDataType::Float64, &[], FunctionKind::Aggregate),
            ),
            (
                "LOWER".to_string(),
                builtin(
                    VeinDataType::Utf8,
                    &[VeinDataType::Utf8],
                    FunctionKind::Scalar,
                ),
            ),
            (
                "ROUND".to_string(),
                builtin(
                    VeinDataType::Float64,
                    &[VeinDataType::Float64],
                    FunctionKind::Scalar,
                ),
            ),
            (
                "EXTRACT".to_string(),
                builtin(VeinDataType::Int64, &[], FunctionKind::Scalar),
            ),
            (
                "EMBED".to_string(),
                builtin(
                    VeinDataType::Vector(1536),
                    &[VeinDataType::Utf8],
                    FunctionKind::AiOperator,
                ),
            ),
            (
                "VECTOR_DISTANCE".to_string(),
                builtin(
                    VeinDataType::Float64,
                    &[VeinDataType::Vector(0), VeinDataType::Vector(0)],
                    FunctionKind::Scalar,
                ),
            ),
        ] {
            functions.insert(name, sig);
        }
        Self { functions }
    }

    /// Registers an additional function, overriding any existing entry.
    pub fn register(&mut self, name: impl Into<String>, signature: FunctionSignature) {
        self.functions.insert(name.into().to_uppercase(), signature);
    }
}

impl Default for MockFunctionRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl FunctionRegistry for MockFunctionRegistry {
    fn resolve(&self, name: &str) -> Result<FunctionSignature, VeinError> {
        self.functions
            .get(&name.to_uppercase())
            .cloned()
            .ok_or_else(|| VeinError::not_found(format!("Function '{name}' not found in registry")))
    }
}

/// Binds a SQL string end-to-end: parse → bind with the in-memory catalog and
/// the pre-populated mock registry. Returns a fully-typed logical plan or a
/// typed `VeinError`; never panics on user input.
pub async fn plan(sql: &str) -> error::Result<LogicalPlan> {
    let node = Parser::new(sql)
        .parse_statement()
        .map_err(|e| VeinError::semantic(format!("parse error: {e}"), parse_error_span(&e)))?;

    QueryBinder::new(
        Arc::new(InMemoryCatalog::new()),
        Arc::new(MockFunctionRegistry::new()),
    )
    .bind(&node)
    .await
}

/// Extracts the source span of a parse error, when the variant carries one.
fn parse_error_span(err: &parser::parser::ParseError) -> Option<parser::token::Span> {
    use parser::parser::ParseError;
    match err {
        ParseError::Expected { span, .. }
        | ParseError::Unexpected { span, .. }
        | ParseError::NumberParse { span, .. } => Some(*span),
        ParseError::Lexical(_)
        | ParseError::Unsupported(_)
        | ParseError::RecursionLimitExceeded { .. } => None,
    }
}