//! Mock function signatures for binder tests.

use crate::traits::{FunctionKind, FunctionRegistry, FunctionSignature};
use error::VeinError;
use plan::{VeinDataType, Volatility};
use std::collections::HashMap;

/// Defines the built-in functions a fresh [`MockFunctionRegistry`] knows.
fn builtin(
    return_type: VeinDataType,
    arg_types: &[VeinDataType],
    kind: FunctionKind,
) -> FunctionSignature {
    let min_arg_count = arg_types.len();
    builtin_with_min_args(return_type, arg_types, kind, min_arg_count)
}

fn builtin_with_min_args(
    return_type: VeinDataType,
    arg_types: &[VeinDataType],
    kind: FunctionKind,
    min_arg_count: usize,
) -> FunctionSignature {
    FunctionSignature {
        return_type,
        volatility: Volatility::Immutable,
        arg_types: arg_types.to_vec(),
        min_arg_count,
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
    /// (`SUM`, `COUNT`, `AVG`, `MIN`, `MAX`, `LOWER`, `ROUND`, temporal extractors,
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
                "SUBSTRING".to_string(),
                builtin_with_min_args(
                    VeinDataType::Utf8,
                    &[VeinDataType::Utf8, VeinDataType::Int64, VeinDataType::Int64],
                    FunctionKind::Scalar,
                    2,
                ),
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
        for name in crate::functions::TEMPORAL_EXTRACTORS {
            functions.insert(
                (*name).to_string(),
                builtin(VeinDataType::Int32, &[], FunctionKind::TemporalExtract),
            );
        }
        functions.insert(
            "SHIFT".to_string(),
            builtin(VeinDataType::Null, &[], FunctionKind::TemporalShift),
        );
        functions.insert(
            "TRUNCATE".to_string(),
            builtin(VeinDataType::Null, &[], FunctionKind::TemporalTruncate),
        );
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
