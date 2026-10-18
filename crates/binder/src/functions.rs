use std::collections::HashMap;

use crate::traits::{FunctionKind, FunctionRegistry, FunctionSignature};
use error::{Result, VeinError};
use plan::{VeinDataType, Volatility};

pub(crate) const TEMPORAL_EXTRACTORS: &[&str] = &[
    "YEAR", "ISOYEAR", "QUARTER", "MONTH", "WEEK", "ISOWEEK", "DAY", "DOW", "DOW1",
    "ISODOW0", "ISODOW", "DOY", "HOUR", "MINUTE", "SECOND", "MILLISECOND", "MICROSECOND",
    "NANOSECOND",
];

/// Registry of built-in function signatures understood by the binder.
#[derive(Default)]
pub struct BuiltinFunctionRegistry {
    functions: HashMap<String, FunctionSignature>,
}

impl BuiltinFunctionRegistry {
    /// Creates a registry populated with the built-in function signatures.
    pub fn new() -> Self {
        let mut registry = Self::default();
        for (name, signature) in [
            ("SUM", aggregate(VeinDataType::Float64)),
            ("COUNT", aggregate(VeinDataType::Int64)),
            ("AVG", aggregate(VeinDataType::Float64)),
            ("MIN", aggregate(VeinDataType::Float64)),
            ("MAX", aggregate(VeinDataType::Float64)),
            ("LOWER", scalar(VeinDataType::Utf8, vec![VeinDataType::Utf8])),
            (
                "ROUND",
                scalar(VeinDataType::Float64, vec![VeinDataType::Float64]),
            ),
            (
                "SUBSTRING",
                scalar_with_min_args(
                    VeinDataType::Utf8,
                    vec![VeinDataType::Utf8, VeinDataType::Int64, VeinDataType::Int64],
                    2,
                ),
            ),
            (
                "EMBED",
                FunctionSignature {
                    return_type: VeinDataType::Vector(1536),
                    volatility: Volatility::Immutable,
                    arg_types: vec![VeinDataType::Utf8],
                    min_arg_count: 1,
                    kind: FunctionKind::AiOperator,
                    bytecode_ref: None,
                },
            ),
            (
                "VECTOR_DISTANCE",
                scalar(
                    VeinDataType::Float64,
                    vec![VeinDataType::Vector(0), VeinDataType::Vector(0)],
                ),
            ),
        ] {
            registry.register(name, signature);
        }
        for name in TEMPORAL_EXTRACTORS {
            registry.register(name, temporal_extract());
        }
        registry.register("SHIFT", temporal_shift());
        registry.register("TRUNCATE", temporal_truncate());
        registry
    }

    /// Registers or replaces a function signature.
    pub fn register(&mut self, name: impl AsRef<str>, signature: FunctionSignature) {
        self.functions
            .insert(name.as_ref().to_ascii_uppercase(), signature);
    }
}

impl FunctionRegistry for BuiltinFunctionRegistry {
    fn resolve(&self, name: &str) -> Result<FunctionSignature> {
        self.functions
            .get(&name.to_ascii_uppercase())
            .cloned()
            .ok_or_else(|| VeinError::not_found(format!("Function '{name}' not found")))
    }
}

fn aggregate(return_type: VeinDataType) -> FunctionSignature {
    FunctionSignature {
        return_type,
        volatility: Volatility::Immutable,
        arg_types: Vec::new(),
        min_arg_count: 0,
        kind: FunctionKind::Aggregate,
        bytecode_ref: None,
    }
}

fn scalar(return_type: VeinDataType, arg_types: Vec<VeinDataType>) -> FunctionSignature {
    let min_arg_count = arg_types.len();
    scalar_with_min_args(return_type, arg_types, min_arg_count)
}

fn scalar_with_min_args(
    return_type: VeinDataType,
    arg_types: Vec<VeinDataType>,
    min_arg_count: usize,
) -> FunctionSignature {
    FunctionSignature {
        return_type,
        volatility: Volatility::Immutable,
        arg_types,
        min_arg_count,
        kind: FunctionKind::Scalar,
        bytecode_ref: None,
    }
}

fn temporal_extract() -> FunctionSignature {
    FunctionSignature {
        return_type: VeinDataType::Int32,
        volatility: Volatility::Immutable,
        arg_types: Vec::new(),
        min_arg_count: 0,
        kind: FunctionKind::TemporalExtract,
        bytecode_ref: None,
    }
}

fn temporal_shift() -> FunctionSignature {
    FunctionSignature {
        return_type: VeinDataType::Null,
        volatility: Volatility::Immutable,
        arg_types: Vec::new(),
        min_arg_count: 0,
        kind: FunctionKind::TemporalShift,
        bytecode_ref: None,
    }
}

fn temporal_truncate() -> FunctionSignature {
    FunctionSignature {
        return_type: VeinDataType::Null,
        volatility: Volatility::Immutable,
        arg_types: Vec::new(),
        min_arg_count: 0,
        kind: FunctionKind::TemporalTruncate,
        bytecode_ref: None,
    }
}
