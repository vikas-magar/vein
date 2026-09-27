use std::collections::HashMap;

use crate::traits::{FunctionKind, FunctionRegistry, FunctionSignature};
use error::{Result, VeinError};
use plan::{VeinDataType, Volatility};

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
            ("EXTRACT", scalar(VeinDataType::Int64, Vec::new())),
            (
                "EMBED",
                FunctionSignature {
                    return_type: VeinDataType::Vector(1536),
                    volatility: Volatility::Immutable,
                    arg_types: vec![VeinDataType::Utf8],
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
        kind: FunctionKind::Aggregate,
        bytecode_ref: None,
    }
}

fn scalar(return_type: VeinDataType, arg_types: Vec<VeinDataType>) -> FunctionSignature {
    FunctionSignature {
        return_type,
        volatility: Volatility::Immutable,
        arg_types,
        kind: FunctionKind::Scalar,
        bytecode_ref: None,
    }
}
