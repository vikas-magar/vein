use std::fmt;
use std::sync::Arc;

use crate::{
    AggregateFunction, FunctionImplementation, FunctionSignature, ScalarFunction,
};

/// A named function signature and its runtime implementation.
#[derive(Clone)]
pub struct FunctionDefinition {
    name: String,
    signature: FunctionSignature,
    implementation: FunctionImplementation,
}

impl FunctionDefinition {
    /// Defines a function backed by a scalar implementation.
    pub fn scalar<F>(name: impl Into<String>, signature: FunctionSignature, function: F) -> Self
    where
        F: ScalarFunction + 'static,
    {
        Self {
            name: name.into(),
            signature,
            implementation: FunctionImplementation::Scalar(Arc::new(function)),
        }
    }

    /// Defines an aggregate backed by an extensible vectorized kernel.
    pub fn aggregate<F>(
        name: impl Into<String>,
        signature: FunctionSignature,
        function: F,
    ) -> Self
    where
        F: AggregateFunction + 'static,
    {
        Self {
            name: name.into(),
            signature,
            implementation: FunctionImplementation::Aggregate(Arc::new(function)),
        }
    }

    /// Defines a special function lowered by another engine stage.
    pub fn catalog_only(name: impl Into<String>, signature: FunctionSignature) -> Self {
        Self {
            name: name.into(),
            signature,
            implementation: FunctionImplementation::CatalogOnly,
        }
    }

    /// Canonical name used for registry lookup.
    pub fn name(&self) -> &str { &self.name }
    /// Semantic signature used by the binder.
    pub fn signature(&self) -> &FunctionSignature { &self.signature }
    /// Runtime implementation associated with this entry.
    pub fn implementation(&self) -> &FunctionImplementation { &self.implementation }
}

impl fmt::Debug for FunctionDefinition {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FunctionDefinition")
            .field("name", &self.name)
            .field("signature", &self.signature)
            .field("implementation", &self.implementation.kind_name())
            .finish()
    }
}
