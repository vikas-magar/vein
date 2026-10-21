use std::sync::Arc;

use crate::{AggregateFunction, ScalarFunction};

/// Runtime implementation attached to a function definition.
#[derive(Clone)]
pub enum FunctionImplementation {
    /// Vectorized Arrow scalar kernel.
    Scalar(Arc<dyn ScalarFunction>),
    /// Vectorized aggregate kernel with an owned, mergeable state contract.
    Aggregate(Arc<dyn AggregateFunction>),
    /// A bind-time or planner-only definition with no scalar kernel.
    CatalogOnly,
}

impl FunctionImplementation {
    pub(super) fn kind_name(&self) -> &'static str {
        match self {
            Self::Scalar(_) => "scalar",
            Self::Aggregate(_) => "aggregate",
            Self::CatalogOnly => "catalog_only",
        }
    }
}
