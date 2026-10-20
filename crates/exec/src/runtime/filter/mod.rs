//! Runtime filter contracts, scan bindings, and join-side filter wiring.
//!
//! Filters are built from execution-local evidence, then bound to source schema
//! positions by an adapter. Payload arrays remain borrowed; only masks are made.

mod contract;
mod policy;
mod set;
mod join_runtime_filter;
mod join_runtime_filter_context;

pub use contract::{RuntimeFilter, RuntimeFilterBuildInput, RuntimeFilterFactory};
pub use policy::{
    ConservativeRuntimeFilterPolicy, RuntimeFilterDecisionContext, RuntimeFilterDecisionPolicy,
    RuntimeFilterPruningDiagnostic,
};
pub use set::{RuntimeFilterBinding, RuntimeFilterSet};

#[cfg(test)]
use std::sync::Arc;
#[cfg(test)]
use common::Result;
#[cfg(test)]
use arrow::array::{Array, BooleanArray, builder::BooleanBuilder};
#[cfg(test)]
use arrow::record_batch::RecordBatch;
#[cfg(test)]
use error::VeinError;
#[cfg(test)]
use types::{ColumnId, VeinSchema};

#[cfg(test)]
mod tests;
