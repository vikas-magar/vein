//! Aggregate state representation and its focused operations.

use super::*;

mod allocation;
mod column;
mod intermediate;
mod output;
mod update;

pub(super) use column::AggregateStateColumn;
