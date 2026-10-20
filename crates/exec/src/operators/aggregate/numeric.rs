//! Typed numeric aggregate inputs, accumulators, and selected-row folding.

use super::*;

mod accumulator;
mod column;
mod fold;
mod input;
mod value;

pub(super) use accumulator::NumericAccumulator;
pub(super) use fold::{GroupRows, average_values, numeric_values, update_numeric_states};
pub(super) use input::AggregateInput;
pub(super) use value::AggregateValue;
