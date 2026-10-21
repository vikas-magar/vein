//! Regression tests for aggregate hashing, state, spill, and pipeline behavior.

use std::sync::Arc;

use arrow::array::{
    Date32Array, Decimal256Array, DictionaryArray, Float16Array, Float32Array, Float64Array, Int32Array,
    Int64Array, StringArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use arrow::datatypes::{Int32Type, i256};
use common::arrow::SelectionVector;
use crate::pipeline::test_support::materialized_column;

use super::*;

mod streaming;
mod numeric_decimal;
mod numeric_float;
mod numeric_integer;
mod grouping;
mod memory;
mod intermediate;
mod spill;
mod distinct;
mod distinct_view;
