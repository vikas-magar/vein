use super::*;
use std::io::Cursor;

use arrow::array::{Int64Array, StringArray};
use crate::spill::IpcRunOutput;
use crate::operators::aggregate::intermediate::test_support::count_state;

impl IpcRunOutput for &mut Cursor<Vec<u8>> {}

mod support;
use support::{managed_run, sample_group};
mod round_trip;
mod memory;
mod streaming;
mod partition;
#[path = "batch_tests.rs"]
mod batch_tests;
