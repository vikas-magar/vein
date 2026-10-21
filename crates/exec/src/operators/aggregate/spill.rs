//! Versioned Arrow IPC runs for grouped aggregate state.
//!
//! In-memory group keys remain shared Arrow array/row references. This module
//! gathers those keys only when writing a run, then reads run batches back as
//! shared array references so the merge path can retain them without scalar
//! copies.

use std::collections::HashMap;
use std::io::{Read, Seek};
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, ArrowPrimitiveType, BinaryArray, BinaryBuilder, DictionaryArray, Int32Array,
    PrimitiveArray, PrimitiveBuilder, StringArray, StringBuilder, UInt64Array,
};
use arrow::datatypes::{DataType, Field, Int32Type, Schema, SchemaRef};
use arrow::ipc::reader::FileReader;
use common::Result;
use common::arrow::SelectionVector;
use error::VeinError;
use types::VeinDataType;

use super::intermediate::{AggregateGroupKeyRef, AggregateIntermediateGroup};
use crate::memory::{MemoryReservation, QueryMemoryTracker};
use crate::pipeline::DataChunkView;
use crate::spill::SpillRun;

const FORMAT_METADATA_KEY: &str = "vein.aggregate.spill.format";
const FORMAT_METADATA_VALUE: &str = "2";
const GROUPING_SET_ID_FIELD: &str = "grouping_set_id";
const HASH_FIELD: &str = "group_hash";
const STATE_FIELD: &str = "aggregate_state";

/// Spill batches stay smaller than normal aggregate batches so decoded inputs stay bounded.
const SPILL_RECORD_BATCH_SIZE: usize = super::AGGREGATE_BATCH_ROWS.get() / 16;
const SPILL_WORKSPACE_BUDGET_DIVISOR: usize = 16;

mod batch_memory;
mod decimal_key_builder;
mod encode;
mod keys;
mod partition;
mod reader;
mod schema;
mod writer;
use writer::AggregateSpillWriter;

#[cfg(test)]
mod tests;

/// Describes the stable typed layout shared by all runs of one aggregate.
#[derive(Clone)]
pub(super) struct AggregateSpillSchema {
    key_types: Vec<VeinDataType>,
    grouping_sets: Vec<Vec<usize>>,
    schema: SchemaRef,
}

/// Incremental reader for a run with validated schema and format metadata.
pub(super) struct AggregateSpillReader<R> {
    reader: FileReader<R>,
    layout: AggregateSpillSchema,
}

/// Holds decoded key and state arrays with their memory leases for one batch.
pub(super) struct AggregateSpillBatch {
    pub(super) groups: Vec<AggregateIntermediateGroup>,
    _leases: Vec<Arc<MemoryReservation>>,
}

pub(super) fn resolve_source_key(
    key: Option<&AggregateGroupKeyRef>,
) -> Result<Option<(ArrayRef, usize)>> {
    key.map(AggregateGroupKeyRef::source_row)
        .transpose()
        .map(Option::flatten)
}

fn key_type_error(array: &ArrayRef, expected: &DataType) -> VeinError {
    VeinError::internal(format!(
        "aggregate spill key type {:?} does not match planned type {expected:?}",
        array.data_type()
    ))
}

fn spill_error(context: &str, error: impl std::fmt::Display) -> VeinError {
    VeinError::internal(format!("{context}: {error}"))
}
