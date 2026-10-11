//! Versioned Arrow IPC runs for grouped aggregate state.
//!
//! In-memory group keys remain shared Arrow array/row references. This module
//! gathers those keys only when writing a run, then reads run batches back as
//! shared array references so the merge path can retain them without scalar
//! copies.

use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{Read, Seek, Write};
use std::path::PathBuf;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, ArrowPrimitiveType, BinaryArray, BinaryBuilder, DictionaryArray, Int32Array,
    PrimitiveArray, PrimitiveBuilder, StringArray, StringBuilder, UInt64Array,
};
use arrow::datatypes::{DataType, Field, Int32Type, Schema, SchemaRef};
use arrow::ipc::reader::FileReader;
use arrow::ipc::writer::FileWriter;
use arrow::record_batch::RecordBatch;
use common::Result;
use error::VeinError;
use types::VeinDataType;

use crate::memory::QueryMemoryTracker;
use super::intermediate::AggregateIntermediateGroup;

const FORMAT_METADATA_KEY: &str = "vein.aggregate.spill.format";
const FORMAT_METADATA_VALUE: &str = "2";
const GROUPING_SET_ID_FIELD: &str = "grouping_set_id";
const HASH_FIELD: &str = "group_hash";
const STATE_FIELD: &str = "aggregate_state";
/// Bound typed Arrow builders and IPC record-batch workspace per spill write.
const SPILL_RECORD_BATCH_SIZE: usize = super::AGGREGATE_BATCH_ROWS.get();

/// Describes the stable typed layout shared by all runs of one aggregate.
#[derive(Clone)]
pub(super) struct AggregateSpillSchema {
    key_types: Vec<VeinDataType>,
    grouping_sets: Vec<Vec<usize>>,
    schema: SchemaRef,
}

impl AggregateSpillSchema {
    pub(super) fn try_new(
        key_types: Vec<VeinDataType>,
        grouping_sets: Vec<Vec<usize>>,
    ) -> Result<Self> {
        for grouping_set in &grouping_sets {
            if grouping_set.iter().any(|&index| index >= key_types.len()) {
                return Err(VeinError::internal(
                    "aggregate spill grouping set references an unknown key",
                ));
            }
        }

        let mut fields = Vec::with_capacity(key_types.len().saturating_add(3));
        fields.push(Field::new(GROUPING_SET_ID_FIELD, DataType::UInt64, false));
        fields.push(Field::new(HASH_FIELD, DataType::UInt64, false));
        fields.extend(key_types.iter().enumerate().map(|(index, data_type)| {
            Field::new(format!("key_{index}"), data_type.to_arrow(), true)
        }));
        fields.push(Field::new(STATE_FIELD, DataType::Binary, false));

        let mut metadata = HashMap::new();
        metadata.insert(FORMAT_METADATA_KEY.to_owned(), FORMAT_METADATA_VALUE.to_owned());
        let schema = Arc::new(Schema::new_with_metadata(fields, metadata));
        Ok(Self { key_types, grouping_sets, schema })
    }

    /// Writes groups incrementally as a versioned Arrow IPC run.
    ///
    /// The iterator should snapshot groups on demand so accumulator snapshots,
    /// Arrow builders, and record batches stay bounded by the run batch size.
    #[cfg(test)]
    pub(super) fn write_run<W, I>(&self, output: W, groups: I) -> Result<()>
    where
        W: Write + Seek,
        I: IntoIterator<Item=AggregateIntermediateGroup>,
    {
        let mut writer = AggregateSpillWriter::new(self, output)?;
        for group in groups {
            writer.push(group)?;
        }
        writer.finish()
    }

    /// Streams snapshots from an indexed sink without retaining a second group
    /// list in memory.
    pub(super) fn write_run_with<W>(
        &self,
        output: W,
        group_count: usize,
        mut group_at: impl FnMut(usize) -> Result<AggregateIntermediateGroup>,
    ) -> Result<()>
    where
        W: Write + Seek,
    {
        let mut writer = AggregateSpillWriter::new(self, output)?;
        for slot in 0..group_count {
            writer.push(group_at(slot)?)?;
        }
        writer.finish()
    }

    /// Repartitions existing runs by one hash bit using two bounded IPC writers.
    ///
    /// Each source file is opened and consumed before the next is opened. The
    /// input files remain intact until both output runs have finished, so a
    /// failed partition can be discarded without losing aggregate state.
    pub(super) fn partition_runs(
        &self,
        runs: &[PathBuf],
        tracker: &QueryMemoryTracker,
        bit: u32,
    ) -> Result<[Option<PathBuf>; 2]> {
        if bit >= u64::BITS {
            return Err(VeinError::internal("aggregate spill partition bit is out of range"));
        }
        tracker.check_cancelled()?;
        let (zero_path, zero_file) = tracker.create_spill_file()?;
        let (one_path, one_file) = tracker.create_spill_file()?;
        let mut zero = AggregateSpillWriter::new(self, zero_file)?;
        let mut one = AggregateSpillWriter::new(self, one_file)?;
        let mut has_zero = false;
        let mut has_one = false;

        for path in runs {
            tracker.check_cancelled()?;
            let file = File::open(path)
                .map_err(|error| spill_error("open aggregate spill run for partition", error))?;
            let mut reader = self.open_reader(file)?;
            while let Some(batch) = reader.next_batch()? {
                tracker.check_cancelled()?;
                for group in batch {
                    if group.hash & (1_u64 << bit) == 0 {
                        zero.push(group)?;
                        has_zero = true;
                    } else {
                        one.push(group)?;
                        has_one = true;
                    }
                }
            }
        }

        zero.finish()?;
        one.finish()?;

        if !has_zero {
            fs::remove_file(&zero_path)
                .map_err(|error| spill_error("remove empty aggregate spill partition", error))?;
        }
        if !has_one {
            fs::remove_file(&one_path)
                .map_err(|error| spill_error("remove empty aggregate spill partition", error))?;
        }

        for path in runs {
            fs::remove_file(path)
                .map_err(|error| spill_error("remove repartitioned aggregate run", error))?;
        }
        Ok([
            has_zero.then_some(zero_path),
            has_one.then_some(one_path),
        ])
    }

    /// Opens a run after validating its complete schema and format version.
    pub(super) fn open_reader<R: Read + Seek>(&self, input: R) -> Result<AggregateSpillReader<R>> {
        let reader = FileReader::try_new(input, None)
            .map_err(|error| spill_error("open aggregate IPC run", error))?;
        if reader.schema().as_ref() != self.schema.as_ref() {
            return Err(VeinError::internal(
                "aggregate IPC run schema does not match the active aggregate plan",
            ));
        }
        Ok(AggregateSpillReader { reader, layout: self.clone() })
    }

    fn encode_batch(&self, groups: &[AggregateIntermediateGroup]) -> Result<RecordBatch> {
        let mut grouping_set_ids = Vec::with_capacity(groups.len());
        let mut hashes = Vec::with_capacity(groups.len());
        let mut key_builders = self
            .key_types
            .iter()
            .map(|data_type| key_builder(data_type, groups.len()))
            .collect::<Result<Vec<_>>>()?;
        let mut states = BinaryBuilder::new();
        let mut state_bytes = 0_usize;

        for group in groups {
            let grouping_set = self.grouping_sets.get(group.grouping_set_id).ok_or_else(|| {
                VeinError::internal("aggregate spill group has an invalid grouping-set ID")
            })?;
            if group.keys.len() != self.key_types.len() {
                return Err(VeinError::internal(
                    "aggregate spill group key count does not match its schema",
                ));
            }
            grouping_set_ids.push(u64::try_from(group.grouping_set_id).map_err(|_| {
                VeinError::internal("aggregate spill grouping-set ID exceeds UInt64")
            })?);
            hashes.push(group.hash);
            for (key_index, (builder, group_key)) in
                key_builders.iter_mut().zip(&group.keys).enumerate()
            {
                let included = grouping_set.contains(&key_index);
                match (included, group_key) {
                    (true, Some((array, row))) if *row >= array.len() => {
                        return Err(VeinError::internal(
                            "aggregate spill key row is outside its Arrow array",
                        ));
                    }
                    (true, Some(key)) => builder.append(Some(key))?,
                    (true, None) => {
                        return Err(VeinError::internal(
                            "included aggregate spill key is missing from its group",
                        ));
                    }
                    (false, None) => builder.append(None)?,
                    (false, Some(_)) => {
                        return Err(VeinError::internal(
                            "excluded aggregate spill key is present in its group",
                        ));
                    }
                }
            }
            let payload = group.encode_aggregate_states()?;
            if payload.len() > i32::MAX as usize {
                return Err(VeinError::internal(
                    "aggregate spill state exceeds Arrow Binary offset capacity",
                ));
            }
            state_bytes = state_bytes
                .checked_add(payload.len())
                .filter(|&size| size <= i32::MAX as usize)
                .ok_or_else(|| {
                    VeinError::internal("aggregate spill batch exceeds Arrow Binary offset capacity")
                })?;
            states.append_value(payload);
        }

        let mut columns: Vec<ArrayRef> = Vec::with_capacity(self.key_types.len() + 3);
        columns.push(Arc::new(UInt64Array::from(grouping_set_ids)));
        columns.push(Arc::new(UInt64Array::from(hashes)));
        for builder in key_builders {
            columns.push(builder.finish());
        }
        columns.push(Arc::new(states.finish()) as ArrayRef);
        RecordBatch::try_new(Arc::clone(&self.schema), columns)
            .map_err(|error| spill_error("build aggregate spill batch", error))
    }

    fn decode_batch(&self, batch: RecordBatch) -> Result<Vec<AggregateIntermediateGroup>> {
        if batch.schema().as_ref() != self.schema.as_ref() {
            return Err(VeinError::internal(
                "aggregate IPC batch schema changed within a run",
            ));
        }
        let grouping_set_ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| VeinError::internal("invalid aggregate spill grouping-set column"))?;
        let hashes = batch
            .column(1)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| VeinError::internal("invalid aggregate spill hash column"))?;
        let state_payloads = batch
            .column(self.key_types.len() + 2)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| VeinError::internal("invalid aggregate spill state column"))?;

        let mut groups = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            if grouping_set_ids.is_null(row) || hashes.is_null(row) || state_payloads.is_null(row) {
                return Err(VeinError::internal(
                    "aggregate spill metadata columns cannot contain NULL",
                ));
            }
            let grouping_set_id = usize::try_from(grouping_set_ids.value(row))
                .map_err(|_| VeinError::internal("aggregate spill grouping-set ID overflows usize"))?;
            let grouping_set = self.grouping_sets.get(grouping_set_id).ok_or_else(|| {
                VeinError::internal("aggregate spill record has an invalid grouping-set ID")
            })?;
            let keys = (0..self.key_types.len())
                .map(|key_index| {
                    if grouping_set.contains(&key_index) {
                        Some((Arc::clone(batch.column(key_index + 2)), row))
                    } else {
                        None
                    }
                })
                .collect();
            let aggregate_states =
                AggregateIntermediateGroup::decode_aggregate_states(state_payloads.value(row))?;
            groups.push(AggregateIntermediateGroup {
                hash: hashes.value(row),
                grouping_set_id,
                keys,
                aggregate_states,
            });
        }
        Ok(groups)
    }
}

/// Bounded, incremental IPC writer shared by flush and repartition paths.
struct AggregateSpillWriter<'a, W: Write + Seek> {
    layout: &'a AggregateSpillSchema,
    writer: Option<FileWriter<W>>,
    pending: Vec<AggregateIntermediateGroup>,
}

impl<'a, W: Write + Seek> AggregateSpillWriter<'a, W> {
    fn new(layout: &'a AggregateSpillSchema, output: W) -> Result<Self> {
        let writer = FileWriter::try_new(output, &layout.schema)
            .map_err(|error| spill_error("create aggregate IPC writer", error))?;
        Ok(Self {
            layout,
            writer: Some(writer),
            pending: Vec::with_capacity(SPILL_RECORD_BATCH_SIZE),
        })
    }

    fn push(&mut self, group: AggregateIntermediateGroup) -> Result<()> {
        self.pending.push(group);
        if self.pending.len() == SPILL_RECORD_BATCH_SIZE {
            self.flush()?;
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        if self.pending.is_empty() {
            return Ok(());
        }
        let encoded = self.layout.encode_batch(&self.pending)?;
        self.writer
            .as_mut()
            .expect("writer remains live until finish")
            .write(&encoded)
            .map_err(|error| spill_error("write aggregate IPC batch", error))?;
        self.pending.clear();
        Ok(())
    }

    fn finish(mut self) -> Result<()> {
        self.flush()?;
        self.writer
            .take()
            .expect("writer remains live until finish")
            .finish()
            .map_err(|error| spill_error("finish aggregate IPC run", error))
    }
}

/// Incremental reader: each call decodes one IPC record batch, so callers can
/// merge and release a batch before loading the next one.
pub(super) struct AggregateSpillReader<R> {
    reader: FileReader<R>,
    layout: AggregateSpillSchema,
}

impl<R: Read + Seek> AggregateSpillReader<R> {
    pub(super) fn next_batch(&mut self) -> Result<Option<Vec<AggregateIntermediateGroup>>> {
        match self.reader.next() {
            Some(Ok(batch)) => self.layout.decode_batch(batch).map(Some),
            Some(Err(error)) => Err(spill_error("read aggregate IPC batch", error)),
            None => Ok(None),
        }
    }
}

trait SpillKeyBuilder {
    fn append(&mut self, key: Option<&(ArrayRef, usize)>) -> Result<()>;
    fn finish(self: Box<Self>) -> ArrayRef;
}

struct PrimitiveKeyColumnBuilder<T: ArrowPrimitiveType>(PrimitiveBuilder<T>);

impl<T: ArrowPrimitiveType> SpillKeyBuilder for PrimitiveKeyColumnBuilder<T> {
    fn append(&mut self, key: Option<&(ArrayRef, usize)>) -> Result<()> {
        let Some((array, row)) = key else {
            self.0.append_null();
            return Ok(());
        };
        let values = array
            .as_any()
            .downcast_ref::<PrimitiveArray<T>>()
            .ok_or_else(|| key_type_error(array, &T::DATA_TYPE))?;
        if values.is_null(*row) {
            self.0.append_null();
        } else {
            self.0.append_value(values.value(*row));
        }
        Ok(())
    }

    fn finish(mut self: Box<Self>) -> ArrayRef {
        Arc::new(self.0.finish())
    }
}

struct Utf8KeyColumnBuilder {
    values: StringBuilder,
    payload_bytes: usize,
}

impl Utf8KeyColumnBuilder {
    fn append_value(&mut self, value: &str) -> Result<()> {
        self.payload_bytes = self
            .payload_bytes
            .checked_add(value.len())
            .filter(|&size| size <= i32::MAX as usize)
            .ok_or_else(|| {
                VeinError::internal("aggregate spill key exceeds Arrow UTF-8 offset capacity")
            })?;
        self.values.append_value(value);
        Ok(())
    }
}

impl SpillKeyBuilder for Utf8KeyColumnBuilder {
    fn append(&mut self, key: Option<&(ArrayRef, usize)>) -> Result<()> {
        let Some((array, row)) = key else {
            self.values.append_null();
            return Ok(());
        };
        if let Some(values) = array.as_any().downcast_ref::<StringArray>() {
            if values.is_null(*row) {
                self.values.append_null();
            } else {
                self.append_value(values.value(*row))?;
            }
            return Ok(());
        }
        if let Some(dictionary) = array.as_any().downcast_ref::<DictionaryArray<Int32Type>>() {
            let keys = dictionary
                .keys()
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| key_type_error(array, &DataType::Utf8))?;
            if keys.is_null(*row) {
                self.values.append_null();
                return Ok(());
            }
            let value_index = usize::try_from(keys.value(*row))
                .map_err(|_| VeinError::internal("aggregate spill dictionary key is negative"))?;
            let values = dictionary
                .values()
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| key_type_error(array, &DataType::Utf8))?;
            if value_index >= values.len() {
                return Err(VeinError::internal(
                    "aggregate spill dictionary key is outside its values array",
                ));
            }
            if values.is_null(value_index) {
                self.values.append_null();
            } else {
                self.append_value(values.value(value_index))?;
            }
            return Ok(());
        }
        Err(key_type_error(array, &DataType::Utf8))
    }

    fn finish(mut self: Box<Self>) -> ArrayRef {
        Arc::new(self.values.finish())
    }
}

fn key_builder(data_type: &VeinDataType, capacity: usize) -> Result<Box<dyn SpillKeyBuilder>> {
    macro_rules! primitive_builder {
        ($native:ty) => {
            Box::new(PrimitiveKeyColumnBuilder::<$native>(
                PrimitiveBuilder::with_capacity(capacity),
            ))
                as Box<dyn SpillKeyBuilder>
        };
    }

    Ok(match data_type {
        VeinDataType::Int64 => primitive_builder!(arrow::datatypes::Int64Type),
        VeinDataType::UInt8 => primitive_builder!(arrow::datatypes::UInt8Type),
        VeinDataType::UInt16 => primitive_builder!(arrow::datatypes::UInt16Type),
        VeinDataType::UInt32 => primitive_builder!(arrow::datatypes::UInt32Type),
        VeinDataType::UInt64 => primitive_builder!(arrow::datatypes::UInt64Type),
        VeinDataType::Utf8 => Box::new(Utf8KeyColumnBuilder {
            values: StringBuilder::with_capacity(capacity, 0),
            payload_bytes: 0,
        }),
        unsupported => {
            return Err(VeinError::internal(format!(
                "aggregate spill does not support grouping key type {unsupported:?}"
            )));
        }
    })
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

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use arrow::array::{Int64Array, StringArray};

    use super::*;
    use crate::operators::aggregate::intermediate::AggregateIntermediateState;

    fn sample_group(
        grouping_set_id: usize,
        key: Option<(ArrayRef, usize)>,
        count: i64,
    ) -> AggregateIntermediateGroup {
        AggregateIntermediateGroup {
            hash: u64::try_from(count).expect("test count is non-negative"),
            grouping_set_id,
            keys: vec![key],
            aggregate_states: vec![AggregateIntermediateState::Count(count)],
        }
    }

    #[test]
    fn aggregate_ipc_run_round_trips_typed_keys_nulls_grouping_sets_and_states() {
        let layout = AggregateSpillSchema::try_new(
            vec![VeinDataType::Int64],
            vec![vec![0], vec![]],
        )
            .expect("create typed aggregate spill schema");
        let key: ArrayRef = Arc::new(Int64Array::from(vec![None, Some(17)]));
        let groups = vec![
            sample_group(0, Some((Arc::clone(&key), 0)), 2),
            sample_group(0, Some((Arc::clone(&key), 1)), 5),
            sample_group(1, None, 7),
        ];
        let mut bytes = Cursor::new(Vec::new());
        layout
            .write_run(&mut bytes, groups.iter().cloned())
            .expect("write IPC run");

        let mut reader = layout
            .open_reader(Cursor::new(bytes.into_inner()))
            .expect("validate and open IPC run");
        let restored = reader.next_batch().expect("read IPC batch").expect("run batch");
        assert_eq!(restored.len(), 3);
        assert_eq!(restored[0].hash, 2);
        assert_eq!(restored[1].hash, 5);
        assert_eq!(restored[2].hash, 7);
        assert!(restored[0].keys[0]
            .as_ref()
            .expect("real NULL grouping key")
            .0
            .is_null(0));
        let (array, row) = restored[1].keys[0].as_ref().expect("ordinary key");
        assert_eq!(array.as_any().downcast_ref::<Int64Array>().unwrap().value(*row), 17);
        assert_eq!(restored[0].grouping_set_id, 0);
        assert_eq!(restored[2].grouping_set_id, 1);
        assert!(restored[2].keys[0].is_none(), "omitted grouping key stays absent");
        assert_eq!(restored[2].aggregate_states.len(), 1);
        assert!(reader.next_batch().expect("end of run").is_none());
    }

    #[test]
    fn aggregate_ipc_run_normalizes_dictionary_strings_to_plan_type() {
        use arrow::array::{DictionaryArray, Int32Array};
        use arrow::datatypes::Int32Type;

        let layout = AggregateSpillSchema::try_new(
            vec![VeinDataType::Utf8],
            vec![vec![0]],
        )
            .expect("create UTF-8 spill schema");
        let dict = DictionaryArray::<Int32Type>::try_new(
            Int32Array::from(vec![Some(0), Some(1)]),
            Arc::new(StringArray::from(vec!["alpha", "beta"])),
        )
            .expect("dictionary source key");
        let key: ArrayRef = Arc::new(dict);
        let groups = vec![sample_group(0, Some((key, 1)), 1)];
        let mut bytes = Cursor::new(Vec::new());
        layout
            .write_run(&mut bytes, groups.iter().cloned())
            .expect("write dictionary key");
        let mut reader = layout.open_reader(Cursor::new(bytes.into_inner())).unwrap();
        let restored = reader.next_batch().unwrap().unwrap();
        let (array, row) = restored[0].keys[0].as_ref().unwrap();
        assert_eq!(array.data_type(), &DataType::Utf8);
        assert_eq!(array.as_any().downcast_ref::<StringArray>().unwrap().value(*row), "beta");
    }

    #[test]
    fn aggregate_ipc_run_rejects_schema_mismatch_and_bad_group_layout() {
        let layout = AggregateSpillSchema::try_new(vec![VeinDataType::Int64], vec![vec![0]])
            .expect("create schema");
        let wrong = AggregateSpillSchema::try_new(vec![VeinDataType::Utf8], vec![vec![0]])
            .expect("create different schema");
        let mut bytes = Cursor::new(Vec::new());
        wrong
            .write_run(&mut bytes, std::iter::empty())
            .expect("write another layout");
        assert!(layout.open_reader(Cursor::new(bytes.into_inner())).is_err());

        let invalid_group = AggregateIntermediateGroup {
            hash: 1,
            grouping_set_id: 0,
            keys: vec![],
            aggregate_states: vec![AggregateIntermediateState::Count(1)],
        };
        assert!(layout.encode_batch(&[invalid_group]).is_err());

        let mismatched_key = AggregateIntermediateGroup {
            hash: 1,
            grouping_set_id: 0,
            keys: vec![Some((Arc::new(StringArray::from(vec!["wrong type"])) as ArrayRef, 0))],
            aggregate_states: vec![AggregateIntermediateState::Count(1)],
        };
        assert!(layout.encode_batch(&[mismatched_key]).is_err());

        let out_of_range_set = AggregateIntermediateGroup {
            hash: 1,
            grouping_set_id: 1,
            keys: vec![None],
            aggregate_states: vec![AggregateIntermediateState::Count(1)],
        };
        assert!(layout.encode_batch(&[out_of_range_set]).is_err());
    }

    #[test]
    fn aggregate_ipc_run_streams_groups_across_bounded_record_batches() {
        let layout = AggregateSpillSchema::try_new(vec![VeinDataType::Int64], vec![vec![0]])
            .expect("create spill schema");
        let key: ArrayRef = Arc::new(Int64Array::from_iter_values(
            0..(SPILL_RECORD_BATCH_SIZE as i64 + 1),
        ));
        let groups = (0..SPILL_RECORD_BATCH_SIZE + 1).map(|row| {
            sample_group(
                0,
                Some((Arc::clone(&key), row)),
                i64::try_from(row).expect("test group count fits i64"),
            )
        });
        let mut bytes = Cursor::new(Vec::new());
        layout.write_run(&mut bytes, groups).expect("write streamed run");

        let mut reader = layout
            .open_reader(Cursor::new(bytes.into_inner()))
            .expect("open streamed run");
        let first = reader.next_batch().expect("read first batch").expect("first batch");
        let second = reader.next_batch().expect("read second batch").expect("second batch");
        assert_eq!(first.len(), SPILL_RECORD_BATCH_SIZE);
        assert_eq!(second.len(), 1);
        assert!(matches!(
            second[0].aggregate_states.as_slice(),
            [AggregateIntermediateState::Count(1024)]
        ));
        assert!(reader.next_batch().expect("end of run").is_none());
    }

    #[test]
    fn aggregate_spill_partitions_runs_by_saved_hash_and_removes_inputs_after_success() {
        let layout = AggregateSpillSchema::try_new(vec![VeinDataType::Int64], vec![vec![0]])
            .expect("create spill schema");
        let tracker = QueryMemoryTracker::unlimited(common::cancellation::CancellationToken::new());
        let key: ArrayRef = Arc::new(Int64Array::from_iter_values(0..8));
        let groups = (0..8).map(|row| {
            sample_group(
                0,
                Some((Arc::clone(&key), row)),
                i64::try_from(row).expect("hash fits i64"),
            )
        });
        let (input_path, file) = tracker.create_spill_file().expect("create input run");
        layout.write_run(file, groups).expect("write input run");

        let children = layout
            .partition_runs(std::slice::from_ref(&input_path), &tracker, 0)
            .expect("partition by low hash bit");

        assert!(!input_path.exists(), "successful partition consumes its input runs");
        let hashes = children
            .into_iter()
            .enumerate()
            .map(|(partition, path)| {
                let Some(path) = path else {
                    return Vec::new();
                };
                let mut reader = layout
                    .open_reader(File::open(path).expect("open child run"))
                    .expect("validate child run");
                let mut hashes = Vec::new();
                while let Some(batch) = reader.next_batch().expect("read child run") {
                    hashes.extend(batch.into_iter().map(|group| {
                        assert_eq!(group.hash & 1, partition as u64);
                        group.hash
                    }));
                }
                hashes
            })
            .flatten()
            .collect::<Vec<_>>();
        let mut sorted_hashes = hashes;
        sorted_hashes.sort_unstable();
        assert_eq!(sorted_hashes, (0..8).collect::<Vec<_>>());
    }

    #[test]
    fn aggregate_spill_partition_removes_unused_empty_child_file() {
        let layout = AggregateSpillSchema::try_new(vec![VeinDataType::Int64], vec![vec![0]])
            .expect("create spill schema");
        let tracker = QueryMemoryTracker::unlimited(common::cancellation::CancellationToken::new());
        let key: ArrayRef = Arc::new(Int64Array::from_iter_values(0..4));
        let groups = (0..4).map(|row| {
            sample_group(
                0,
                Some((Arc::clone(&key), row)),
                i64::try_from(row * 2).expect("even test hash fits i64"),
            )
        });
        let (input_path, file) = tracker.create_spill_file().expect("create input run");
        layout.write_run(file, groups).expect("write input run");
        let directory = input_path.parent().expect("spill directory").to_owned();

        let children = layout
            .partition_runs(std::slice::from_ref(&input_path), &tracker, 0)
            .expect("partition groups with an unused child");

        assert!(children[0].is_some());
        assert!(children[1].is_none());
        let remaining_files = fs::read_dir(directory)
            .expect("read spill directory")
            .count();
        assert_eq!(remaining_files, 1, "only the referenced child run remains");
    }

    #[test]
    fn canceled_aggregate_partition_preserves_source_run_for_query_cleanup() {
        let layout = AggregateSpillSchema::try_new(vec![VeinDataType::Int64], vec![vec![0]])
            .expect("create spill schema");
        let cancel = common::cancellation::CancellationToken::new();
        let tracker = QueryMemoryTracker::unlimited(cancel.clone());
        let key: ArrayRef = Arc::new(Int64Array::from(vec![7]));
        let (input_path, file) = tracker.create_spill_file().expect("create input run");
        layout
            .write_run(file, [sample_group(0, Some((key, 0)), 7)])
            .expect("write input run");

        cancel.cancel();
        assert!(matches!(
            layout.partition_runs(std::slice::from_ref(&input_path), &tracker, 0),
            Err(VeinError::Cancelled)
        ));
        assert!(input_path.exists(), "failed partition leaves source data intact");

        let directory = input_path.parent().expect("spill directory").to_owned();
        drop(tracker);
        assert!(!directory.exists(), "query cleanup removes runs after cancellation");
    }
}
