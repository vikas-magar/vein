//! Arrow IPC runs used by the external hash join.
//!
//! The in-memory path retains reference-backed views. A spill run is the
//! explicit materialization boundary: it writes selected payload rows together
//! with the hash produced by the canonical join-key encoder. Repartitioning
//! uses that saved hash, so it does not decode keys just to route runs.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::ipc::reader::FileReader;
use common::Result;
use common::arrow::SelectionVector;
use error::VeinError;

use crate::memory::QueryMemoryTracker;
use crate::pipeline::DataChunkView;
use crate::pipeline::view::ViewMemoryLease;
use crate::spill::{IpcRunWriter, PendingSpillRun, SpillRun, SpillStream, SpillWriteHandle};

use super::hash_table::{HashTable, JoinKeys};

const HASH_FIELD: &str = "__vein_join_hash";

#[derive(Clone)]
pub(crate) struct JoinSpillSchema {
    payload: SchemaRef,
    run: SchemaRef,
}

impl JoinSpillSchema {
    pub(crate) fn new(types: &[DataType]) -> Self {
        let payload = Arc::new(Schema::new(
            types
                .iter()
                .enumerate()
                .map(|(index, data_type)| Field::new(format!("column_{index}"), data_type.clone(), true))
                .collect::<Vec<_>>(),
        ));
        let mut fields = payload.fields().iter().cloned().collect::<Vec<_>>();
        fields.push(Field::new(HASH_FIELD, DataType::UInt64, false).into());
        Self {
            payload,
            run: Arc::new(Schema::new(fields)),
        }
    }

    pub(crate) fn payload_schema(&self) -> &SchemaRef {
        &self.payload
    }

    fn run_schema(&self) -> &SchemaRef {
        &self.run
    }

    fn payload_columns(&self) -> usize {
        self.payload.fields().len()
    }
}

#[derive(Debug, Clone)]
pub(crate) struct JoinSpillRun {
    pub(crate) run: SpillRun,
    pub(crate) rows: u64,
    pub(crate) bytes: u64,
}

/// Incrementally writes one side of a join to a single IPC run.
pub(crate) struct JoinSpillWriter {
    pending: PendingSpillRun,
    writer: IpcRunWriter<SpillWriteHandle>,
    schema: JoinSpillSchema,
    tracker: QueryMemoryTracker,
    rows: u64,
}

impl JoinSpillWriter {
    pub(crate) fn new(tracker: QueryMemoryTracker, schema: JoinSpillSchema) -> Result<Self> {
        tracker.check_cancelled()?;
        let mut pending = tracker.spill_manager().create_run()?;
        let output = pending.writer()?;
        let writer = IpcRunWriter::new(output, schema.run_schema(), "create join IPC writer")?;
        Ok(Self {
            pending,
            writer,
            schema,
            tracker,
            rows: 0,
        })
    }

    pub(crate) fn push(&mut self, view: &DataChunkView, keys: &JoinKeys) -> Result<()> {
        self.tracker.check_cancelled()?;
        if view.is_empty() {
            return Ok(());
        }
        if view.num_columns() != self.schema.payload_columns() {
            return Err(VeinError::planning(
                format!(
                    "join spill input has {} columns, expected {}",
                    view.num_columns(),
                    self.schema.payload_columns()
                ),
                None,
            ));
        }

        let hashes = HashTable::hashes_for_view(view, keys)?;
        let compacted = view.compact_active_rows()?;
        let mut projection = (0..compacted.num_columns())
            .map(Some)
            .collect::<Vec<_>>();
        projection.push(None);
        let run_view = compacted.project_with_arrays(
            &projection,
            vec![Arc::new(UInt64Array::from(hashes)) as ArrayRef],
            SelectionVector::new_dense(compacted.num_rows()),
        )?;
        self.write_view(&run_view)
    }

    pub(crate) fn rows(&self) -> u64 {
        self.rows
    }

    fn write_view(&mut self, view: &DataChunkView) -> Result<()> {
        self.tracker.check_cancelled()?;
        self.writer.write_view(view, "write join IPC batch")?;
        self.rows = self.rows.saturating_add(view.num_rows() as u64);
        Ok(())
    }

    pub(crate) fn finish(self) -> Result<JoinSpillRun> {
        self.tracker.check_cancelled()?;
        let output = self.writer.finish("finish join IPC run")?;
        let run = self.pending.commit(output)?;
        Ok(JoinSpillRun {
            rows: self.rows,
            bytes: run.bytes(),
            run,
        })
    }
}

/// Repartitions runs using one saved hash bit. Input runs remain available so
/// callers can repartition both sides transactionally before consuming either.
pub(crate) fn partition_runs(
    runs: &[JoinSpillRun],
    schema: &JoinSpillSchema,
    tracker: &QueryMemoryTracker,
    bit: u32,
) -> Result<[Option<JoinSpillRun>; 2]> {
    if bit >= u64::BITS {
        return Err(VeinError::resource_exhausted(
            "join spill partition depth",
            u128::from(u64::BITS) + 1,
            u128::from(u64::BITS),
        ));
    }
    tracker.check_cancelled()?;
    let mut zero = JoinSpillWriter::new(tracker.clone(), schema.clone())?;
    let mut one = JoinSpillWriter::new(tracker.clone(), schema.clone())?;

    for run in runs {
        tracker.check_cancelled()?;
        let reader = open_reader(run)
            .map_err(|error| spill_error("read join IPC run", error))?;
        for batch in reader {
            tracker.check_cancelled()?;
            let batch = batch.map_err(|error| spill_error("decode join IPC batch", error))?;
            let view = crate::pipeline::arrow_adapter::from_record_batch(batch)?;
            let hash_array = view.materialize_column(schema.payload_columns())?;
            let hashes = hash_array
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or_else(|| VeinError::internal("join spill hash column is not UInt64"))?;
            let mut zero_rows = Vec::new();
            let mut one_rows = Vec::new();
            zero_rows.try_reserve(view.num_rows()).map_err(|error| {
                VeinError::internal(format!("cannot reserve join spill row indexes: {error}"))
            })?;
            one_rows.try_reserve(view.num_rows()).map_err(|error| {
                VeinError::internal(format!("cannot reserve join spill row indexes: {error}"))
            })?;
            for row in 0..view.num_rows() {
                if hashes.value(row) & (1_u64 << bit) == 0 {
                    zero_rows.push(row);
                } else {
                    one_rows.push(row);
                }
            }
            if !zero_rows.is_empty() {
                zero.write_view(&select_rows(&view, &zero_rows, schema)?)?;
            }
            if !one_rows.is_empty() {
                one.write_view(&select_rows(&view, &one_rows, schema)?)?;
            }
        }
    }

    let mut zero = Some(zero.finish()?);
    let mut one = Some(one.finish()?);
    if zero.as_ref().is_some_and(|run| run.rows == 0) {
        remove_run(zero.take().expect("checked zero partition"))?;
    }
    if one.as_ref().is_some_and(|run| run.rows == 0) {
        remove_run(one.take().expect("checked one partition"))?;
    }
    Ok([zero, one])
}

fn select_rows(batch: &DataChunkView, rows: &[usize], schema: &JoinSpillSchema) -> Result<DataChunkView> {
    batch
        .select_logical_rows(rows)?
        .with_schema(Arc::clone(schema.run_schema()))
}

pub(crate) fn remove_run(run: JoinSpillRun) -> Result<()> {
    drop(run);
    Ok(())
}

/// Removes the routing hash and returns a reference-backed payload view. The
/// decoded arrays are leased to the query tracker and remain charged when
/// composed output views keep them alive.
pub(crate) fn payload_view(
    batch: DataChunkView,
    schema: &JoinSpillSchema,
    tracker: &QueryMemoryTracker,
) -> Result<DataChunkView> {
    let mut view = batch
        .project(&(0..schema.payload_columns()).collect::<Vec<_>>())?
        .with_schema(Arc::clone(schema.payload_schema()))?;
    let leases = view
        .source_array_memory_sizes()?
        .into_iter()
        .map(|(identity, bytes)| {
            let lease: Arc<dyn ViewMemoryLease> =
                tracker.lease_retained_array(identity, bytes, "join spill arrays")?;
            Ok((identity, lease))
        })
        .collect::<Result<HashMap<_, _>>>()?;
    view.attach_memory_leases(&leases);
    Ok(view)
}

pub(crate) fn open_reader(run: &JoinSpillRun) -> Result<FileReader<Box<dyn SpillStream>>> {
    let stream = run.run.open()?;
    FileReader::try_new(stream, None).map_err(|error| spill_error("read join IPC run", error))
}

fn spill_error(action: &str, error: impl std::fmt::Display) -> VeinError {
    VeinError::internal(format!("{action}: {error}"))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Array, ArrayRef, Int64Array};
    use arrow::datatypes::DataType;

    use super::*;
    use crate::pipeline::DataChunkView;

    fn view(values: &[i64]) -> DataChunkView {
        crate::pipeline::test_support::dense_view(vec![
            Arc::new(Int64Array::from(values.to_vec())) as ArrayRef,
        ])
    }

    fn hashes(run: &JoinSpillRun, schema: &JoinSpillSchema) -> Vec<u64> {
        let reader = open_reader(run).expect("open run");
        let mut values = Vec::new();
        for batch in reader {
            let batch = batch.expect("read batch");
            let hashes = batch
                .column(schema.payload_columns())
                .as_any()
                .downcast_ref::<UInt64Array>()
                .expect("hash column");
            values.extend((0..hashes.len()).map(|row| hashes.value(row)));
        }
        values
    }

    #[test]
    fn ipc_run_round_trips_payload_and_canonical_hashes() {
        let tracker = QueryMemoryTracker::unlimited(common::cancellation::CancellationToken::new());
        let schema = JoinSpillSchema::new(&[DataType::Int64]);
        let input = view(&[4, 9, 4]);
        let keys = JoinKeys::new(vec![0]);
        let expected_hashes = HashTable::hashes_for_view(&input, &keys).expect("hash rows");
        let mut writer = JoinSpillWriter::new(tracker.clone(), schema.clone()).expect("writer");
        writer.push(&input, &keys).expect("write input");
        let run = writer.finish().expect("finish run");

        let mut reader = open_reader(&run).expect("open finished run");
        let batch = reader.next().expect("one batch").expect("valid batch");
        assert_eq!(batch.num_rows(), 3);
        assert_eq!(
            batch.column(0).as_any().downcast_ref::<Int64Array>().unwrap().values(),
            &[4, 9, 4]
        );
        assert_eq!(hashes(&run, &schema), expected_hashes);
    }

    #[test]
    fn recursive_partitioning_keeps_equal_hashes_together_and_preserves_sources_until_commit() {
        let tracker = QueryMemoryTracker::unlimited(common::cancellation::CancellationToken::new());
        let schema = JoinSpillSchema::new(&[DataType::Int64]);
        let input = view(&[1, 2, 3, 4, 5, 6, 7, 8]);
        let mut writer = JoinSpillWriter::new(tracker.clone(), schema.clone()).expect("writer");
        writer
            .push(&input, &JoinKeys::new(vec![0]))
            .expect("write input");
        let source = writer.finish().expect("finish input");

        let children = partition_runs(std::slice::from_ref(&source), &schema, &tracker, 0)
            .expect("partition input by first bit");

        assert!(tracker.spill_manager().exists(&source.run), "source remains available until both sides commit");
        for (partition, child) in children.into_iter().enumerate() {
            if let Some(child) = child {
                for hash in hashes(&child, &schema) {
                    assert_eq!(hash & 1, partition as u64);
                }
            }
        }
        remove_run(source).expect("consume source after partition output is validated");
    }

    #[test]
    fn repartition_removes_empty_child_files() {
        let tracker = QueryMemoryTracker::unlimited(common::cancellation::CancellationToken::new());
        let schema = JoinSpillSchema::new(&[DataType::Int64]);
        let input = view(&[1, 3, 5]);
        let mut writer = JoinSpillWriter::new(tracker.clone(), schema.clone()).expect("writer");
        writer
            .push(&input, &JoinKeys::new(vec![0]))
            .expect("write input");
        let source = writer.finish().expect("finish input");
        let all_hashes = hashes(&source, &schema);
        let bit = (0..u64::BITS)
            .find(|bit| all_hashes.iter().all(|hash| *hash & (1_u64 << bit) == 0))
            .expect("one unused bit");
        let children = partition_runs(std::slice::from_ref(&source), &schema, &tracker, bit)
            .expect("partition into one nonempty child");

        assert!(children[0].is_some());
        assert!(children[1].is_none());
    }

    #[test]
    fn canceled_repartition_preserves_source_for_query_cleanup() {
        let cancel = common::cancellation::CancellationToken::new();
        let tracker = QueryMemoryTracker::unlimited(cancel.clone());
        let schema = JoinSpillSchema::new(&[DataType::Int64]);
        let input = view(&[3]);
        let mut writer = JoinSpillWriter::new(tracker.clone(), schema.clone()).expect("writer");
        writer
            .push(&input, &JoinKeys::new(vec![0]))
            .expect("write input");
        let source = writer.finish().expect("finish input");

        cancel.cancel();
        assert!(matches!(
            partition_runs(std::slice::from_ref(&source), &schema, &tracker, 0),
            Err(VeinError::Cancelled)
        ));
        assert!(tracker.spill_manager().exists(&source.run));
    }
}
