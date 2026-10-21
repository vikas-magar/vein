//! Bounded-batch regressions for aggregate spill encoding.

use std::io::Cursor;
use std::sync::Arc;

use arrow::array::{ArrayRef, Decimal256Array};
use arrow::datatypes::i256;
use common::cancellation::CancellationToken;
use common::Result;

use crate::memory::QueryMemoryTracker;

use super::super::super::intermediate::{AggregateGroupKeyRef, AggregateIntermediateGroup};
use super::super::AggregateSpillSchema;
use super::super::super::intermediate::test_support::count_state;
use types::VeinDataType;

#[test]
fn spill_writer_splits_batches_by_decoded_workspace_budget() -> Result<()> {
    let group_count = 64;
    let layout = AggregateSpillSchema::try_new(
        vec![VeinDataType::Decimal256(76, 0)],
        vec![vec![0]],
    )?;
    let tracker = QueryMemoryTracker::new(1024, CancellationToken::new());
    let target_bytes = layout.spill_workspace_target(&tracker);
    let keys: ArrayRef = Arc::new(
        Decimal256Array::from_iter_values((0..group_count).map(|value| i256::from(value as i64)))
            .with_precision_and_scale(76, 0)
            .expect("valid Decimal256 test keys"),
    );

    let output = layout.write_run_with(
        Cursor::new(Vec::new()),
        &tracker,
        group_count,
        |row| {
            Ok(AggregateIntermediateGroup {
                hash: row as u64,
                grouping_set_id: 0,
                keys: vec![Some(AggregateGroupKeyRef::from_array(Arc::clone(&keys), row)?)],
                aggregate_states: vec![count_state(1)?],
            })
        },
    )?;

    let mut reader = layout
        .open_reader(Cursor::new(output.into_inner()))
        .expect("open generated aggregate spill run");
    let mut rows = 0;
    let mut batches = 0;
    for batch in reader.reader.by_ref() {
        let view = crate::pipeline::arrow_adapter::from_record_batch(
            batch.expect("read generated aggregate spill batch"),
        )?;
        let memory = layout.decoded_batch_memory(&view)?;
        let decoded_bytes = memory.total_bytes()?;
        assert!(
            decoded_bytes <= target_bytes || view.num_rows() == 1,
            "spill batch of {} rows uses {decoded_bytes} bytes (workspace {}, retained keys {}), target is {target_bytes}",
            memory.workspace_bytes,
            memory.retained_key_bytes,
            view.num_rows()
        );
        rows += view.num_rows();
        batches += 1;
    }

    assert_eq!(rows, group_count);
    assert!(batches > 1, "the memory budget must split the default 64-row batch");
    Ok(())
}
