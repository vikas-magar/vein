//! Date32 grouping and spill regression coverage.

use super::*;

fn date_sum_plan() -> AggregatePlan {
    AggregatePlan {
        groups: 1,
        estimated_groups: None,
        grouping_sets: None,
        emit_empty_group_on_empty_input: false,
        aggs: vec![AggOpCode::Sum],
        aggregate_input_types: vec![VeinDataType::Int64],
        types: vec![VeinDataType::Date32, VeinDataType::int128()],
    }
}

async fn collect_date_sums(
    sink: &mut AggregateSink,
) -> Result<std::collections::BTreeMap<Option<i32>, i128>> {
    let mut groups = std::collections::BTreeMap::new();
    sink.finish_with(AGGREGATE_BATCH_ROWS, |batch: DataChunkView| {
        let dates = crate::pipeline::test_support::materialized_column::<Date32Array>(&batch, 0);
        let sums = int128_column(&batch, 1);
        for row in 0..batch.num_rows() {
            groups.insert(dates.is_valid(row).then(|| dates.value(row)), sums.value(row));
        }
        Ok(())
    }).await?;
    Ok(groups)
}

#[tokio::test]
async fn date32_group_keys_preserve_nulls_and_merge_repeated_values_across_batches() {
    let mut sink = AggregateSink::new(date_sum_plan()).expect("create aggregate sink");
    sink.accumulate(crate::pipeline::test_support::dense_view(vec![
        Arc::new(Date32Array::from(vec![Some(10), None, Some(20), Some(10)])),
        Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
    ]))
        .expect("accumulate first date batch");
    sink.accumulate(crate::pipeline::test_support::dense_view(vec![
        Arc::new(Date32Array::from(vec![Some(20), None, Some(30)])),
        Arc::new(Int64Array::from(vec![5, 6, 7])),
    ]))
        .expect("accumulate repeated date keys");

    assert_eq!(
        collect_date_sums(&mut sink).await.expect("finish date aggregate"),
        std::collections::BTreeMap::from([
            (None, 8),
            (Some(10), 5),
            (Some(20), 8),
            (Some(30), 7),
        ])
    );
}

#[tokio::test]
async fn date32_group_keys_survive_aggregate_spill_and_merge() {
    let tracker = QueryMemoryTracker::new(
        32 * 1024,
        common::cancellation::CancellationToken::new(),
    );
    let mut sink = AggregateSink::with_memory_tracker(date_sum_plan(), tracker)
        .expect("create budgeted date aggregate");
    let rows = 4096_i64;
    let dates = (0..rows).map(|row| Some((row % 257) as i32)).collect::<Vec<_>>();
    sink.accumulate(crate::pipeline::test_support::dense_view(vec![
        Arc::new(Date32Array::from(dates)),
        Arc::new(Int64Array::from_iter_values(1..=rows)),
    ]))
        .expect("spill date groups under the small memory budget");
    assert!(!sink.spill_runs.is_empty(), "date groups must exercise spill");

    let actual = collect_date_sums(&mut sink).await.expect("merge spilled date groups");
    assert_eq!(actual.len(), 257);
    for (date, sum) in actual {
        let key = i64::from(date.expect("test data has no null dates"));
        let expected = (0..rows)
            .filter(|row| row % 257 == key)
            .map(|row| row + 1)
            .sum::<i64>();
        assert_eq!(sum, i128::from(expected), "wrong sum for date key {key}");
    }
}
