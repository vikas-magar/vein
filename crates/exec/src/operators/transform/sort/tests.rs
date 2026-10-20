use std::sync::{Arc, Mutex};

use arrow::array::{Array, ArrayRef, Int64Array, StringArray};
use arrow::record_batch::RecordBatch;
use common::arrow::SelectionVector;
use common::protocol::{LimitSpec, SortKey, SortPlan};

use super::*;
use crate::operators::transform::limit::LimitTransform;
use crate::pipeline::DataChunkView;

#[derive(Clone)]
struct Collect(Arc<Mutex<Vec<DataChunkView>>>);

impl PipelineNode for Collect {
    fn push(&mut self, view: DataChunkView) -> Result<()> {
        self.0.lock().unwrap().push(view);
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        Ok(())
    }
}

fn strings(values: &[Option<&str>]) -> ArrayRef {
    Arc::new(StringArray::from(values.to_vec()))
}

fn integers(values: &[i64]) -> ArrayRef {
    Arc::new(Int64Array::from(values.to_vec()))
}

fn view(view: DataChunkView) -> DataChunkView {
    view
}

fn int_at(view: &DataChunkView, column: usize, row: usize) -> Option<i64> {
    let (array, physical_row) = view.resolve(column, row).ok()??;
    let values = array
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("int64 view column");
    values
        .is_valid(physical_row)
        .then(|| values.value(physical_row))
}

fn string_at(view: &DataChunkView, column: usize, row: usize) -> Option<String> {
    let (array, physical_row) = view.resolve(column, row).ok()??;
    let values = array
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("string view column");
    values
        .is_valid(physical_row)
        .then(|| values.value(physical_row).to_owned())
}

#[test]
fn globally_sorts_multiple_chunks_by_keys_and_null_order() {
    let output = Arc::new(Mutex::new(Vec::new()));
    let plan = SortPlan {
        keys: vec![
            SortKey {
                column: 0,
                ascending: true,
                nulls_first: false,
            },
            SortKey {
                column: 1,
                ascending: false,
                nulls_first: false,
            },
        ],
    };
    let mut sort = SortTransform::new(plan, Box::new(Collect(output.clone())));
    sort.push(view(crate::pipeline::test_support::selected_view(
        vec![strings(&[Some("b"), Some("a")]), integers(&[1, 1])],
        SelectionVector::new_dense(2),
    )))
        .unwrap();
    sort.push(view(crate::pipeline::test_support::selected_view(
        vec![strings(&[Some("b"), None]), integers(&[3, 9])],
        SelectionVector::new_dense(2),
    )))
        .unwrap();

    sort.flush().unwrap();

    let chunks = output.lock().unwrap();
    let labels: Vec<Option<String>> = chunks
        .iter()
        .flat_map(|view| (0..view.num_rows()).map(|row| string_at(view, 0, row)))
        .collect();
    let tie_breakers: Vec<i64> = chunks
        .iter()
        .flat_map(|view| (0..view.num_rows()).map(|row| int_at(view, 1, row).unwrap()))
        .collect();

    assert_eq!(
        labels,
        vec![Some("a".into()), Some("b".into()), Some("b".into()), None]
    );
    assert_eq!(tie_breakers, vec![1, 3, 1, 9]);
}

#[test]
fn full_sort_accounts_distinct_source_arrays_once_across_retained_views() {
    let output = Arc::new(Mutex::new(Vec::new()));
    let tracker = QueryMemoryTracker::unlimited(common::cancellation::CancellationToken::new());
    let source = integers(&[2, 1]);
    let source_bytes = source.get_array_memory_size();
    let row_reservation =
        2 * size_of::<RowRef>() + size_of::<Option<DataChunkView>>() + size_of::<usize>();
    let mut sort = SortTransform::with_memory_tracker(
        SortPlan {
            keys: vec![SortKey {
                column: 0,
                ascending: true,
                nulls_first: false,
            }],
        },
        None,
        tracker.clone(),
        Box::new(Collect(Arc::clone(&output))),
    )
        .expect("construct sort with tracked memory");

    for _ in 0..2 {
        sort.push(view(crate::pipeline::test_support::dense_view(vec![
            Arc::clone(&source),
        ])))
            .expect("retain view without copying source values");
    }

    assert_eq!(
        tracker.used(),
        source_bytes + 2 * row_reservation,
        "two views share one source-array reservation"
    );
    sort.flush().expect("sort retained views");

    let output = output.lock().unwrap();
    let values = output
        .iter()
        .flat_map(|view| (0..view.num_rows()).map(|row| int_at(view, 0, row).unwrap()))
        .collect::<Vec<_>>();
    assert_eq!(values, vec![1, 1, 2, 2]);
    for view in output.iter() {
        for row in 0..view.num_rows() {
            let (array, _) = view.resolve(0, row).unwrap().unwrap();
            assert!(Arc::ptr_eq(array, &source));
        }
    }
}

#[test]
fn top_n_skips_non_candidates_and_rejects_unaccountable_candidate_before_heap_mutation() {
    let output = Arc::new(Mutex::new(Vec::new()));
    let first_key = integers(&[5]);
    let first_payload = strings(&[Some("small")]);
    let first_bytes = first_key.get_array_memory_size() + first_payload.get_array_memory_size();
    let tracker = QueryMemoryTracker::new(
        first_bytes + 1024,
        common::cancellation::CancellationToken::new(),
    );
    let mut sort = SortTransform::with_limit_and_memory_tracker(
        SortPlan {
            keys: vec![SortKey {
                column: 0,
                ascending: true,
                nulls_first: false,
            }],
        },
        None,
        Some(LimitSpec {
            offset: 0,
            count: 1,
        }),
        tracker.clone(),
        Box::new(Collect(Arc::clone(&output))),
    )
        .expect("construct budgeted Top-N sort");
    sort.push(view(crate::pipeline::test_support::dense_view(vec![
        Arc::clone(&first_key),
        Arc::clone(&first_payload),
    ])))
        .expect("retain first candidate");
    let retained_bytes = tracker.used();

    let large_payload = strings(&[Some(&"x".repeat(8192))]);
    sort.push(view(crate::pipeline::test_support::dense_view(vec![
        integers(&[99]),
        Arc::clone(&large_payload),
    ])))
        .expect("discard a batch that cannot enter Top-N");
    assert_eq!(tracker.used(), retained_bytes);
    assert_eq!(sort.chunks.len(), 1, "non-candidate view is not retained");
    let retained_sort_bytes = sort.memory.bytes();

    let error = sort
        .push(view(crate::pipeline::test_support::dense_view(vec![
            integers(&[1]),
            Arc::clone(&large_payload),
        ])))
        .expect_err("candidate source arrays exceed the query budget");
    assert!(matches!(error, VeinError::ResourceExhausted { .. }));
    assert_eq!(tracker.used(), retained_bytes);
    assert_eq!(sort.memory.bytes(), retained_sort_bytes);
    assert_eq!(
        sort.chunks.len(),
        1,
        "failed accounting leaves retained chunks intact"
    );
    let retained_row = *sort.top_n.as_ref().unwrap().peek().unwrap();
    assert_eq!(retained_row.chunk, 0);
    assert_eq!(
        retained_row.row, 0,
        "failed accounting does not mutate the Top-N heap"
    );

    sort.flush().expect("emit the original retained candidate");
    let values = output.lock().unwrap();
    assert_eq!(values.len(), 1);
    assert_eq!(int_at(&values[0], 0, 0), Some(5));
}

#[test]
fn full_sort_includes_source_arrays_when_selecting_spill_path() {
    let output = Arc::new(Mutex::new(Vec::new()));
    let source = integers(&(0..10).rev().collect::<Vec<_>>());
    let source_bytes = source.get_array_memory_size();
    let row_count = 10;
    let row_bytes =
        row_count * size_of::<RowRef>() + size_of::<Option<DataChunkView>>() + size_of::<usize>();
    let tracker = QueryMemoryTracker::new(
        source_bytes + row_bytes - 1,
        common::cancellation::CancellationToken::new(),
    );
    let mut sort = SortTransform::with_memory_tracker(
        SortPlan {
            keys: vec![SortKey {
                column: 0,
                ascending: true,
                nulls_first: false,
            }],
        },
        None,
        tracker.clone(),
        Box::new(Collect(Arc::clone(&output))),
    )
        .expect("construct constrained sort");

    sort.push(view(crate::pipeline::test_support::dense_view(vec![
        source,
    ])))
        .expect("fall back to external sorting when retained memory is over budget");
    assert!(
        sort.spilling,
        "source arrays are included in the sort budget"
    );
    assert_eq!(
        sort.spill_runs.len(),
        1,
        "the batch is written as a spill run"
    );
    assert_eq!(
        sort.metrics.spill_write_materialization.gathered_columns, 1,
        "spill serialization gathers selected values into an IPC batch"
    );
    assert!(
        sort.metrics
            .spill_write_materialization
            .gathered_output_buffer_bytes
            > 0,
        "spill output buffers are accounted separately from sort comparisons"
    );
    assert!(sort.metrics.row_comparisons > 0);
    assert!(tracker.used() <= tracker.limit());
    let spill_directory = tracker
        .spill_manager()
        .local_directory()
        .expect("sort spill directory");

    drop(sort);
    drop(tracker);
    assert!(
        !spill_directory.exists(),
        "the query spill directory is removed after the sort drops"
    );
    assert!(output.lock().unwrap().is_empty());
}

#[test]
fn sort_composes_cross_batch_payload_references_with_sparse_selections() {
    let output = Arc::new(Mutex::new(Vec::new()));
    let first_keys = integers(&[4, 999, 1]);
    let first_payload = integers(&[40, 9_990, 10]);
    let second_keys = integers(&[3, 2]);
    let second_payload = integers(&[30, 20]);
    let mut sort = SortTransform::new(
        SortPlan {
            keys: vec![SortKey {
                column: 1,
                ascending: true,
                nulls_first: false,
            }],
        },
        Box::new(Collect(output.clone())),
    );
    sort.push(view(crate::pipeline::test_support::selected_view(
        vec![Arc::clone(&first_keys), Arc::clone(&first_payload)],
        SelectionVector::new_sparse(vec![0, 2]),
    )))
        .expect("push first sparse input view");
    sort.push(view(crate::pipeline::test_support::selected_view(
        vec![Arc::clone(&second_keys), Arc::clone(&second_payload)],
        SelectionVector::new_dense(2),
    )))
        .expect("push second input view");

    sort.flush().expect("sort sparse input views");

    let output = output.lock().unwrap();
    let sorted_keys = output
        .iter()
        .flat_map(|view| (0..view.num_rows()).map(|row| int_at(view, 0, row).unwrap()))
        .collect::<Vec<_>>();
    let sorted_payload = output
        .iter()
        .flat_map(|view| (0..view.num_rows()).map(|row| int_at(view, 1, row).unwrap()))
        .collect::<Vec<_>>();
    assert_eq!(sorted_keys, vec![1, 2, 3, 4]);
    assert_eq!(sorted_payload, vec![10, 20, 30, 40]);
    assert_eq!(sort.metrics.output_rows, 4);
    for view in output.iter() {
        for row in 0..view.num_rows() {
            let (array, _) = view.resolve(1, row).unwrap().unwrap();
            assert!(
                Arc::ptr_eq(array, &first_payload) || Arc::ptr_eq(array, &second_payload),
                "sorted payload must still reference an input array"
            );
        }
    }
}

#[test]
fn single_chunk_sort_reuses_arrow_columns_and_reorders_selection() {
    let output = Arc::new(Mutex::new(Vec::new()));
    let values = integers(&[3, 1, 2]);
    let mut sort = SortTransform::new(
        SortPlan {
            keys: vec![SortKey {
                column: 0,
                ascending: true,
                nulls_first: false,
            }],
        },
        Box::new(Collect(output.clone())),
    );
    sort.push(view(crate::pipeline::test_support::dense_view(vec![
        values.clone(),
    ])))
        .unwrap();

    sort.flush().unwrap();

    let chunks = output.lock().unwrap();
    assert_eq!(chunks.len(), 1);
    let (array, _) = chunks[0].resolve(0, 0).unwrap().unwrap();
    assert!(Arc::ptr_eq(array, &values));
    let selected: Vec<i64> = (0..chunks[0].num_rows())
        .map(|row| int_at(&chunks[0], 0, row).unwrap())
        .collect();
    assert_eq!(selected, vec![1, 2, 3]);
}

#[test]
fn full_sort_and_top_n_compare_retained_sort_values_without_gathering() {
    for top_n in [false, true] {
        let output = Arc::new(Mutex::new(Vec::new()));
        let keys = integers(&[30, 10, 20]);
        let payload = integers(&[300, 100, 200]);
        let mut sort = SortTransform::with_limit(
            SortPlan {
                keys: vec![SortKey {
                    column: 0,
                    ascending: true,
                    nulls_first: false,
                }],
            },
            None,
            top_n.then_some(LimitSpec {
                offset: 0,
                count: 2,
            }),
            Box::new(Collect(Arc::clone(&output))),
        );
        for selection in [
            SelectionVector::new_dense(3),
            SelectionVector::new_sparse(vec![2, 0]),
        ] {
            sort.push(view(crate::pipeline::test_support::selected_view(
                vec![Arc::clone(&keys), Arc::clone(&payload)],
                selection,
            )))
                .unwrap();
        }
        sort.flush().unwrap();
        assert_eq!(
            sort.metrics.spill_write_materialization,
            MaterializationReport::default()
        );
        assert!(sort.metrics.row_comparisons > 0);
        assert!(sort.metrics.comparator_cache_entries > 0);
        let views = output.lock().unwrap();
        let rows: Vec<_> = views
            .iter()
            .flat_map(|view| {
                (0..view.num_rows()).map(|row| {
                    assert!(Arc::ptr_eq(
                        view.resolve(1, row).unwrap().unwrap().0,
                        &payload
                    ));
                    (int_at(view, 0, row).unwrap(), int_at(view, 1, row).unwrap())
                })
            })
            .collect();
        let expected = if top_n {
            vec![(10, 100), (20, 200)]
        } else {
            vec![(10, 100), (20, 200), (20, 200), (30, 300), (30, 300)]
        };
        assert_eq!(rows, expected);
    }
}

#[test]
fn top_n_sorts_only_offset_plus_count_and_keeps_single_chunk_payload_shared() {
    let output = Arc::new(Mutex::new(Vec::new()));
    let values = integers(&[4, 1, 3, 2]);
    let sink = Box::new(Collect(output.clone()));
    let window = Box::new(LimitTransform::new(1, 2, sink));
    let mut sort = SortTransform::with_limit(
        SortPlan {
            keys: vec![SortKey {
                column: 0,
                ascending: true,
                nulls_first: false,
            }],
        },
        None,
        Some(LimitSpec {
            offset: 1,
            count: 2,
        }),
        window,
    );
    sort.push(view(crate::pipeline::test_support::dense_view(vec![
        values.clone(),
    ])))
        .unwrap();

    sort.flush().unwrap();

    let chunks = output.lock().unwrap();
    assert_eq!(chunks.len(), 1);
    let (array, _) = chunks[0].resolve(0, 0).unwrap().unwrap();
    assert!(Arc::ptr_eq(array, &values));
    let sorted: Vec<i64> = (0..chunks[0].num_rows())
        .map(|row| int_at(&chunks[0], 0, row).unwrap())
        .collect();
    assert_eq!(sorted, vec![2, 3]);
}

#[test]
fn top_n_releases_chunks_without_current_winning_rows() {
    let output = Arc::new(Mutex::new(Vec::new()));
    let winner = integers(&[1]);
    let mut sort = SortTransform::with_limit(
        SortPlan {
            keys: vec![SortKey {
                column: 0,
                ascending: true,
                nulls_first: false,
            }],
        },
        None,
        Some(LimitSpec {
            offset: 0,
            count: 1,
        }),
        Box::new(Collect(output.clone())),
    );
    sort.push(view(crate::pipeline::test_support::dense_view(vec![
        integers(&[8, 9]),
    ])))
        .unwrap();
    sort.push(view(crate::pipeline::test_support::dense_view(vec![
        winner.clone(),
    ])))
        .unwrap();

    assert!(sort.chunks[0].is_none());
    assert!(sort.chunks[1].is_some());
    sort.flush().unwrap();

    let chunks = output.lock().unwrap();
    assert_eq!(chunks.len(), 1);
    let (array, _) = chunks[0].resolve(0, 0).unwrap().unwrap();
    assert!(Arc::ptr_eq(array, &winner));
}

#[test]
fn top_n_gathers_sorted_winners_from_multiple_chunks() {
    let output = Arc::new(Mutex::new(Vec::new()));
    let mut sort = SortTransform::with_limit(
        SortPlan {
            keys: vec![SortKey {
                column: 0,
                ascending: true,
                nulls_first: false,
            }],
        },
        None,
        Some(LimitSpec {
            offset: 0,
            count: 3,
        }),
        Box::new(Collect(output.clone())),
    );
    let first = integers(&[4, 1]);
    let second = integers(&[3, 2]);
    sort.push(view(crate::pipeline::test_support::dense_view(vec![
        first.clone(),
    ])))
        .unwrap();
    sort.push(view(crate::pipeline::test_support::dense_view(vec![
        second.clone(),
    ])))
        .unwrap();

    let replacement = sort
        .top_n
        .as_ref()
        .unwrap()
        .entries
        .iter()
        .find(|row| row.chunk == 1 && row.row == 1)
        .expect("second chunk's better candidate remains in the heap");
    assert_eq!(replacement.chunk, 1);
    assert_eq!(replacement.row, 1);

    sort.flush().unwrap();

    assert_eq!(sort.metrics.input_batches, 2);
    assert_eq!(sort.metrics.input_rows, 4);
    assert_eq!(sort.metrics.candidate_rows_examined, 4);
    assert_eq!(sort.metrics.candidate_insertions, 4);
    assert_eq!(sort.metrics.candidate_replacements, 1);
    assert!(sort.metrics.row_comparisons > 0);
    assert_eq!(
        sort.metrics.spill_write_materialization,
        MaterializationReport::default()
    );
    assert_eq!(sort.metrics.output_rows, 3);

    let chunks = output.lock().unwrap();
    let sorted: Vec<i64> = chunks
        .iter()
        .flat_map(|view| (0..view.num_rows()).map(|row| int_at(view, 0, row).unwrap()))
        .collect();
    assert_eq!(sorted, vec![1, 2, 3]);
    for output in chunks.iter() {
        for row in 0..output.num_rows() {
            let (array, _) = output.resolve(0, row).unwrap().unwrap();
            assert!(Arc::ptr_eq(array, &first) || Arc::ptr_eq(array, &second));
        }
    }
}

#[test]
fn top_n_sort_reuses_its_candidate_reference_buffer() {
    let mut top_n = BoundedTopN::new(4);
    let mut compare = |left: &i64, right: &i64| Ok(left.cmp(right));
    for value in [3, 1, 4, 2] {
        assert!(matches!(
            top_n.offer(value, &mut compare).unwrap(),
            TopNChange::Inserted
        ));
    }
    let candidate_buffer = top_n.entries.as_ptr();

    let sorted = top_n.sort_and_take_by(&mut compare).unwrap();

    assert_eq!(sorted.as_ptr(), candidate_buffer);
    assert_eq!(sorted, vec![1, 2, 3, 4]);
}

#[test]
fn top_n_keeps_candidate_reference_reservation_when_replacing_source_chunk() {
    let output = Arc::new(Mutex::new(Vec::new()));
    let tracker = QueryMemoryTracker::new(1 << 20, common::cancellation::CancellationToken::new());
    let mut sort = SortTransform::with_limit_and_memory_tracker(
        SortPlan {
            keys: vec![SortKey {
                column: 0,
                ascending: true,
                nulls_first: false,
            }],
        },
        None,
        Some(LimitSpec {
            offset: 0,
            count: 1,
        }),
        tracker.clone(),
        Box::new(Collect(output)),
    )
        .expect("create budgeted Top-N sort");
    let long_key = Arc::new(StringArray::from(vec!["z".repeat(128)]));
    let short_key = Arc::new(StringArray::from(vec!["a"]));
    sort.push(view(crate::pipeline::test_support::dense_view(vec![
        long_key,
    ])))
        .expect("retain first candidate");
    let before_replacement = sort.memory.bytes();
    let chunk_metadata = size_of::<Option<DataChunkView>>() + size_of::<usize>();
    let short_key_bytes = short_key.get_array_memory_size();

    sort.push(view(crate::pipeline::test_support::dense_view(vec![
        short_key,
    ])))
        .expect("replace with better short candidate");

    assert_eq!(
        sort.memory.bytes(),
        before_replacement + chunk_metadata,
        "replacement keeps the candidate reference and accounts the new chunk"
    );
    assert_eq!(
        tracker.used(),
        sort.memory.bytes() + short_key_bytes,
        "the current source array is budgeted separately from sort metadata"
    );
}

#[test]
fn zero_count_top_n_finishes_without_retaining_input() {
    let output = Arc::new(Mutex::new(Vec::new()));
    let mut sort = SortTransform::with_limit(
        SortPlan {
            keys: vec![SortKey {
                column: 0,
                ascending: true,
                nulls_first: false,
            }],
        },
        None,
        Some(LimitSpec {
            offset: 10,
            count: 0,
        }),
        Box::new(Collect(output.clone())),
    );

    assert!(sort.finished());
    sort.push(view(crate::pipeline::test_support::dense_view(vec![
        integers(&[3, 1, 2]),
    ])))
        .unwrap();
    sort.flush().unwrap();

    assert!(sort.chunks.is_empty());
    assert!(output.lock().unwrap().is_empty());
}

#[test]
fn sort_reference_growth_respects_budget_and_releases_on_error() {
    let tracker = QueryMemoryTracker::new(0, common::cancellation::CancellationToken::new());
    let mut sort = SortTransform::with_memory_tracker(
        SortPlan {
            keys: vec![SortKey {
                column: 0,
                ascending: true,
                nulls_first: false,
            }],
        },
        None,
        tracker.clone(),
        Box::new(Collect(Arc::new(Mutex::new(Vec::new())))),
    )
        .expect("empty sort state fits zero bytes");
    let error = sort
        .push(view(crate::pipeline::test_support::dense_view(vec![
            integers(&[3, 1, 2]),
        ])))
        .expect_err("sort row references must reserve memory before growth");
    assert!(matches!(error, VeinError::ResourceExhausted { .. }));
    drop(sort);
    assert_eq!(tracker.used(), 0, "failed sort state releases reservations");
}

#[test]
fn memory_pressure_switches_sort_to_spill_and_releases_temporary_reservations() {
    let tracker = QueryMemoryTracker::new(64, common::cancellation::CancellationToken::new());
    let mut sort = SortTransform::with_memory_tracker(
        SortPlan {
            keys: vec![SortKey {
                column: 0,
                ascending: true,
                nulls_first: false,
            }],
        },
        None,
        tracker.clone(),
        Box::new(Collect(Arc::new(Mutex::new(Vec::new())))),
    )
        .expect("construct sort under a small budget");
    sort.push(view(crate::pipeline::test_support::dense_view(vec![
        integers(&[2]),
    ])))
        .expect("retain first row within budget");

    sort.push(view(crate::pipeline::test_support::dense_view(vec![
        integers(&[1]),
    ])))
        .expect("spill retained and incoming views when references exceed budget");

    assert!(sort.spilling);
    assert_eq!(sort.spill_runs.len(), 2);
    assert!(sort.spill_runs.iter().all(|path| path.exists()));
    assert!(tracker.used() <= tracker.limit());
    let spill_directory = tracker
        .spill_manager()
        .local_directory()
        .expect("spill directory");
    drop(sort);
    drop(tracker);
    assert!(
        !spill_directory.exists(),
        "failed or unfinished spill state is cleaned"
    );
}

#[test]
fn spill_runs_merge_without_losing_projection_and_account_for_retained_output() {
    let output = Arc::new(Mutex::new(Vec::new()));
    let tracker = QueryMemoryTracker::unlimited(common::cancellation::CancellationToken::new());
    let keys_a = integers(&[4, 1]);
    let payload_a = integers(&[40, 10]);
    let keys_b = integers(&[3, 2]);
    let payload_b = integers(&[30, 20]);
    let mut sort = SortTransform::with_memory_tracker(
        SortPlan {
            keys: vec![SortKey {
                column: 1,
                ascending: true,
                nulls_first: false,
            }],
        },
        Some(1),
        tracker.clone(),
        Box::new(Collect(Arc::clone(&output))),
    )
        .expect("construct constrained sort");
    sort.spilling = true;
    sort.push(view(crate::pipeline::test_support::dense_view(vec![
        Arc::clone(&payload_a),
        Arc::clone(&keys_a),
    ])))
        .expect("retain first batch within budget");
    sort.push(view(crate::pipeline::test_support::dense_view(vec![
        Arc::clone(&payload_b),
        Arc::clone(&keys_b),
    ])))
        .expect("spill after the second batch exceeds the budget");
    let spill_directory = tracker
        .spill_manager()
        .local_directory()
        .expect("spill run directory");

    sort.flush().expect("merge sorted spill runs");

    let collected = output.lock().unwrap();
    let sorted_payload = collected
        .iter()
        .flat_map(|view| (0..view.num_rows()).map(|row| int_at(view, 0, row).unwrap()))
        .collect::<Vec<_>>();
    assert_eq!(sorted_payload, vec![10, 20, 30, 40]);
    assert!(collected.iter().all(|view| view.num_columns() == 1));
    assert!(sort.metrics.spill_runs >= 2);
    assert!(sort.metrics.spill_bytes > 0);
    assert!(collected.iter().all(|view| {
        let (array, _) = view.resolve(0, 0).unwrap().unwrap();
        !Arc::ptr_eq(array, &payload_a) && !Arc::ptr_eq(array, &payload_b)
    }));
    assert!(
        tracker.used() > 0,
        "output views retain spill array reservations"
    );
    assert_eq!(
        tracker.spill_bytes_used(),
        0,
        "completed external merge releases every consumed run"
    );
    drop(collected);
    output.lock().unwrap().clear();
    assert_eq!(
        tracker.used(),
        0,
        "dropping output views releases spill reservations"
    );
    drop(sort);
    drop(tracker);
    assert!(
        !spill_directory.exists(),
        "spill directory is cleaned after query state drops"
    );
}

#[test]
fn external_sort_uses_bounded_multi_pass_merge_for_many_spill_runs() {
    let output = Arc::new(Mutex::new(Vec::new()));
    let tracker = QueryMemoryTracker::unlimited(common::cancellation::CancellationToken::new());
    let mut sort = SortTransform::with_memory_tracker(
        SortPlan {
            keys: vec![SortKey {
                column: 0,
                ascending: true,
                nulls_first: false,
            }],
        },
        None,
        tracker.clone(),
        Box::new(Collect(Arc::clone(&output))),
    )
        .expect("construct constrained sort");
    sort.spilling = true;
    for value in (0..68i64).rev() {
        sort.push(view(crate::pipeline::test_support::dense_view(vec![
            integers(&[value]),
        ])))
            .expect("spill input after memory budget fills");
    }
    assert!(sort.spill_runs.len() > SPILL_MERGE_FAN_IN);

    sort.flush().expect("merge spill runs over bounded passes");

    let collected = output.lock().unwrap();
    let values = collected
        .iter()
        .flat_map(|view| (0..view.num_rows()).map(|row| int_at(view, 0, row).unwrap()))
        .collect::<Vec<_>>();
    assert_eq!(values, (0..68i64).collect::<Vec<_>>());
    assert_eq!(sort.metrics.spill_merge_passes, 1);
    assert!(
        tracker.used() > 0,
        "output views retain decoded spill arrays"
    );
    drop(collected);
    output.lock().unwrap().clear();
    drop(sort);
    assert_eq!(
        tracker.used(),
        0,
        "dropping output views releases spill reservations"
    );
}

#[test]
fn external_sort_preserves_null_order_and_duplicate_keys() {
    let output = Arc::new(Mutex::new(Vec::new()));
    let tracker = QueryMemoryTracker::unlimited(common::cancellation::CancellationToken::new());
    let mut sort = SortTransform::with_memory_tracker(
        SortPlan {
            keys: vec![SortKey {
                column: 1,
                ascending: true,
                nulls_first: true,
            }],
        },
        Some(1),
        tracker,
        Box::new(Collect(Arc::clone(&output))),
    )
        .expect("construct constrained sort");
    sort.spilling = true;
    for (payload, key) in [
        (vec![1, 2], vec![None, Some("z")]),
        (vec![3, 4], vec![Some("a"), None]),
    ] {
        sort.push(view(crate::pipeline::test_support::dense_view(vec![
            integers(&payload),
            strings(&key),
        ])))
            .expect("spill input rows");
    }

    sort.flush().expect("merge duplicate and NULL keys");

    let output = output.lock().unwrap();
    let payload = output
        .iter()
        .flat_map(|view| (0..view.num_rows()).map(|row| int_at(view, 0, row).unwrap()))
        .collect::<Vec<_>>();
    assert_eq!(payload, vec![1, 4, 3, 2]);
}

#[test]
fn canceled_external_merge_returns_cancelled_and_cleans_spill_files() {
    let output = Arc::new(Mutex::new(Vec::new()));
    let cancel = common::cancellation::CancellationToken::new();
    let tracker = QueryMemoryTracker::new(64, cancel.clone());
    let mut sort = SortTransform::with_memory_tracker(
        SortPlan {
            keys: vec![SortKey {
                column: 0,
                ascending: true,
                nulls_first: false,
            }],
        },
        None,
        tracker.clone(),
        Box::new(Collect(Arc::clone(&output))),
    )
        .expect("construct constrained sort");
    for values in [[4, 1], [3, 2]] {
        sort.push(view(crate::pipeline::test_support::dense_view(vec![
            integers(&values),
        ])))
            .expect("spill input rows");
    }
    let spill_directory = tracker
        .spill_manager()
        .local_directory()
        .expect("spill run directory");
    cancel.cancel();

    assert!(matches!(sort.flush(), Err(VeinError::Cancelled)));
    assert!(output.lock().unwrap().is_empty());
    drop(sort);
    drop(tracker);
    assert!(
        !spill_directory.exists(),
        "cancelled query spill files are cleaned"
    );
}

#[test]
fn external_merge_emits_bounded_batches_across_run_batch_boundaries() {
    let output = Arc::new(Mutex::new(Vec::new()));
    let tracker = QueryMemoryTracker::unlimited(common::cancellation::CancellationToken::new());
    let mut sort = SortTransform::with_memory_tracker(
        SortPlan {
            keys: vec![SortKey {
                column: 0,
                ascending: true,
                nulls_first: false,
            }],
        },
        None,
        tracker,
        Box::new(Collect(Arc::clone(&output))),
    )
        .expect("construct constrained sort");
    sort.spilling = true;
    for batch in (0..4i64).rev() {
        let values = (0..9_000i64)
            .rev()
            .map(|value| value + batch * 9_000)
            .collect::<Vec<_>>();
        sort.push(view(crate::pipeline::test_support::dense_view(vec![
            integers(&values),
        ])))
            .expect("spill after the retained views exceed the budget");
    }

    sort.flush().expect("merge runs with multiple IPC batches");

    assert!(
        sort.metrics.spill_write_materialization.gathered_columns > 0,
        "spill runs gather output arrays at the IPC boundary"
    );
    assert!(
        sort.metrics
            .spill_write_materialization
            .gathered_output_buffer_bytes
            > 0
    );
    assert!(sort.metrics.row_comparisons > 0);
    assert!(sort.metrics.comparator_cache_entries > 0);

    let output = output.lock().unwrap();
    assert!(output.len() > 1);
    assert!(
        output
            .iter()
            .all(|view| view.num_rows() <= OUTPUT_BATCH_ROWS)
    );
    let values = output
        .iter()
        .flat_map(|view| (0..view.num_rows()).map(|row| int_at(view, 0, row).unwrap()))
        .collect::<Vec<_>>();
    assert_eq!(values, (0..36_000i64).collect::<Vec<_>>());
}

#[test]
fn spill_array_reservation_lives_until_the_last_composed_view_is_dropped() {
    let tracker = QueryMemoryTracker::unlimited(common::cancellation::CancellationToken::new());
    let source: ArrayRef = Arc::new(Int64Array::from(vec![10, 20, 30]));
    let source_bytes = source.get_array_memory_size();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        arrow::datatypes::DataType::Int64,
        false,
    )]));
    let batch = RecordBatch::try_new(schema, vec![source]).expect("spill batch");
    let batch = crate::pipeline::arrow_adapter::from_record_batch(batch).expect("spill view");
    let input = view_from_spill_batch(batch, &tracker).expect("reference spill arrays");
    assert_eq!(tracker.used(), source_bytes);

    let output = DataChunkView::compose_rows(&[&input], &[(0, 2), (0, 0)], 1)
        .expect("compose output rows without copying payloads");
    drop(input);
    assert_eq!(
        tracker.used(),
        source_bytes,
        "composed output owns the lease"
    );
    assert_eq!(int_at(&output, 0, 0), Some(30));
    assert_eq!(int_at(&output, 0, 1), Some(10));

    drop(output);
    assert_eq!(tracker.used(), 0, "last view drop releases the reservation");
}
