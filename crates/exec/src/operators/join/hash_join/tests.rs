use std::sync::{Arc, Mutex};
use std::time::Instant;

use crate::pipeline::{DataChunkView, MaterializationReport, PipelineNode};
use arrow::array::{Array, ArrayRef, Int64Array};
use arrow::datatypes::DataType;
use common::Result;
use error::VeinError;

use super::super::{HashTable, JoinKeys, JoinType, OutputColumn};
use super::*;
#[path = "tests/residual_support.rs"]
mod residual_support;
use residual_support::{
    TestExpressionExecutor, expression_binary, expression_column, expression_filter,
};

#[derive(Clone, Default)]
struct Collect(Arc<Mutex<Vec<DataChunkView>>>);

impl PipelineNode<DataChunkView> for Collect {
    fn push(&mut self, view: DataChunkView) -> Result<()> {
        self.0.lock().expect("collect lock").push(view);
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        Ok(())
    }

    fn fork_local(&self) -> Result<Option<Box<dyn PipelineNode<DataChunkView>>>> {
        Ok(Some(Box::new(Self::default())))
    }

    fn combine_local(
        self: Box<Self>,
        canonical: &mut dyn PipelineNode<DataChunkView>,
    ) -> Result<()> {
        let target = canonical
            .as_any_mut()
            .downcast_mut::<Collect>()
            .ok_or_else(|| VeinError::internal("collect local state has a mismatched target"))?;
        let mut local_chunks = self
            .0
            .lock()
            .map_err(|_| VeinError::internal("collect mutex poisoned"))?;
        let mut target_chunks = target
            .0
            .lock()
            .map_err(|_| VeinError::internal("collect mutex poisoned"))?;
        target_chunks
            .try_reserve(local_chunks.len())
            .map_err(|error| {
                VeinError::internal(format!("cannot reserve collected test batches: {error}"))
            })?;
        target_chunks.extend(std::mem::take(&mut *local_chunks));
        Ok(())
    }
}

impl HashJoin {
    fn push(&mut self, chunk: DataChunkView) -> Result<()> {
        PipelineNode::<DataChunkView>::push(self, chunk)
    }

    fn flush(&mut self) -> Result<()> {
        PipelineNode::<DataChunkView>::flush(self)
    }
}

fn ints(values: &[i64]) -> ArrayRef {
    Arc::new(Int64Array::from(values.to_vec()))
}

fn nullable_ints(values: &[Option<i64>]) -> ArrayRef {
    Arc::new(Int64Array::from(values.to_vec()))
}

fn selected_value(chunk: &DataChunkView, column: usize, physical_row: usize) -> Option<i64> {
    let values = crate::pipeline::test_support::materialized_column::<Int64Array>(&chunk, column);
    values
        .is_valid(physical_row)
        .then(|| values.value(physical_row))
}

fn cells(chunk: &DataChunkView, column: usize) -> Vec<Option<i64>> {
    let array = crate::pipeline::test_support::materialized_column::<Int64Array>(chunk, column);
    let ints = array
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("int64 output");
    (0..ints.len())
        .map(|row| ints.is_valid(row).then(|| ints.value(row)))
        .collect()
}

fn selected_cells(chunk: &DataChunkView, column: usize) -> Vec<Option<i64>> {
    (0..chunk.num_rows())
        .map(|logical_row| selected_value(chunk, column, chunk.selection().get_index(logical_row)))
        .collect()
}

fn selected_rows(chunk: &DataChunkView) -> Vec<[Option<i64>; 4]> {
    (0..chunk.num_rows())
        .map(|position| {
            let physical = chunk.selection().get_index(position);
            std::array::from_fn(|column| selected_value(chunk, column, physical))
        })
        .collect()
}

fn residual_greater_than_filter() -> (TestExpressionExecutor, JoinFilterCollector) {
    let collector = JoinFilterCollector::default();
    let predicate = expression_binary(
        common::protocol::BinaryExprOp::Greater,
        expression_column(0, types::VeinDataType::Int64),
        expression_column(1, types::VeinDataType::Int64),
        types::VeinDataType::Boolean,
    );
    (expression_filter(predicate, collector.clone()), collector)
}

fn run_outer_join_with_residual(join_type: JoinType) -> Vec<[Option<i64>; 4]> {
    let mut table = HashTable::new(JoinKeys::new(vec![0]), JoinKeys::new(vec![0]));
    table
        .insert(crate::pipeline::test_support::dense_view(vec![
            ints(&[1, 1, 3]),
            nullable_ints(&[Some(5), Some(15), Some(7)]),
        ]))
        .expect("build rows");

    let sink = Collect::default();
    let (residual_transform, collector) = residual_greater_than_filter();
    let mut join = HashJoin::new(
        Arc::new(table),
        join_type,
        vec![
            OutputColumn::Probe(0),
            OutputColumn::Probe(1),
            OutputColumn::Build(0),
            OutputColumn::Build(1),
        ],
        vec![DataType::Int64, DataType::Int64],
        vec![DataType::Int64, DataType::Int64],
        Box::new(sink.clone()),
    )
    .with_residual_filter(
        vec![OutputColumn::Probe(1), OutputColumn::Build(1)],
        Box::new(residual_transform),
        collector,
    );

    join.push(crate::pipeline::test_support::dense_view(vec![
        ints(&[1, 1, 1, 2]),
        nullable_ints(&[Some(1), Some(10), None, Some(9)]),
    ]))
    .expect("probe rows");
    join.flush().expect("flush outer join");

    let chunks = sink.0.lock().expect("collect lock");
    let mut rows = chunks.iter().flat_map(selected_rows).collect::<Vec<_>>();
    rows.sort_unstable();
    rows
}

/// A built table over one chunk of `values`.
fn build(values: &[i64]) -> HashTable {
    let mut sink = BuildSink::new(JoinKeys::new(vec![0]), JoinKeys::new(vec![0]));
    sink.push(crate::pipeline::test_support::dense_view(vec![ints(
        values,
    )]))
    .expect("build");
    sink.finish()
}

#[derive(Clone, Default)]
struct ViewCollect(Arc<Mutex<Vec<DataChunkView>>>);

impl PipelineNode<DataChunkView> for ViewCollect {
    fn push(&mut self, view: DataChunkView) -> Result<()> {
        self.0.lock().expect("view collector lock").push(view);
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        Ok(())
    }
}

#[test]
fn output_timing_excludes_downstream_on_probe_flush_and_error() {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Duration;

    struct ObservingSink {
        fail: bool,
        elapsed_us: Arc<AtomicU64>,
    }

    impl PipelineNode<DataChunkView> for ObservingSink {
        fn push(&mut self, _view: DataChunkView) -> Result<()> {
            let started = Instant::now();
            // Make double-counting observable without asserting an upper
            // latency limit, which would depend on machine load.
            std::thread::sleep(Duration::from_millis(5));
            self.elapsed_us
                .store(started.elapsed().as_micros() as u64, Ordering::Relaxed);
            if self.fail {
                Err(VeinError::internal("test sink failure"))
            } else {
                Ok(())
            }
        }

        fn flush(&mut self) -> Result<()> {
            Ok(())
        }
    }

    for join_type in [JoinType::Inner, JoinType::Right] {
        for fail in [false, true] {
            let downstream_elapsed = Arc::new(AtomicU64::new(0));
            let mut join = HashJoin::new(
                Arc::new(build(&[7])),
                join_type,
                vec![OutputColumn::Probe(0), OutputColumn::Build(0)],
                vec![DataType::Int64],
                vec![DataType::Int64],
                Box::new(ObservingSink {
                    fail,
                    elapsed_us: Arc::clone(&downstream_elapsed),
                }),
            );
            let probe = crate::pipeline::test_support::dense_view(vec![ints(&[7])]);
            let started = Instant::now();
            let outcome = if join_type == JoinType::Right {
                join.flush()
            } else {
                join.push(probe)
            };
            let total_us = started.elapsed().as_micros() as u64;
            let sink_us = downstream_elapsed.load(Ordering::Relaxed);
            assert_eq!(outcome.is_err(), fail);
            assert!(sink_us > 0, "the output must reach the observing sink");
            assert!(
                join.downstream_push_us >= sink_us,
                "downstream time must include the sink even on errors"
            );
            assert!(join.view_compose_us.saturating_add(join.downstream_push_us) <= total_us);
            assert!(
                join.view_compose_us.saturating_add(sink_us) <= total_us,
                "{join_type:?}, fail={fail}: composition {} us plus downstream {sink_us} us exceeds total {total_us} us",
                join.view_compose_us
            );
        }
    }
}

#[test]
fn join_keeps_build_and_probe_payloads_as_views() {
    use common::arrow::SelectionVector;

    let build_payload = ints(&[70, 80, 90]);
    let mut build = BuildSink::new(JoinKeys::new(vec![0]), JoinKeys::new(vec![0]));
    build
        .push(crate::pipeline::test_support::selected_view(
            vec![ints(&[7, 8, 9]), Arc::clone(&build_payload)],
            SelectionVector::new_sparse(vec![2, 0]),
        ))
        .unwrap();
    build.flush().unwrap();
    let table = Arc::new(build.finish());
    let build_metrics = table.build_key_materialization();
    assert_eq!(build_metrics.0, MaterializationReport::default());
    let probe_payload = ints(&[700, 900, 800]);
    let keys = ints(&[7, 9, 8]);
    let sink = ViewCollect::default();
    let mut join = HashJoin::new(
        Arc::clone(&table),
        JoinType::Inner,
        vec![OutputColumn::Probe(1), OutputColumn::Build(1)],
        vec![DataType::Int64; 2],
        vec![DataType::Int64; 2],
        Box::new(sink.clone()),
    );
    for selection in [
        SelectionVector::new_dense(3),
        SelectionVector::new_sparse(vec![1, 0]),
    ] {
        PipelineNode::<DataChunkView>::push(
            &mut join,
            crate::pipeline::test_support::selected_view(
                vec![Arc::clone(&keys), Arc::clone(&probe_payload)],
                selection,
            ),
        )
        .unwrap();
    }
    join.flush().unwrap();
    assert_eq!(
        join.probe_key_materialization,
        MaterializationReport::default()
    );
    assert_eq!(table.build_key_materialization(), build_metrics);
    let views = sink.0.lock().unwrap();
    let mut pairs = Vec::new();
    for view in views.iter() {
        for row in 0..view.num_rows() {
            let (probe, probe_row) = view.resolve(0, row).unwrap().unwrap();
            let (build, build_row) = view.resolve(1, row).unwrap().unwrap();
            assert!(Arc::ptr_eq(probe, &probe_payload));
            assert!(Arc::ptr_eq(build, &build_payload));
            pairs.push((
                probe
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(probe_row),
                build
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(build_row),
            ));
        }
    }
    assert_eq!(pairs, vec![(700, 70), (900, 90), (900, 90), (700, 70)]);
}

#[test]
fn join_output_view_references_original_probe_and_build_payloads() {
    let build_key = ints(&[7, 8]);
    let build_payload = ints(&[70, 80]);
    let mut table = HashTable::new(JoinKeys::new(vec![0]), JoinKeys::new(vec![0]));
    table
        .insert(crate::pipeline::test_support::dense_view(vec![
            build_key,
            Arc::clone(&build_payload),
        ]))
        .expect("build table");

    let probe_key = ints(&[8]);
    let probe_payload = ints(&[800]);
    let sink = ViewCollect::default();
    let mut join = HashJoin::new(
        Arc::new(table),
        JoinType::Inner,
        vec![OutputColumn::Probe(1), OutputColumn::Build(1)],
        vec![DataType::Int64, DataType::Int64],
        vec![DataType::Int64, DataType::Int64],
        Box::new(sink.clone()),
    );
    PipelineNode::<DataChunkView>::push(
        &mut join,
        crate::pipeline::test_support::dense_view(vec![probe_key, Arc::clone(&probe_payload)]),
    )
    .expect("probe join");
    PipelineNode::<DataChunkView>::flush(&mut join).expect("flush join");

    let outputs = sink.0.lock().expect("view collector lock");
    assert_eq!(outputs.len(), 1);
    let (probe_array, probe_row) = outputs[0].resolve(0, 0).unwrap().unwrap();
    let (build_array, build_row) = outputs[0].resolve(1, 0).unwrap().unwrap();
    assert!(Arc::ptr_eq(probe_array, &probe_payload));
    assert!(Arc::ptr_eq(build_array, &build_payload));
    assert_eq!(
        probe_array
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(probe_row),
        800
    );
    assert_eq!(
        build_array
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(build_row),
        80
    );
}

#[test]
fn multiple_build_batches_preserve_duplicate_output_references() {
    let payloads = [ints(&[100, 101]), ints(&[200, 201])];
    let mut table = HashTable::new(JoinKeys::new(vec![0]), JoinKeys::new(vec![0]));
    for payload in &payloads {
        table
            .insert(crate::pipeline::test_support::dense_view(vec![
                ints(&[7, 7]),
                Arc::clone(payload),
            ]))
            .unwrap();
    }
    let probe_payload = ints(&[700]);
    let sink = ViewCollect::default();
    let mut join = HashJoin::new(
        Arc::new(table),
        JoinType::Inner,
        vec![
            OutputColumn::Probe(1),
            OutputColumn::Build(1),
            OutputColumn::Build(1),
        ],
        vec![DataType::Int64; 2],
        vec![DataType::Int64; 2],
        Box::new(sink.clone()),
    );
    PipelineNode::<DataChunkView>::push(
        &mut join,
        crate::pipeline::test_support::dense_view(vec![ints(&[7]), Arc::clone(&probe_payload)]),
    )
    .unwrap();
    join.flush().unwrap();
    let outputs = sink.0.lock().unwrap();
    assert_eq!(
        outputs.len(),
        2,
        "output is bounded by the larger input batch"
    );
    assert!(outputs.iter().all(|view| view.num_rows() <= 2));
    let mut values = Vec::new();
    for view in outputs.iter() {
        for row in 0..view.num_rows() {
            let (probe, _) = view.resolve(0, row).unwrap().unwrap();
            let (build, index) = view.resolve(1, row).unwrap().unwrap();
            let (duplicate, duplicate_index) = view.resolve(2, row).unwrap().unwrap();
            assert!(Arc::ptr_eq(probe, &probe_payload));
            assert!(payloads.iter().any(|payload| Arc::ptr_eq(build, payload)));
            assert!(Arc::ptr_eq(build, duplicate));
            assert_eq!(index, duplicate_index);
            values.push(
                build
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(index),
            );
        }
    }
    values.sort_unstable();
    assert_eq!(values, vec![100, 101, 200, 201]);
}

#[test]
fn zero_column_join_views_retain_match_cardinality() {
    let sink = ViewCollect::default();
    let mut join = HashJoin::new(
        Arc::new(build(&[7, 7])),
        JoinType::Inner,
        vec![],
        vec![DataType::Int64],
        vec![DataType::Int64],
        Box::new(sink.clone()),
    );
    PipelineNode::<DataChunkView>::push(
        &mut join,
        crate::pipeline::test_support::dense_view(vec![ints(&[7, 8, 7])]),
    )
    .unwrap();
    join.flush().unwrap();
    let outputs = sink.0.lock().unwrap();
    assert_eq!(
        outputs.iter().map(DataChunkView::num_rows).sum::<usize>(),
        4
    );
    for view in outputs.iter() {
        assert_eq!(view.num_columns(), 0);
        let (batch, report) = view
            .to_record_batch_with_report(Arc::new(arrow::datatypes::Schema::empty()))
            .unwrap();
        assert_eq!(batch.num_rows(), view.num_rows());
        assert_eq!(batch.num_columns(), 0);
        assert_eq!(report, MaterializationReport::default());
    }
}

#[test]
fn outer_join_views_preserve_payloads_with_empty_opposite_sides() {
    for join_type in [JoinType::Left, JoinType::Right, JoinType::Full] {
        for empty_build in [true, false] {
            let payload = ints(&[10, 20]);
            let mut table = HashTable::new(JoinKeys::new(vec![0]), JoinKeys::new(vec![0]));
            if !empty_build {
                table
                    .insert(crate::pipeline::test_support::dense_view(vec![Arc::clone(
                        &payload,
                    )]))
                    .unwrap();
            }
            let sink = ViewCollect::default();
            let mut join = HashJoin::new(
                Arc::new(table),
                join_type,
                vec![OutputColumn::Probe(0), OutputColumn::Build(0)],
                vec![DataType::Int64],
                vec![DataType::Int64],
                Box::new(sink.clone()),
            );
            let probe = if empty_build {
                Arc::clone(&payload)
            } else {
                ints(&[])
            };
            PipelineNode::<DataChunkView>::push(
                &mut join,
                crate::pipeline::test_support::dense_view(vec![probe]),
            )
            .unwrap();
            join.flush().unwrap();
            let outputs = sink.0.lock().unwrap();
            let emits = join_type == JoinType::Full
                || (empty_build && join_type == JoinType::Left)
                || (!empty_build && join_type == JoinType::Right);
            assert_eq!(
                outputs.iter().map(DataChunkView::num_rows).sum::<usize>(),
                if emits { 2 } else { 0 }
            );
            for view in outputs.iter() {
                for row in 0..view.num_rows() {
                    let present = if empty_build { 0 } else { 1 };
                    let absent = 1 - present;
                    let (source, index) = view.resolve(present, row).unwrap().unwrap();
                    assert!(Arc::ptr_eq(source, &payload));
                    assert_eq!(index, row);
                    assert!(view.resolve(absent, row).unwrap().is_none());
                    assert_eq!(view.column_data_type(absent).unwrap(), &DataType::Int64);
                }
            }
        }
    }
}

#[test]
fn empty_build_null_extension_requires_declared_output_column() {
    let sink = ViewCollect::default();
    let mut join = HashJoin::new(
        Arc::new(HashTable::new(
            JoinKeys::new(vec![0]),
            JoinKeys::new(vec![0]),
        )),
        JoinType::Left,
        vec![OutputColumn::Probe(0), OutputColumn::Build(0)],
        vec![DataType::Int64],
        vec![],
        Box::new(sink.clone()),
    );
    let error = PipelineNode::<DataChunkView>::push(
        &mut join,
        crate::pipeline::test_support::dense_view(vec![ints(&[10])]),
    )
    .expect_err("missing build type must reject the output mapping");
    assert!(
        error.to_string().contains("missing column 0 in input 1"),
        "{error}"
    );
    assert!(sink.0.lock().unwrap().is_empty());
}

/// The whole operator: absorb a build side, probe it, emit joined columns.
#[test]
fn inner_join_end_to_end() {
    let sink = Collect::default();
    let mut join = HashJoin::new(
        Arc::new(build(&[20, 30])),
        JoinType::Inner,
        vec![OutputColumn::Probe(0), OutputColumn::Build(0)],
        vec![DataType::Int64],
        vec![DataType::Int64],
        Box::new(sink.clone()),
    );

    join.push(crate::pipeline::test_support::dense_view(vec![ints(&[
        10, 20,
    ])]))
    .expect("probe");
    join.flush().expect("flush");

    let chunks = sink.0.lock().expect("collect lock");
    let all: Vec<Option<i64>> = chunks.iter().flat_map(|chunk| cells(chunk, 0)).collect();
    assert_eq!(all, vec![Some(20)], "only the matching probe row survives");
}

#[test]
fn cross_join_emits_cartesian_product_with_empty_key_vectors() {
    let mut table = HashTable::new(JoinKeys::new(Vec::new()), JoinKeys::new(Vec::new()));
    let build_values = ints(&[10, 20, 30]);
    table
        .insert(crate::pipeline::test_support::dense_view(vec![Arc::clone(
            &build_values,
        )]))
        .expect("insert build rows without join keys");

    let probe_values = ints(&[1, 2]);
    let sink = Collect::default();
    let mut join = HashJoin::new(
        Arc::new(table),
        JoinType::Cross,
        vec![OutputColumn::Probe(0), OutputColumn::Build(0)],
        vec![DataType::Int64],
        vec![DataType::Int64],
        Box::new(sink.clone()),
    );

    join.push(crate::pipeline::test_support::dense_view(vec![Arc::clone(
        &probe_values,
    )]))
    .expect("probe cross join rows");
    join.flush().expect("flush cross join");

    let chunks = sink.0.lock().expect("collect lock");
    let mut pairs = chunks
        .iter()
        .flat_map(|chunk| {
            selected_cells(chunk, 0)
                .into_iter()
                .zip(selected_cells(chunk, 1))
        })
        .collect::<Vec<_>>();
    pairs.sort_unstable();
    assert_eq!(
        pairs,
        vec![
            (Some(1), Some(10)),
            (Some(1), Some(20)),
            (Some(1), Some(30)),
            (Some(2), Some(10)),
            (Some(2), Some(20)),
            (Some(2), Some(30)),
        ]
    );
}

#[test]
fn left_join_residual_false_and_null_leave_probe_rows_unmatched() {
    assert_eq!(
        run_outer_join_with_residual(JoinType::Left),
        vec![
            [Some(1), None, None, None],
            [Some(1), Some(1), None, None],
            [Some(1), Some(10), Some(1), Some(5)],
            [Some(2), Some(9), None, None],
        ]
    );
}

#[test]
fn right_join_residual_false_and_null_leave_build_rows_unmatched() {
    assert_eq!(
        run_outer_join_with_residual(JoinType::Right),
        vec![
            [None, None, Some(1), Some(15)],
            [None, None, Some(3), Some(7)],
            [Some(1), Some(10), Some(1), Some(5)],
        ]
    );
}

#[test]
fn full_join_tracks_matches_only_after_residual_filtering() {
    assert_eq!(
        run_outer_join_with_residual(JoinType::Full),
        vec![
            [None, None, Some(1), Some(15)],
            [None, None, Some(3), Some(7)],
            [Some(1), None, None, None],
            [Some(1), Some(1), None, None],
            [Some(1), Some(10), Some(1), Some(5)],
            [Some(2), Some(9), None, None],
        ]
    );
}

#[test]
fn semi_join_emits_a_probe_row_once_despite_duplicate_build_matches() {
    let sink = Collect::default();
    let mut join = HashJoin::new(
        Arc::new(build(&[20, 20, 30])),
        JoinType::Semi,
        vec![OutputColumn::Probe(0)],
        vec![DataType::Int64],
        vec![DataType::Int64],
        Box::new(sink.clone()),
    );

    join.push(crate::pipeline::test_support::dense_view(vec![ints(&[
        20, 10, 30,
    ])]))
    .expect("probe");
    join.flush().expect("flush");

    let chunks = sink.0.lock().expect("collect lock");
    let rows: Vec<Option<i64>> = chunks
        .iter()
        .flat_map(|chunk| selected_cells(chunk, 0))
        .collect();
    assert_eq!(rows, vec![Some(20), Some(30)]);
    assert!(chunks.iter().all(|chunk| chunk.num_columns() == 1));
}

#[test]
fn semi_join_materializes_sparse_selected_rows_at_the_result_boundary() {
    use common::arrow::SelectionVector;

    let sink = Collect::default();
    let probe_values = ints(&[10, 20, 30, 40]);
    let probe = crate::pipeline::test_support::selected_view(
        vec![Arc::clone(&probe_values)],
        SelectionVector::new_sparse(vec![1, 3]),
    );
    let mut join = HashJoin::new(
        Arc::new(build(&[40, 40])),
        JoinType::Semi,
        vec![OutputColumn::Probe(0)],
        vec![DataType::Int64],
        vec![DataType::Int64],
        Box::new(sink.clone()),
    );

    join.push(probe).expect("probe");
    join.flush().expect("flush");

    let chunks = sink.0.lock().expect("collect lock");
    assert_eq!(chunks.len(), 1);
    assert_eq!(chunks[0].num_rows(), 1);
    assert_eq!(cells(&chunks[0], 0), vec![Some(40)]);
}

#[test]
fn anti_join_materializes_unmatched_selected_rows_at_the_result_boundary() {
    use common::arrow::SelectionVector;

    let sink = Collect::default();
    let probe_values = ints(&[10, 20, 30, 40]);
    let probe = crate::pipeline::test_support::selected_view(
        vec![Arc::clone(&probe_values)],
        SelectionVector::new_sparse(vec![1, 3]),
    );
    let mut join = HashJoin::new(
        Arc::new(build(&[40])),
        JoinType::Anti,
        vec![OutputColumn::Probe(0)],
        vec![DataType::Int64],
        vec![DataType::Int64],
        Box::new(sink.clone()),
    );

    join.push(probe).expect("probe");
    join.flush().expect("flush");

    let chunks = sink.0.lock().expect("collect lock");
    assert_eq!(chunks.len(), 1);
    assert_eq!(chunks[0].num_rows(), 1);
    assert_eq!(cells(&chunks[0], 0), vec![Some(20)]);
}

#[test]
fn semi_join_emits_no_chunk_when_no_probe_key_matches() {
    let sink = Collect::default();
    let mut join = HashJoin::new(
        Arc::new(build(&[20])),
        JoinType::Semi,
        vec![OutputColumn::Probe(0)],
        vec![DataType::Int64],
        vec![DataType::Int64],
        Box::new(sink.clone()),
    );

    join.push(crate::pipeline::test_support::dense_view(vec![ints(&[
        10, 30,
    ])]))
    .expect("probe");
    join.flush().expect("flush");

    assert!(sink.0.lock().expect("collect lock").is_empty());
}

#[test]
fn anti_join_keeps_nonmatching_and_null_probe_keys() {
    let sink = Collect::default();
    let mut join = HashJoin::new(
        Arc::new(build(&[20, 20])),
        JoinType::Anti,
        vec![OutputColumn::Probe(0)],
        vec![DataType::Int64],
        vec![DataType::Int64],
        Box::new(sink.clone()),
    );

    join.push(crate::pipeline::test_support::dense_view(vec![Arc::new(
        Int64Array::from(vec![Some(20), Some(10), None]),
    )]))
    .expect("probe");
    join.flush().expect("flush");

    let chunks = sink.0.lock().expect("collect lock");
    let rows: Vec<Option<i64>> = chunks
        .iter()
        .flat_map(|chunk| selected_cells(chunk, 0))
        .collect();
    assert_eq!(rows, vec![Some(10), None]);
    assert!(chunks.iter().all(|chunk| chunk.num_columns() == 1));
}

/// A right join owes its unmatched build rows, and emits them at flush with
/// a null probe side.
#[test]
fn right_join_emits_unmatched_build_rows_at_flush() {
    let sink = Collect::default();
    let mut join = HashJoin::new(
        Arc::new(build(&[20, 40])),
        JoinType::Right,
        vec![OutputColumn::Probe(0), OutputColumn::Build(0)],
        vec![DataType::Int64],
        vec![DataType::Int64],
        Box::new(sink.clone()),
    );

    join.push(crate::pipeline::test_support::dense_view(vec![ints(&[20])]))
        .expect("probe");
    join.flush().expect("flush");

    let chunks = sink.0.lock().expect("collect lock");
    let probe: Vec<Option<i64>> = chunks.iter().flat_map(|c| cells(c, 0)).collect();
    let build: Vec<Option<i64>> = chunks.iter().flat_map(|c| cells(c, 1)).collect();
    assert_eq!(
        probe,
        vec![Some(20), None],
        "the match, then a null probe row"
    );
    assert_eq!(build, vec![Some(20), Some(40)], "build 40 matched nothing");
}

#[test]
fn single_join_checks_cardinality_and_keeps_both_input_buffers() {
    let mut table = HashTable::new(JoinKeys::new(vec![0]), JoinKeys::new(vec![0]));
    let build_keys = ints(&[10, 20]);
    let build_values = ints(&[100, 200]);
    table
        .insert(crate::pipeline::test_support::dense_view(vec![
            build_keys,
            Arc::clone(&build_values),
        ]))
        .expect("insert single-match build rows");

    let probe_keys = ints(&[20, 10]);
    let probe_payload = ints(&[2_000, 1_000]);
    let sink = Collect::default();
    let mut join = HashJoin::new(
        Arc::new(table),
        JoinType::Single,
        vec![OutputColumn::Probe(1), OutputColumn::Build(1)],
        vec![DataType::Int64, DataType::Int64],
        vec![DataType::Int64, DataType::Int64],
        Box::new(sink.clone()),
    );

    join.push(crate::pipeline::test_support::dense_view(vec![
        probe_keys,
        Arc::clone(&probe_payload),
    ]))
    .expect("single match should not multiply probe rows");
    join.flush().expect("flush single join");

    let chunks = sink.0.lock().expect("collect lock");
    assert_eq!(chunks.len(), 1);
    assert_eq!(chunks[0].num_rows(), 2);
    let probe = crate::pipeline::test_support::materialized_column::<Int64Array>(&chunks[0], 0);
    let build = crate::pipeline::test_support::materialized_column::<Int64Array>(&chunks[0], 1);
    let mut pairs: Vec<(i64, i64)> = (0..chunks[0].num_rows())
        .map(|row| (probe.value(row), build.value(row)))
        .collect();
    pairs.sort_unstable();
    assert_eq!(pairs, vec![(1_000, 100), (2_000, 200)]);
}

#[test]
fn single_join_returns_null_for_an_unmatched_probe_row() {
    let sink = Collect::default();
    let mut join = HashJoin::new(
        Arc::new(build(&[20])),
        JoinType::Single,
        vec![OutputColumn::Probe(0), OutputColumn::Build(0)],
        vec![DataType::Int64],
        vec![DataType::Int64],
        Box::new(sink.clone()),
    );

    join.push(crate::pipeline::test_support::dense_view(vec![ints(&[10])]))
        .expect("unmatched probe should produce a null scalar");
    join.flush().expect("flush single join");

    let chunks = sink.0.lock().expect("collect lock");
    assert_eq!(chunks.len(), 1);
    assert!(chunks[0].is_null(1, 0).unwrap());
}

#[test]
fn single_join_errors_on_multiple_matches_across_build_chunks() {
    let mut table = HashTable::new(JoinKeys::new(vec![0]), JoinKeys::new(vec![0]));
    table
        .insert(crate::pipeline::test_support::dense_view(vec![ints(&[20])]))
        .expect("first build batch");
    table
        .insert(crate::pipeline::test_support::dense_view(vec![ints(&[20])]))
        .expect("second build batch");
    let sink = Collect::default();
    let mut join = HashJoin::new(
        Arc::new(table),
        JoinType::Single,
        vec![OutputColumn::Probe(0), OutputColumn::Build(0)],
        vec![DataType::Int64],
        vec![DataType::Int64],
        Box::new(sink),
    );

    let error = join
        .push(crate::pipeline::test_support::dense_view(vec![ints(&[20])]))
        .expect_err("two inner rows violate scalar cardinality");
    assert!(
        error
            .to_string()
            .contains("scalar subquery returned more than one row")
    );
}

#[test]
fn inner_join_preserves_duplicate_output_across_probe_batches() {
    let mut table = HashTable::new(JoinKeys::new(vec![0]), JoinKeys::new(vec![0]));
    table
        .insert(crate::pipeline::test_support::dense_view(vec![
            ints(&[7, 8]),
            ints(&[70, 80]),
        ]))
        .expect("first build batch");
    table
        .insert(crate::pipeline::test_support::dense_view(vec![
            ints(&[7, 9]),
            ints(&[71, 90]),
        ]))
        .expect("second build batch");

    let sink = Collect::default();
    let mut join = HashJoin::new(
        Arc::new(table),
        JoinType::Inner,
        vec![OutputColumn::Probe(1), OutputColumn::Build(1)],
        vec![DataType::Int64, DataType::Int64],
        vec![DataType::Int64, DataType::Int64],
        Box::new(sink.clone()),
    );
    join.push(crate::pipeline::test_support::dense_view(vec![
        ints(&[7]),
        ints(&[700]),
    ]))
    .expect("first probe batch");
    join.push(crate::pipeline::test_support::dense_view(vec![
        ints(&[9, 7]),
        ints(&[900, 701]),
    ]))
    .expect("second probe batch");
    join.flush().expect("flush join");

    let chunks = sink.0.lock().expect("collect lock");
    let probe = chunks
        .iter()
        .flat_map(|chunk| cells(chunk, 0))
        .collect::<Vec<_>>();
    let build = chunks
        .iter()
        .flat_map(|chunk| cells(chunk, 1))
        .collect::<Vec<_>>();
    let mut pairs = probe.into_iter().zip(build).collect::<Vec<_>>();
    pairs.sort_unstable();

    assert_eq!(
        pairs,
        vec![
            (Some(700), Some(70)),
            (Some(700), Some(71)),
            (Some(701), Some(70)),
            (Some(701), Some(71)),
            (Some(900), Some(90)),
        ],
        "reused build-chunk buckets retain duplicate matches and do not leak prior batches"
    );
}

#[test]
fn inner_join_worker_local_probe_states_combine_duplicate_matches() {
    let mut table = HashTable::new(JoinKeys::new(vec![0]), JoinKeys::new(vec![0]));
    table
        .insert(crate::pipeline::test_support::dense_view(vec![
            ints(&[7, 7, 8]),
            ints(&[70, 71, 80]),
        ]))
        .expect("build duplicate keys");

    let sink = Collect::default();
    let mut canonical = HashJoin::new(
        Arc::new(table),
        JoinType::Inner,
        vec![OutputColumn::Probe(1), OutputColumn::Build(1)],
        vec![DataType::Int64, DataType::Int64],
        vec![DataType::Int64, DataType::Int64],
        Box::new(sink.clone()),
    );
    let mut first = canonical
        .fork_local()
        .expect("fork join probe state")
        .expect("inner join supports local probe state");
    let mut second = canonical
        .fork_local()
        .expect("fork second join probe state")
        .expect("inner join supports local probe state");

    first
        .push(crate::pipeline::test_support::dense_view(vec![
            ints(&[7, 8]),
            ints(&[700, 800]),
        ]))
        .expect("probe first worker batch");
    second
        .push(crate::pipeline::test_support::dense_view(vec![
            ints(&[7]),
            ints(&[701]),
        ]))
        .expect("probe second worker batch");

    for (local, compose_us, downstream_us) in [(&mut first, 13, 17), (&mut second, 19, 23)] {
        let local = local
            .as_mut()
            .as_any_mut()
            .downcast_mut::<HashJoin>()
            .expect("worker-local hash join");
        local.view_compose_us = compose_us;
        local.downstream_push_us = downstream_us;
    }
    first
        .combine_local(&mut canonical)
        .expect("combine first worker state");
    second
        .combine_local(&mut canonical)
        .expect("combine second worker state");
    canonical.flush().expect("flush canonical join");

    let chunks = sink.0.lock().expect("collect lock");
    let probe = chunks
        .iter()
        .flat_map(|chunk| cells(chunk, 0))
        .collect::<Vec<_>>();
    let build = chunks
        .iter()
        .flat_map(|chunk| cells(chunk, 1))
        .collect::<Vec<_>>();
    let mut pairs = probe.into_iter().zip(build).collect::<Vec<_>>();
    pairs.sort_unstable();
    assert_eq!(
        pairs,
        vec![
            (Some(700), Some(70)),
            (Some(700), Some(71)),
            (Some(701), Some(70)),
            (Some(701), Some(71)),
            (Some(800), Some(80)),
        ],
        "combining probe-local state preserves each duplicate build match"
    );
    assert_eq!(canonical.probed_rows, 3);
    assert_eq!(canonical.matched_rows, 5);
    assert_eq!(canonical.view_compose_us, 32);
    assert_eq!(canonical.downstream_push_us, 40);
}

#[path = "tests/residual_projection.rs"]
mod residual_projection;

#[path = "tests/correlated_semi_anti.rs"]
mod correlated_semi_anti;

#[path = "tests/output_batching.rs"]
mod output_batching;
