//! The hash join measured in isolation: no Parquet, no decode, no catalog.
//!
//! The end-to-end benchmark cannot resolve a join change below about a
//! millisecond — `probe_us` varied 2x between runs of identical code, because
//! it folds in row-group I/O, Parquet decode and machine noise. This measures
//! only the join: fixed in-memory chunks, best of N, no I/O. If a change to the
//! build, the probe or the gather is real, it shows up here.
//!
//! Run with `cargo run --profile perf -p exec --example join_microbench`.

use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::DataType;

use exec::operators::join::{BuildSink, HashJoin, JoinKeys, JoinType, OutputColumn};
use exec::pipeline::{DataChunk, PipelineNode};

/// Build rows, and probe rows. One match per probe row, as a keyed join on a
/// unique key gives.
const ROWS: usize = 1_000_000;
/// Rows per chunk — the scan's batch size, so the per-chunk costs are realistic.
const CHUNK: usize = 8192;

fn column(values: impl Iterator<Item = i64>) -> ArrayRef {
    Arc::new(Int64Array::from_iter_values(values))
}

/// `ROWS` rows in `CHUNK`-sized chunks, key and payload both `f(index)`.
fn chunks(rows: usize, key: impl Fn(usize) -> i64, payload: impl Fn(usize) -> i64) -> Vec<DataChunk> {
    (0..rows)
        .step_by(CHUNK)
        .map(|start| {
            let end = (start + CHUNK).min(rows);
            let keys = column((start..end).map(&key));
            let payloads = column((start..end).map(&payload));
            DataChunk::from_vectors(vec![keys, payloads])
        })
        .collect()
}

/// Counts the rows a pipeline produced.
#[derive(Default)]
struct Count(u64);

impl PipelineNode<DataChunk> for Count {
    fn push(&mut self, chunk: DataChunk) -> common::Result<()> {
        self.0 += chunk.num_rows() as u64;
        Ok(())
    }

    fn flush(&mut self) -> common::Result<()> {
        Ok(())
    }
}

/// One full join: build to completion, then probe.
///
/// Returns (build, probe+gather, rows out).
fn run(build: &[DataChunk], probe: &[DataChunk]) -> (Duration, Duration, u64) {
    let started = Instant::now();
    let mut sink = BuildSink::new(JoinKeys::new(vec![0]), JoinKeys::new(vec![0]));
    for chunk in build {
        sink.push(chunk.clone()).expect("build");
    }
    let table = sink.finish();
    let build_time = started.elapsed();

    let started = Instant::now();
    let collected = Count::default();
    let mut join = HashJoin::new(
        table,
        JoinType::Inner,
        vec![OutputColumn::Probe(0), OutputColumn::Build(1)],
        vec![DataType::Int64, DataType::Int64],
        vec![DataType::Int64, DataType::Int64],
        Box::new(collected),
    );
    for chunk in probe {
        join.push(chunk.clone()).expect("probe");
    }
    join.flush().expect("flush");
    (build_time, started.elapsed(), 0)
}

fn report(label: &str, elapsed: Duration, rows: usize) {
    println!(
        "{label:<18} {:>9.3}ms {:>8.2} ns/row",
        elapsed.as_secs_f64() * 1e3,
        elapsed.as_nanos() as f64 / rows as f64
    );
}

fn main() {
    let build = chunks(ROWS, |i| i as i64, |i| (i * 2) as i64);
    let probe = chunks(ROWS, |i| i as i64, |_| 0);

    // Warm up — the first pass pays for page faults on freshly allocated chunks.
    for _ in 0..2 {
        run(&build, &probe);
    }

    let (mut best_build, mut best_probe) = (Duration::MAX, Duration::MAX);
    for _ in 0..5 {
        let (build_time, probe_time, _) = run(&build, &probe);
        best_build = best_build.min(build_time);
        best_probe = best_probe.min(probe_time);
    }

    println!("hash join, {ROWS} rows in {CHUNK}-row chunks, best of 5, warm\n");
    report("build insert", best_build, ROWS);
    report("probe + gather", best_probe, ROWS);
    report("total", best_build + best_probe, ROWS);
}
