//! The blocking sort measured in isolation: no Parquet, decode, or catalog.
//!
//! Run with `cargo run --profile perf -p exec --example sort_microbench`.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use arrow::array::{ArrayRef, Int64Array};
use common::protocol::{SortKey, SortPlan};
use exec::operators::SortTransform;
use exec::pipeline::{DataChunk, PipelineNode};

const ROWS: usize = 1_000_000;
const CHUNK: usize = 8192;

fn column(values: impl Iterator<Item=i64>) -> ArrayRef {
    Arc::new(Int64Array::from_iter_values(values))
}

fn chunks() -> Vec<DataChunk> {
    (0..ROWS)
        .step_by(CHUNK)
        .map(|start| {
            let end = (start + CHUNK).min(ROWS);
            let keys = column((start..end).map(|row| (ROWS - row) as i64));
            let payload = column((start..end).map(|row| row as i64));
            DataChunk::from_vectors(vec![keys, payload])
        })
        .collect()
}

struct Count(Arc<AtomicU64>);

impl PipelineNode<DataChunk> for Count {
    fn push(&mut self, chunk: DataChunk) -> common::Result<()> {
        self.0.fetch_add(chunk.num_rows() as u64, Ordering::Relaxed);
        Ok(())
    }

    fn flush(&mut self) -> common::Result<()> {
        Ok(())
    }
}

fn run(input: &[DataChunk], plan: SortPlan) -> (Duration, u64) {
    let rows_out = Arc::new(AtomicU64::new(0));
    let mut sort = SortTransform::new(plan, Box::new(Count(Arc::clone(&rows_out))));
    let started = Instant::now();
    for chunk in input {
        sort.push(chunk.clone()).expect("push input chunk");
    }
    sort.flush().expect("sort and gather");
    (started.elapsed(), rows_out.load(Ordering::Relaxed))
}

fn main() {
    let input = chunks();
    let plan = SortPlan {
        keys: vec![SortKey {
            column: 0,
            ascending: true,
            nulls_first: false,
        }],
    };

    for _ in 0..2 {
        run(&input, plan.clone());
    }

    let mut best = Duration::MAX;
    let mut output_rows = 0;
    for _ in 0..5 {
        let (elapsed, rows) = run(&input, plan.clone());
        best = best.min(elapsed);
        output_rows = rows;
    }

    println!("blocking sort + gather, {ROWS} rows in {CHUNK}-row chunks, best of 5, warm");
    println!(
        "{:>9.3}ms {:>8.2} ns/row; output rows={output_rows}",
        best.as_secs_f64() * 1e3,
        best.as_nanos() as f64 / ROWS as f64,
    );
}
