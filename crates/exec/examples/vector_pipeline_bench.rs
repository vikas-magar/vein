//! Experiment: the engine-representation pipeline against the Arrow path.
//!
//! `01-final-architecture.md` §11 ranks the ways to spend effort, and
//! `08-performance-engineering.md` §1 requires measured numbers before claiming
//! any of them. This measures, on one real Parquet scan:
//!
//! * the Arrow path's filter (`SelectionVector::filter` over the scanned arrays),
//! * the engine representation's filter, driven by a VIR program,
//! * the conversion between them, on its own.
//!
//! Run with `cargo run -p exec --example vector_pipeline_bench`. The dev profile
//! is `opt-level = 1`, so absolute numbers are pessimistic and the per-row gap
//! to Arrow is overstated — `profile.dev` cannot inline the engine's cross-crate
//! accessors. `--profile perf` (which inherits `release` with thin LTO) is the
//! build `08-performance-engineering.md` and Rule 41 ask for; every stage is
//! compiled identically either way, so a stage-by-stage comparison holds in
//! both.

use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use arrow::array::{ArrayRef, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use catalog::MetadataStore;
use common::Result;
use common::arrow::SelectionVector;
use common::protocol::{FileFormat, MicroOp, MicroProgram};
use exec::execution::{Filter, ProgramPredicate, execute_chunk};
use exec::operators::source::scan::{ScanFile, ScanSource};
use exec::pipeline::{DataChunk, PipelineNode, Source, bridge};
use exec::udf::FunctionRegistry;
use exec::vm::VectorVmExecutor;
use types::LogicalLiteral;
use types::VeinSchema;
use vector::DataChunk as VectorChunk;

const ROWS: i64 = 500_000;
const THRESHOLD: i64 = 100_000;

/// Collects the Arrow chunks a scan produces.
#[derive(Clone, Default)]
struct CollectArrow(Arc<Mutex<Vec<DataChunk>>>);

impl PipelineNode<DataChunk> for CollectArrow {
    fn push(&mut self, chunk: DataChunk) -> Result<()> {
        self.0.lock().expect("lock").push(chunk);
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        Ok(())
    }
}

/// Collects the engine-representation chunks a pipeline produces.
#[derive(Clone, Default)]
struct CollectVector(Arc<Mutex<u64>>);

impl PipelineNode<VectorChunk> for CollectVector {
    fn push(&mut self, chunk: VectorChunk) -> Result<()> {
        *self.0.lock().expect("lock") += chunk.active_rows() as u64;
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        Ok(())
    }
}

fn write_parquet(dir: &Path, rows: i64) -> std::io::Result<std::path::PathBuf> {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let column: ArrayRef = Arc::new(Int64Array::from((0..rows).collect::<Vec<_>>()));
    let batch = RecordBatch::try_new(schema.clone(), vec![column])
        .map_err(|error| std::io::Error::other(error.to_string()))?;
    let path = dir.join("part-0.parquet");
    let file = std::fs::File::create(&path)?;
    let mut writer = parquet::arrow::ArrowWriter::try_new(file, schema, None)
        .map_err(|error| std::io::Error::other(error.to_string()))?;
    writer
        .write(&batch)
        .map_err(|error| std::io::Error::other(error.to_string()))?;
    writer
        .close()
        .map_err(|error| std::io::Error::other(error.to_string()))?;
    Ok(path)
}

fn scan_source(dir: &Path, path: &Path) -> ScanSource {
    let builder = opendal::services::Fs::default().root(&dir.to_string_lossy());
    let operator = opendal::Operator::new(builder).expect("local fs operator");
    let files = vec![ScanFile {
        relative: "part-0.parquet".to_string(),
        absolute: std::fs::canonicalize(path).expect("canonical path"),
    }];
    ScanSource::new(
        operator,
        files,
        FileFormat::Parquet,
        Arc::new(VeinSchema::empty()),
        None,
        MetadataStore::new(),
        Arc::new(FunctionRegistry::new()),
    )
}

fn program() -> MicroProgram {
    MicroProgram {
        register_count: 3,
        instructions: vec![
            MicroOp::LoadConstant {
                dst: 1,
                value: LogicalLiteral::Int64(THRESHOLD),
            },
            MicroOp::CmpGt {
                dst: 2,
                src1: 0,
                src2: 1,
            },
        ],
    }
}

/// The program shape the planner emits for `WHERE id > t |> SELECT id`, which is
/// what the executor's engine path actually runs end to end.
fn wired_program() -> MicroProgram {
    MicroProgram {
        register_count: 3,
        instructions: vec![
            MicroOp::LoadConstant {
                dst: 1,
                value: LogicalLiteral::Int64(THRESHOLD),
            },
            MicroOp::CmpGt {
                dst: 2,
                src1: 0,
                src2: 1,
            },
            MicroOp::FilterMask { bool_reg: 2 },
            MicroOp::ProjectRegisters { out_regs: vec![0] },
        ],
    }
}

/// Best-of-N wall time for `work`.
fn best_of<F: FnMut() -> u64>(iterations: usize, mut work: F) -> (Duration, u64) {
    let mut produced = 0;
    for _ in 0..2 {
        produced = work();
    }
    let mut best = Duration::MAX;
    for _ in 0..iterations {
        let started = Instant::now();
        produced = work();
        best = best.min(started.elapsed());
    }
    (best, produced)
}

fn report(label: &str, elapsed: Duration, rows: u64) {
    let per_row = elapsed.as_nanos() as f64 / rows.max(1) as f64;
    println!(
        "{label:<34} {elapsed:>10.3?}  {per_row:>7.2} ns/row  {:>10.1} M rows/s",
        rows as f64 / elapsed.as_secs_f64().max(f64::MIN_POSITIVE) / 1e6
    );
}

#[tokio::main]
async fn main() -> Result<()> {
    let dir = tempfile::tempdir().expect("temp dir");
    let path = write_parquet(dir.path(), ROWS).expect("write parquet");

    // --- scan, once, so the two filters are compared on the same chunks -----
    let started = Instant::now();
    let collect = CollectArrow::default();
    let mut sink = collect.clone();
    let mut source = scan_source(dir.path(), &path);
    source.execute(&mut sink).await.expect("scan");
    let chunks = collect.0.lock().expect("lock").clone();
    let scanned: u64 = chunks.iter().map(|chunk| chunk.num_rows() as u64).sum();
    println!("rows {scanned}, chunks {}\n", chunks.len());
    report("scan (Arrow, from storage)", started.elapsed(), scanned);

    // --- Arrow filter ------------------------------------------------------
    let (elapsed, kept) = best_of(20, || {
        let mut kept = 0u64;
        for chunk in &chunks {
            let values = chunk
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("int64");
            let selection: SelectionVector =
                chunk.selection.filter(|row| values.value(row) > THRESHOLD);
            kept += selection.size() as u64;
        }
        kept
    });
    report("filter (Arrow path)", elapsed, scanned);

    // --- conversion alone --------------------------------------------------
    let (elapsed, _) = best_of(20, || {
        let mut rows = 0u64;
        for chunk in &chunks {
            let converted = bridge::arrow_chunk_to_vector(chunk).expect("convertible");
            rows += converted.row_count() as u64;
        }
        rows
    });
    report("arrow -> vector conversion", elapsed, scanned);

    // --- engine representation, VIR-driven ---------------------------------
    let (elapsed, kept_vector) = best_of(20, || {
        let sink = CollectVector::default();
        let predicate = ProgramPredicate::new(Arc::new(program()), 2);
        let mut pipeline = Filter::new(predicate, Box::new(sink.clone()));
        for chunk in &chunks {
            let converted = bridge::arrow_chunk_to_vector(chunk).expect("convertible");
            pipeline.push(converted).expect("push");
        }
        pipeline.flush().expect("flush");
        *sink.0.lock().expect("lock")
    });
    report("convert + VIR filter", elapsed, scanned);

    // --- the wired path: what the executor runs for this program ------------
    // Convert in, filter, project, convert out — the whole cost the cutover
    // added to a task, measured against the Arrow path above.
    let wired = wired_program();
    let (elapsed, wired_rows) = best_of(20, || {
        let mut rows = 0u64;
        for chunk in &chunks {
            let converted = bridge::arrow_chunk_to_vector(chunk).expect("convertible");
            let filtered = execute_chunk(&wired, converted).expect("executed");
            rows += filtered.active_rows() as u64;
            bridge::vector_chunk_to_arrow(&filtered).expect("convertible back");
        }
        rows
    });
    report("wired (convert+engine+convert)", elapsed, scanned);

    // --- the Arrow VM: what the executor ran before the cutover -------------
    // Same chunks, same program, same operator. The comparison that decides
    // whether the cutover pays for its two boundary conversions.
    let arrow_vm_program = Arc::new(wired_program());
    let (elapsed, vm_rows) = best_of(20, || {
        let sink = CollectArrow::default();
        let mut vm = VectorVmExecutor::new(
            Arc::clone(&arrow_vm_program),
            Arc::new(FunctionRegistry::new()),
            Box::new(sink.clone()),
        );
        for chunk in &chunks {
            vm.push(chunk.clone()).expect("push");
        }
        vm.flush().expect("flush");
        sink.0
            .lock()
            .expect("lock")
            .iter()
            .map(|chunk| chunk.selection.size() as u64)
            .sum()
    });
    report("arrow VM (pre-cutover path)", elapsed, scanned);

    println!("\nrows kept: Arrow {kept}, engine representation {kept_vector}");
    assert_eq!(
        kept, kept_vector,
        "the two paths must agree before their timings mean anything"
    );
    assert_eq!(
        kept, wired_rows,
        "the wired path must keep the same rows as the others"
    );
    assert_eq!(
        kept, vm_rows,
        "the Arrow VM must keep the same rows before it is compared"
    );
    Ok(())
}
