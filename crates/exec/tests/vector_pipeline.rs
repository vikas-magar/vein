//! Differential test: a real Parquet scan through the engine's own pipeline.
//!
//! `09-testing-and-correctness.md` §4 requires every path to be compared against
//! a trusted reference. Here the reference is the existing Arrow scan: the same
//! Parquet file feeds both the Arrow chunk path and the `vector`-backed pipeline,
//! and the two must agree on the surviving rows.
//!
//! This is the first exercise of the new representation on real storage rather
//! than synthetic chunks, so it is where a conversion that loses or reorders
//! rows would show up.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use arrow::array::{ArrayRef, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use catalog::MetadataStore;
use common::Result;
use common::protocol::{FileFormat, MicroOp, MicroProgram};
use types::LogicalLiteral;
use exec::execution::{BitMask, Filter, Limit, Predicate, ProgramPredicate, Project, VectorScanSource};
use exec::operators::source::scan::{ScanFile, ScanSource};
use exec::pipeline::bridge;
use exec::pipeline::{DataChunk, PipelineNode, Source};
use exec::udf::FunctionRegistry;
use types::VeinSchema;

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

/// Collects the chunks the engine-representation pipeline produces.
#[derive(Clone, Default)]
struct CollectVector(Arc<Mutex<Vec<vector::DataChunk>>>);

impl CollectVector {
    /// Active `id` values across every received chunk, in order.
    fn ids(&self) -> Vec<i64> {
        let mut out = Vec::new();
        for chunk in self.0.lock().expect("lock").iter() {
            let view = chunk.view(0).expect("column 0");
            let values = view.resolve_i64().expect("i64 column");
            out.extend(view.rows().filter_map(|row| values.at(row)));
        }
        out
    }
}

impl PipelineNode<vector::DataChunk> for CollectVector {
    fn push(&mut self, chunk: vector::DataChunk) -> Result<()> {
        self.0.lock().expect("lock").push(chunk);
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        Ok(())
    }
}

/// Keeps rows whose `id` exceeds `threshold`.
struct GreaterThan(i64);

impl Predicate for GreaterThan {
    fn evaluate(&self, chunk: &vector::DataChunk) -> Result<BitMask> {
        let values = chunk
            .view(0)
            .and_then(|view| view.resolve_i64())
            .ok_or_else(|| error::VeinError::internal("column 0 is not readable as Int64"))?;
        let words = (chunk.row_count() as usize).div_ceil(64);
        let mut values_bits = vec![0u64; words];
        let mut valid = vec![0u64; words];
        for row in 0..chunk.row_count() {
            let Some(value) = values.at(row) else {
                continue;
            };
            let bit = 1u64 << (row % 64);
            valid[row as usize / 64] |= bit;
            if value > self.0 {
                values_bits[row as usize / 64] |= bit;
            }
        }
        Ok(BitMask {
            values: values_bits.into(),
            valid: valid.into(),
        })
    }
}

fn write_parquet(dir: &Path, name: &str, ids: &[i64]) -> std::io::Result<PathBuf> {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let column: ArrayRef = Arc::new(Int64Array::from(ids.to_vec()));
    let batch = RecordBatch::try_new(schema.clone(), vec![column])
        .map_err(|error| std::io::Error::other(error.to_string()))?;
    let path = dir.join(name);
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

/// Scans `path` through the existing Arrow source and returns the chunks.
async fn scan_arrow(dir: &Path, name: &str, path: &Path) -> Vec<DataChunk> {
    let builder = opendal::services::Fs::default().root(&dir.to_string_lossy());
    let operator = opendal::Operator::new(builder).expect("local fs operator");
    let files = vec![ScanFile {
        relative: name.to_string(),
        absolute: std::fs::canonicalize(path).expect("canonical path"),
    }];
    let mut source = ScanSource::new(
        operator,
        files,
        FileFormat::Parquet,
        Arc::new(VeinSchema::empty()),
        None,
        MetadataStore::new(),
        Arc::new(FunctionRegistry::new()),
    );
    let collect = CollectArrow::default();
    let mut sink = collect.clone();
    source.execute(&mut sink).await.expect("scan succeeds");
    collect.0.lock().expect("lock").clone()
}

/// A scan producing the engine's representation.
fn vector_scan(dir: &Path, name: &str, path: &Path) -> VectorScanSource {
    let builder = opendal::services::Fs::default().root(&dir.to_string_lossy());
    let operator = opendal::Operator::new(builder).expect("local fs operator");
    let files = vec![ScanFile {
        relative: name.to_string(),
        absolute: std::fs::canonicalize(path).expect("canonical path"),
    }];
    VectorScanSource::new(ScanSource::new(
        operator,
        files,
        FileFormat::Parquet,
        Arc::new(VeinSchema::empty()),
        None,
        MetadataStore::new(),
        Arc::new(FunctionRegistry::new()),
    ))
}

/// `column 0 > 15`, as the planner's bytecode compiler would emit it: column 0 is
/// mounted in register 0, the constant in R1 and the condition in R2.
fn greater_than_15_program() -> MicroProgram {
    MicroProgram {
        register_count: 3,
        instructions: vec![
            MicroOp::LoadConstant {
                dst: 1,
                value: LogicalLiteral::Int64(15),
            },
            MicroOp::CmpGt {
                dst: 2,
                src1: 0,
                src2: 1,
            },
        ],
    }
}

/// The whole new path, driven by its own scan source.
#[tokio::test]
async fn the_scan_source_drives_the_engine_pipeline() {
    let dir = tempfile::tempdir().expect("temp dir");
    let ids: Vec<i64> = (1..=20).collect();
    let path = write_parquet(dir.path(), "part-0.parquet", &ids).expect("write parquet");

    let mut source = vector_scan(dir.path(), "part-0.parquet", &path);
    let sink = CollectVector::default();
    let mut pipeline = Filter::new(GreaterThan(15), Box::new(sink.clone()));
    source.execute(&mut pipeline).await.expect("scan");

    assert_eq!(sink.ids(), (16..=20).collect::<Vec<_>>());
    assert!(
        source.converted_chunks() > 0,
        "the scan must report what it converted"
    );
    assert!(source.bytes_read() > 0, "the scan must report bytes read");
}

/// A **VIR program** — not a hand-written Rust predicate — filtering a real scan.
///
/// This is the point of VIR being the execution contract: the condition is an
/// instruction sequence the planner emits, and the interpreter here runs it
/// against the engine's own representation without a second compiler.
#[tokio::test]
async fn a_vir_program_filters_a_real_scan() {
    let dir = tempfile::tempdir().expect("temp dir");
    let ids: Vec<i64> = (1..=20).collect();
    let path = write_parquet(dir.path(), "part-0.parquet", &ids).expect("write parquet");

    let mut source = vector_scan(dir.path(), "part-0.parquet", &path);
    let sink = CollectVector::default();
    let predicate = ProgramPredicate::new(Arc::new(greater_than_15_program()), 2);
    let mut pipeline = Filter::new(predicate, Box::new(sink.clone()));
    source.execute(&mut pipeline).await.expect("scan");

    assert_eq!(sink.ids(), (16..=20).collect::<Vec<_>>());
}

#[tokio::test]
async fn a_parquet_scan_flows_through_the_engine_representation() {
    let dir = tempfile::tempdir().expect("temp dir");
    let ids: Vec<i64> = (1..=20).collect();
    let path = write_parquet(dir.path(), "part-0.parquet", &ids).expect("write parquet");

    let arrow_chunks = scan_arrow(dir.path(), "part-0.parquet", &path).await;
    let arrow_rows: u32 = arrow_chunks
        .iter()
        .map(DataChunk::num_rows)
        .sum::<usize>() as u32;
    assert_eq!(arrow_rows, 20, "the scan must see every row");

    // Feed the scanned chunks through the engine's own pipeline:
    // id > 15, project [id], offset 1, limit 2  ->  17, 18
    let sink = CollectVector::default();
    let limit = Limit::new(1, 2, Box::new(sink.clone()));
    let project = Project::new(vec![0], Box::new(limit));
    let mut pipeline = Filter::new(GreaterThan(15), Box::new(project));

    for chunk in &arrow_chunks {
        let converted =
            bridge::arrow_chunk_to_vector(chunk).expect("the scanned column is convertible");
        pipeline.push(converted).expect("push");
    }
    pipeline.flush().expect("flush");

    assert_eq!(sink.ids(), vec![17, 18]);
}

#[tokio::test]
async fn the_conversion_preserves_every_row_of_a_real_scan() {
    let dir = tempfile::tempdir().expect("temp dir");
    let ids: Vec<i64> = (1..=20).collect();
    let path = write_parquet(dir.path(), "part-0.parquet", &ids).expect("write parquet");

    let arrow_chunks = scan_arrow(dir.path(), "part-0.parquet", &path).await;
    let mut converted_ids = Vec::new();
    for chunk in &arrow_chunks {
        let converted = bridge::arrow_chunk_to_vector(chunk).expect("convertible");
        assert_eq!(converted.row_count(), chunk.physical_len() as u32);
        let view = converted.view(0).expect("column 0");
        let values = view.resolve_i64().expect("i64 column");
        converted_ids.extend(view.rows().filter_map(|row| values.at(row)));
    }
    assert_eq!(
        converted_ids, ids,
        "no row may be lost, duplicated or reordered by the conversion"
    );

    // The reverse direction agrees with the oracle too.
    for chunk in &arrow_chunks {
        let converted = bridge::arrow_chunk_to_vector(chunk).expect("convertible");
        let back = bridge::vector_chunk_to_arrow(&converted).expect("convertible back");
        let before = chunk
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("int64")
            .values()
            .to_vec();
        let after = back
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("int64")
            .values()
            .to_vec();
        assert_eq!(after, before);
    }
}
