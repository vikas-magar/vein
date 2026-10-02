//! The default [`ExecutorBuilder`] implementation.
//!
//! Maps task descriptions onto concrete sources, sinks, and Vector VM execution.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use arrow::record_batch::RecordBatch;

use crate::operators::sink::{CollectSink, DiscardSink, WriteSink, WriteStorage};
use crate::operators::source::scan::ScanFile;
use crate::operators::source::ScanSource;
use crate::pipeline::{DataChunk, PipelineNode, Sink, Source, Transform};
use crate::runtime::ExecutorBuilder;
use crate::udf::FunctionRegistry;
use crate::vm::VectorVmExecutor;
use catalog::MetadataStore;
use common::Result;
use common::identifiers::TaskId;
use common::protocol::{DataSink, DataSource, MicroOp, MicroProgram};
use error::VeinError;
use opendal::Operator;
use types::LogicalLiteral;

/// Builds a [`TaskExecutor`](crate::runtime::TaskExecutor) with configurable
/// stages and Vector VM execution engine.
///
/// `new()` wires the default implementation ([`DefaultExecutorBuilder`] with a
/// default [`FunctionRegistry`]); the `with_*` methods override specific parts
/// for customization.
pub struct TaskExecutorBuilder {
    builder: Arc<dyn ExecutorBuilder>,
    registry: Arc<FunctionRegistry>,
    storage: Option<Arc<dyn WriteStorage>>,
    metadata_store: MetadataStore,
}

impl TaskExecutorBuilder {
    /// Build a task executor with the default stages and Vector VM engine.
    pub fn new() -> Self {
        let registry = Arc::new(FunctionRegistry::new());
        Self {
            builder: Arc::new(DefaultExecutorBuilder::new(registry.clone())),
            registry,
            storage: None,
            metadata_store: MetadataStore::new(),
        }
    }

    /// Share the catalog's parsed-metadata store with the executor.
    ///
    /// The catalog that resolves tables fills the store; handing it to the
    /// executor lets scans reuse file metadata the catalog already parsed
    /// instead of reading the same files again.
    pub fn with_metadata_store(mut self, metadata_store: MetadataStore) -> Self {
        self.metadata_store = metadata_store;
        self.rebuild_default();
        self
    }

    /// Override the UDF function registry used by the Vector VM.
    pub fn with_registry(mut self, registry: Arc<FunctionRegistry>) -> Self {
        self.registry = registry;
        self.rebuild_default();
        self
    }

    /// Inject the transactional destination backend used by `WRITE` sinks.
    pub fn with_write_storage(mut self, storage: Arc<dyn WriteStorage>) -> Self {
        self.storage = Some(storage);
        self.rebuild_default();
        self
    }

    fn rebuild_default(&mut self) {
        let mut builder = DefaultExecutorBuilder::new(self.registry.clone());
        if let Some(storage) = &self.storage {
            builder = builder.with_write_storage(storage.clone());
        }
        builder = builder.with_metadata_store(self.metadata_store.clone());
        self.builder = Arc::new(builder);
    }

    /// Override the executor builder (which maps the wire protocol to stages) entirely.
    pub fn with_executor_builder(mut self, builder: Arc<dyn ExecutorBuilder>) -> Self {
        self.builder = builder;
        self
    }

    /// Build the task executor.
    pub fn build(self) -> crate::runtime::DefaultTaskExecutor {
        crate::runtime::DefaultTaskExecutor::new(self.builder)
    }
}

impl Default for TaskExecutorBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// How many storage handles one executor keeps before it starts over.
///
/// A query resolves one root per stage, so this is far more than a single query
/// needs; the bound only stops a long-lived executor from growing the map
/// without limit.
const OPERATOR_CACHE_CAPACITY: usize = 64;

/// The default executor builder: builds concrete sources, sinks, and the
/// [`VectorVmExecutor`] from a wire task description.
pub struct DefaultExecutorBuilder {
    registry: Arc<FunctionRegistry>,
    storage: Option<Arc<dyn WriteStorage>>,
    metadata_store: MetadataStore,
    /// Storage handles, keyed by the root they were opened on.
    ///
    /// Opening a handle resolves the root and wires up the service behind it,
    /// which costs more than the first read it serves, and every task of a
    /// stage scans files under the same root. Keeping the handle here — rather
    /// than in the task, which is a serializable description crossing the wire —
    /// lets a stage's tasks share one.
    operators: Mutex<HashMap<PathBuf, Operator>>,
}

impl DefaultExecutorBuilder {
    /// Create a builder with the given UDF function registry for the Vector VM.
    pub fn new(registry: Arc<FunctionRegistry>) -> Self {
        Self {
            registry,
            storage: None,
            metadata_store: MetadataStore::new(),
            operators: Mutex::new(HashMap::new()),
        }
    }

    /// Attach a transactional table storage implementation.
    pub fn with_write_storage(mut self, storage: Arc<dyn WriteStorage>) -> Self {
        self.storage = Some(storage);
        self
    }

    /// Share a parsed-metadata store with scans built by this executor.
    pub fn with_metadata_store(mut self, metadata_store: MetadataStore) -> Self {
        self.metadata_store = metadata_store;
        self
    }

    /// Returns a storage handle rooted at `root`, opening one only the first time.
    ///
    /// The lock is deliberately held across the build: a stage's tasks ask for
    /// the same root at the same instant, and one build with the rest waiting
    /// costs less than a build per task racing to publish the same handle.
    fn local_operator(&self, root: &Path) -> Result<Operator> {
        let mut operators = self
            .operators
            .lock()
            .map_err(|_| VeinError::internal("operator cache mutex poisoned"))?;
        if let Some(operator) = operators.get(root) {
            return Ok(operator.clone());
        }
        if operators.len() >= OPERATOR_CACHE_CAPACITY {
            operators.clear();
        }
        use opendal::services::Fs;
        let builder = Fs::default().root(&root.to_string_lossy());
        let operator = Operator::new(builder)
            .map_err(|error| VeinError::internal(format!("build fs operator: {error}")))?;
        operators.insert(root.to_path_buf(), operator.clone());
        Ok(operator)
    }
}

#[async_trait::async_trait]
impl ExecutorBuilder for DefaultExecutorBuilder {
    fn build_source(
        &self,
        source: &DataSource,
        _task_id: TaskId,
        _attempt: u32,
    ) -> Result<Box<dyn Source<DataChunk>>> {
        match source {
            DataSource::Memory => Err(VeinError::internal(
                "a DataSource::Memory task carries no payload to materialize",
            )),
            DataSource::File(scan) => {
                let (root, relative) = local_scan_paths(&scan.files)?;
                let files = relative
                    .into_iter()
                    .map(|relative| ScanFile {
                        absolute: root.join(&relative),
                        relative,
                    })
                    .collect();
                let op = self.local_operator(&root)?;
                Ok(Box::new(ScanSource::new(
                    op,
                    files,
                    scan.format.clone(),
                    scan.projected.clone(),
                    scan.pushdown.clone(),
                    self.metadata_store.clone(),
                    self.registry.clone(),
                )))
            }
            DataSource::Stream(_) => Err(
                VeinError::internal("this local executor supports only memory and Parquet sources"),
            ),
        }
    }

    fn build_vm(
        &self,
        program: &Arc<MicroProgram>,
        next: Box<dyn PipelineNode<DataChunk>>,
    ) -> Result<Box<dyn Transform<DataChunk>>> {
        if engine_enabled() && engine_supports(program) {
            Ok(Box::new(EngineTransform::new(
                program.clone(),
                self.registry.clone(),
                next,
            )))
        } else {
            Ok(Box::new(VectorVmExecutor::new(
                program.clone(),
                self.registry.clone(),
                next,
            )))
        }
    }

    fn build_sink(&self, sink: &DataSink) -> Result<Box<dyn Sink<DataChunk>>> {
        match sink {
            DataSink::Discard => Ok(Box::new(DiscardSink::new())),
            DataSink::Client { schema } => {
                let field_names = schema
                    .fields
                    .iter()
                    .map(|column| column.name.clone())
                    .collect();
                Ok(Box::new(CollectSink::with_field_names(field_names)))
            }
            DataSink::Storage(target) => {
                let storage = self.storage.as_ref().ok_or_else(|| {
                    VeinError::internal("WRITE requires a configured table storage backend")
                })?;
                Ok(Box::new(WriteSink::new(target.clone(), storage.clone())?))
            }
        }
    }

    fn build_streaming_sink(
        &self,
        sink: &DataSink,
        output: flume::Sender<RecordBatch>,
    ) -> Result<Box<dyn Sink<DataChunk>>> {
        match sink {
            DataSink::Client { schema } => {
                let field_names = schema
                    .fields
                    .iter()
                    .map(|column| column.name.clone())
                    .collect();
                Ok(Box::new(CollectSink::with_field_names_and_sender(
                    field_names,
                    output,
                )))
            }
            _ => self.build_sink(sink),
        }
    }
}

/// Whether to run a task on the engine representation.
///
/// **Off by default.** The engine representation is faster per row, but as wired
/// it converts every batch `Arrow -> Vein -> Arrow`, and measured against
/// `VectorVmExecutor` on 500k rows that cost 1207µs versus 910µs — 33% slower.
/// The conversion, not the engine, is the problem: it disappears only when the
/// scan and sink speak the engine's representation natively. Until then the
/// Arrow VM runs by default and this switch keeps the new path measurable.
fn engine_enabled() -> bool {
    std::env::var_os("VEIN_ENGINE").is_some_and(|value| value != "0")
}

/// Whether the engine representation implements every instruction of `program`.
///
/// Checked before a chunk is converted, so a program the engine cannot run goes
/// straight to the Arrow VM instead of paying one `Arrow -> Vein -> Arrow`
/// round trip to discover the refusal.
///
/// The check is deliberately conservative in one direction: it must never claim
/// support the interpreter does not have, or a query would fail, while refusing
/// a program the interpreter could in fact run only costs the old path's speed.
/// It is necessarily incomplete — nothing here knows a column's type, so a
/// comparison on a non-integer column still reaches [`EngineTransform`] and is
/// caught by its runtime fallback.
fn engine_supports(program: &MicroProgram) -> bool {
    program.instructions.iter().all(|instruction| match instruction {
        // A literal is materialised through the engine's own constant column, so
        // only the types that column can carry count as supported.
        MicroOp::LoadConstant { value, .. } => {
            matches!(value, LogicalLiteral::Int64(_) | LogicalLiteral::String(_))
        }
        MicroOp::CopyRegister { .. }
        | MicroOp::CmpEq { .. }
        | MicroOp::CmpNeq { .. }
        | MicroOp::CmpLt { .. }
        | MicroOp::CmpLte { .. }
        | MicroOp::CmpGt { .. }
        | MicroOp::CmpGte { .. }
        | MicroOp::Add { .. }
        | MicroOp::Sub { .. }
        | MicroOp::Mul { .. }
        | MicroOp::Div { .. }
        | MicroOp::Mod { .. }
        | MicroOp::Neg { .. }
        | MicroOp::And { .. }
        | MicroOp::Or { .. }
        | MicroOp::Not { .. }
        | MicroOp::IsNull { .. }
        | MicroOp::IsNotNull { .. }
        | MicroOp::FilterMask { .. }
        | MicroOp::ProjectRegisters { .. } => true,
        _ => false,
    })
}

/// Runs a task on the engine's own representation, falling back to the Arrow VM
/// for anything it refuses.
///
/// This is the cutover's seam (`09-testing-and-correctness.md` §4). The engine
/// representation runs the programs it implements — converting once in and once
/// out, which ADR 0002 settles as the boundary's cost — and a program it cannot
/// run is handed to the existing [`VectorVmExecutor`], which still owns the full
/// instruction set.
///
/// Falling back is safe per chunk because `execute_chunk` builds a new chunk and
/// never mutates the one it was given, so the same chunk can be replayed through
/// the other VM. Falling back on *any* error is deliberate while the two engines
/// are compared: a query must not start failing because the new path is less
/// complete. It also means the new engine's own diagnosis is hidden behind the
/// old one's, which is the price of the migration and disappears with it.
struct EngineTransform {
    program: Arc<MicroProgram>,
    registry: Arc<FunctionRegistry>,
    /// The downstream node, until a fallback takes ownership of it.
    next: Option<Box<dyn PipelineNode<DataChunk>>>,
    /// The Arrow VM, present once a fallback has happened.
    fallback: Option<VectorVmExecutor>,
}

impl EngineTransform {
    fn new(
        program: Arc<MicroProgram>,
        registry: Arc<FunctionRegistry>,
        next: Box<dyn PipelineNode<DataChunk>>,
    ) -> Self {
        Self {
            program,
            registry,
            next: Some(next),
            fallback: None,
        }
    }

    /// Hands the downstream node to a fresh Arrow VM.
    fn fall_back(&mut self) {
        let next = self
            .next
            .take()
            .expect("the downstream node is present until a fallback happens");
        self.fallback = Some(VectorVmExecutor::new(
            self.program.clone(),
            self.registry.clone(),
            next,
        ));
    }
}

impl PipelineNode<DataChunk> for EngineTransform {
    fn push(&mut self, chunk: DataChunk) -> Result<()> {
        if let Some(fallback) = &mut self.fallback {
            return fallback.push(chunk);
        }
        let engine_chunk = crate::pipeline::bridge::arrow_chunk_to_vector(&chunk)?;
        match crate::execution::execute_chunk(&self.program, engine_chunk) {
            Ok(engine_chunk) => {
                let chunk = crate::pipeline::bridge::vector_chunk_to_arrow(&engine_chunk)?;
                self.next
                    .as_mut()
                    .expect("the downstream node is present until a fallback happens")
                    .push(chunk)
            }
            Err(_) => {
                self.fall_back();
                self.fallback
                    .as_mut()
                    .expect("a fallback was just built")
                    .push(chunk)
            }
        }
    }

    fn flush(&mut self) -> Result<()> {
        match (&mut self.fallback, &mut self.next) {
            (Some(fallback), _) => fallback.flush(),
            (None, Some(next)) => next.flush(),
            (None, None) => Ok(()),
        }
    }
}

impl Transform<DataChunk> for EngineTransform {}

fn local_scan_paths(files: &[String]) -> Result<(PathBuf, Vec<String>)> {
    let paths = files
        .iter()
        .map(|file| {
            std::fs::canonicalize(file)
                .map_err(|error| VeinError::io(format!("resolve local scan file '{file}'"), error))
        })
        .collect::<Result<Vec<_>>>()?;
    let first = paths
        .first()
        .ok_or_else(|| VeinError::internal("file scan contains no files"))?;
    let root = first
        .ancestors()
        .skip(1)
        .find(|candidate| paths.iter().all(|path| path.starts_with(candidate)))
        .ok_or_else(|| VeinError::internal("local scan files have no common directory"))?
        .to_path_buf();
    let relative = paths
        .iter()
        .map(|path| {
            path.strip_prefix(&root)
                .map(|path| path.to_string_lossy().into_owned())
                .map_err(|error| {
                    VeinError::internal(format!("local scan path is outside its root: {error}"))
                })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok((root, relative))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, Int64Array};
    use types::VeinDataType;

    /// Collects the chunks that reach the end of the chain.
    #[derive(Clone, Default)]
    struct Collect(Arc<Mutex<Vec<DataChunk>>>);

    impl PipelineNode<DataChunk> for Collect {
        fn push(&mut self, chunk: DataChunk) -> Result<()> {
            self.0.lock().expect("collect lock").push(chunk);
            Ok(())
        }

        fn flush(&mut self) -> Result<()> {
            Ok(())
        }
    }

    fn arrow_chunk(values: Vec<i64>) -> DataChunk {
        let column: ArrayRef = Arc::new(Int64Array::from(values));
        DataChunk::from_vectors(vec![column])
    }

    fn build(program: MicroProgram) -> (Box<dyn Transform<DataChunk>>, Collect) {
        let collect = Collect::default();
        let builder = DefaultExecutorBuilder::new(Arc::new(FunctionRegistry::new()));
        let vm = builder
            .build_vm(&Arc::new(program), Box::new(collect.clone()))
            .expect("built");
        (vm, collect)
    }

    /// A program the engine implements runs on its own representation:
    /// `id > 2 |> SELECT id`.
    #[test]
    fn the_engine_runs_a_supported_program() {
        let program = MicroProgram {
            register_count: 3,
            instructions: vec![
                MicroOp::LoadConstant {
                    dst: 1,
                    value: LogicalLiteral::Int64(2),
                },
                MicroOp::CmpGt {
                    dst: 2,
                    src1: 0,
                    src2: 1,
                },
                MicroOp::FilterMask { bool_reg: 2 },
                MicroOp::ProjectRegisters { out_regs: vec![0] },
            ],
        };
        let (mut vm, collect) = build(program);
        vm.push(arrow_chunk(vec![1, 2, 3, 4])).expect("push");
        vm.flush().expect("flush");

        let chunks = collect.0.lock().expect("collect lock");
        assert_eq!(chunks.len(), 1);
        let values = chunks[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("int64 output");
        let selection = &chunks[0].selection;
        assert_eq!(selection.size(), 2);
        assert_eq!(values.value(selection.get_index(0)), 3);
        assert_eq!(values.value(selection.get_index(1)), 4);
    }

    /// A program the engine refuses still runs, through the Arrow VM.
    #[test]
    fn an_unsupported_program_falls_back_to_the_arrow_vm() {
        let program = MicroProgram {
            register_count: 2,
            instructions: vec![
                MicroOp::Cast {
                    dst: 1,
                    src: 0,
                    target: VeinDataType::Float64,
                },
                MicroOp::ProjectRegisters { out_regs: vec![1] },
            ],
        };
        let (mut vm, collect) = build(program);
        vm.push(arrow_chunk(vec![1, 2, 3])).expect("push");
        vm.flush().expect("flush");

        let chunks = collect.0.lock().expect("collect lock");
        assert_eq!(chunks.len(), 1);
        assert!(
            chunks[0]
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Float64Array>()
                .is_some(),
            "the Arrow VM's Cast must produce a Float64 column"
        );
    }

    /// The static check never claims more than the interpreter has.
    #[test]
    fn the_support_check_matches_the_engine() {
        let supported = MicroProgram {
            register_count: 3,
            instructions: vec![
                MicroOp::LoadConstant {
                    dst: 1,
                    value: LogicalLiteral::Int64(2),
                },
                MicroOp::CmpGt {
                    dst: 2,
                    src1: 0,
                    src2: 1,
                },
                MicroOp::FilterMask { bool_reg: 2 },
                MicroOp::ProjectRegisters { out_regs: vec![0] },
            ],
        };
        assert!(engine_supports(&supported));

        let cast = MicroProgram {
            register_count: 2,
            instructions: vec![MicroOp::Cast {
                dst: 1,
                src: 0,
                target: VeinDataType::Float64,
            }],
        };
        assert!(!engine_supports(&cast), "Cast is not implemented");

        let float_literal = MicroProgram {
            register_count: 2,
            instructions: vec![MicroOp::LoadConstant {
                dst: 1,
                value: LogicalLiteral::Float64(1.0),
            }],
        };
        assert!(
            !engine_supports(&float_literal),
            "a Float64 literal cannot be projected as a column yet"
        );
    }
}
