//! The core Vector VM execution engine.
//!
//! Provides [`VectorVmExecutor`], which interprets a [`MicroProgram`] against
//! incoming in-flight [`DataChunk`]s in a tight, cache-coherent instruction loop.

use super::ops;
use super::register::RegisterFile;
use crate::pipeline::{DataChunk, PipelineNode, Transform};
use crate::udf::FunctionRegistry;
use arrow::array::ArrayRef;
use common::Result;
use common::protocol::{MicroOp, MicroProgram, RegId};
use std::sync::Arc;
use std::time::Instant;

/// Aggregated execution statistics for one VM pipeline run.
#[derive(Debug, Default)]
struct VmStats {
    /// Chunks evaluated by the VM.
    chunks: u64,
    /// Rows entering the VM.
    rows_in: u64,
    /// Rows leaving the VM after filtering.
    rows_out: u64,
    /// Time spent evaluating instructions.
    eval_us: u64,
}

/// Vector VM Executor node in the physical execution pipeline.
///
/// Holds the pre-allocated virtual register file and compiles/interprets the
/// [`MicroProgram`] instructions on incoming batches without steady-state allocations.
pub struct VectorVmExecutor {
    program: Arc<MicroProgram>,
    registers: RegisterFile,
    registry: Arc<FunctionRegistry>,
    next: Box<dyn PipelineNode<DataChunk>>,
    stats: VmStats,
}

impl VectorVmExecutor {
    /// Constructs a new [`VectorVmExecutor`].
    pub fn new(
        program: Arc<MicroProgram>,
        registry: Arc<FunctionRegistry>,
        next: Box<dyn PipelineNode<DataChunk>>,
    ) -> Self {
        let register_count = program.register_count;
        let registers = RegisterFile::new(register_count);
        Self {
            program,
            registers,
            registry,
            next,
            stats: VmStats::default(),
        }
    }

    /// Borrows the active [`MicroProgram`].
    pub fn program(&self) -> &MicroProgram {
        &self.program
    }

    /// Borrows the function registry.
    pub fn registry(&self) -> &Arc<FunctionRegistry> {
        &self.registry
    }

    /// Borrows the [`RegisterFile`] holding the most recently executed chunk's registers.
    pub fn registers(&self) -> &RegisterFile {
        &self.registers
    }
}

/// Stores an instruction result, keeping a single-element result as a broadcast
/// scalar whenever the chunk has more than one row.
///
/// A length-1 result can only come from scalar-only operands, so this is what
/// lets a scalar stay a scalar for the whole length of an expression tree.
fn store_result(registers: &mut RegisterFile, dst: RegId, array: ArrayRef) -> Result<()> {
    if array.len() == 1 && registers.chunk_len() > 1 {
        registers.set_scalar(dst, array)
    } else {
        registers.set(dst, array)
    }
}

impl VectorVmExecutor {
    /// Evaluates `chunk` through the program, leaving results in the registers.
    fn eval_chunk(&mut self, chunk: &mut DataChunk) -> Result<()> {
        self.registers.mount_chunk(chunk)?;
        for op in &self.program.instructions {
            match op {
                MicroOp::LoadConstant { dst, value } => {
                    let scalar = ops::scalar_constant_to_scalar(value)?;
                    store_result(&mut self.registers, *dst, scalar)?;
                }
                MicroOp::CopyRegister { dst, src } => {
                    let value = self.registers.get(*src)?.clone();
                    self.registers.set_value(*dst, value)?;
                }
                MicroOp::Cast { dst, src, target } => {
                    let (is_scalar, casted) = {
                        let source = self.registers.get(*src)?;
                        (source.is_scalar(), ops::eval_cast(source.array(), target)?)
                    };
                    if is_scalar {
                        self.registers.set_scalar(*dst, casted)?;
                    } else {
                        store_result(&mut self.registers, *dst, casted)?;
                    }
                }
                MicroOp::Add { dst, src1, src2 } => {
                    let res =
                        ops::eval_add(self.registers.get(*src1)?, self.registers.get(*src2)?)?;
                    store_result(&mut self.registers, *dst, res)?;
                }
                MicroOp::Sub { dst, src1, src2 } => {
                    let res =
                        ops::eval_sub(self.registers.get(*src1)?, self.registers.get(*src2)?)?;
                    store_result(&mut self.registers, *dst, res)?;
                }
                MicroOp::Mul { dst, src1, src2 } => {
                    let res =
                        ops::eval_mul(self.registers.get(*src1)?, self.registers.get(*src2)?)?;
                    store_result(&mut self.registers, *dst, res)?;
                }
                MicroOp::Div { dst, src1, src2 } => {
                    let res =
                        ops::eval_div(self.registers.get(*src1)?, self.registers.get(*src2)?)?;
                    store_result(&mut self.registers, *dst, res)?;
                }
                MicroOp::Mod { dst, src1, src2 } => {
                    let res =
                        ops::eval_mod(self.registers.get(*src1)?, self.registers.get(*src2)?)?;
                    store_result(&mut self.registers, *dst, res)?;
                }
                MicroOp::Neg { dst, src } => {
                    let res = ops::eval_neg(self.registers.get(*src)?.array())?;
                    store_result(&mut self.registers, *dst, res)?;
                }
                MicroOp::CmpEq { dst, src1, src2 } => {
                    let res =
                        ops::eval_cmp_eq(self.registers.get(*src1)?, self.registers.get(*src2)?)?;
                    store_result(&mut self.registers, *dst, res)?;
                }
                MicroOp::CmpNeq { dst, src1, src2 } => {
                    let res =
                        ops::eval_cmp_neq(self.registers.get(*src1)?, self.registers.get(*src2)?)?;
                    store_result(&mut self.registers, *dst, res)?;
                }
                MicroOp::CmpGt { dst, src1, src2 } => {
                    let res =
                        ops::eval_cmp_gt(self.registers.get(*src1)?, self.registers.get(*src2)?)?;
                    store_result(&mut self.registers, *dst, res)?;
                }
                MicroOp::CmpGte { dst, src1, src2 } => {
                    let res =
                        ops::eval_cmp_gte(self.registers.get(*src1)?, self.registers.get(*src2)?)?;
                    store_result(&mut self.registers, *dst, res)?;
                }
                MicroOp::CmpLt { dst, src1, src2 } => {
                    let res =
                        ops::eval_cmp_lt(self.registers.get(*src1)?, self.registers.get(*src2)?)?;
                    store_result(&mut self.registers, *dst, res)?;
                }
                MicroOp::CmpLte { dst, src1, src2 } => {
                    let res =
                        ops::eval_cmp_lte(self.registers.get(*src1)?, self.registers.get(*src2)?)?;
                    store_result(&mut self.registers, *dst, res)?;
                }
                MicroOp::And { dst, src1, src2 } => {
                    let lhs = self.registers.to_array(*src1)?;
                    let rhs = self.registers.to_array(*src2)?;
                    let res = ops::eval_and(&lhs, &rhs)?;
                    store_result(&mut self.registers, *dst, res)?;
                }
                MicroOp::Or { dst, src1, src2 } => {
                    let lhs = self.registers.to_array(*src1)?;
                    let rhs = self.registers.to_array(*src2)?;
                    let res = ops::eval_or(&lhs, &rhs)?;
                    store_result(&mut self.registers, *dst, res)?;
                }
                MicroOp::Not { dst, src } => {
                    let value = self.registers.to_array(*src)?;
                    let res = ops::eval_not(&value)?;
                    store_result(&mut self.registers, *dst, res)?;
                }
                MicroOp::IsNull { dst, src } => {
                    let value = self.registers.to_array(*src)?;
                    let res = ops::eval_is_null(&value)?;
                    store_result(&mut self.registers, *dst, res)?;
                }
                MicroOp::IsNotNull { dst, src } => {
                    let value = self.registers.to_array(*src)?;
                    let res = ops::eval_is_not_null(&value)?;
                    store_result(&mut self.registers, *dst, res)?;
                }
                MicroOp::FilterMask { bool_reg } => {
                    let mask = self.registers.to_array(*bool_reg)?;
                    chunk.selection = ops::apply_filter_mask(&chunk.selection, &mask)?;
                    if chunk.selection.is_empty() {
                        return Ok(());
                    }
                }
                MicroOp::ProjectRegisters { out_regs } => {
                    chunk.vectors = self.registers.project(out_regs)?;
                }
                MicroOp::CallUdf {
                    name,
                    arg_regs,
                    dst,
                } => {
                    let mut args = Vec::with_capacity(arg_regs.len());
                    for &reg in arg_regs {
                        args.push(self.registers.to_array(reg)?);
                    }
                    let res = ops::eval_call_udf(&self.registry, name, &args)?;
                    store_result(&mut self.registers, *dst, res)?;
                }
                MicroOp::VectorCosineDistance { dst, src1, src2 } => {
                    let lhs = self.registers.to_array(*src1)?;
                    let rhs = self.registers.to_array(*src2)?;
                    let res = ops::eval_cosine_distance(&lhs, &rhs)?;
                    store_result(&mut self.registers, *dst, res)?;
                }
                MicroOp::VectorL2Distance { dst, src1, src2 } => {
                    let lhs = self.registers.to_array(*src1)?;
                    let rhs = self.registers.to_array(*src2)?;
                    let res = ops::eval_l2_distance(&lhs, &rhs)?;
                    store_result(&mut self.registers, *dst, res)?;
                }
                MicroOp::VectorDotProduct { dst, src1, src2 } => {
                    let lhs = self.registers.to_array(*src1)?;
                    let rhs = self.registers.to_array(*src2)?;
                    let res = ops::eval_dot_product(&lhs, &rhs)?;
                    store_result(&mut self.registers, *dst, res)?;
                }
                MicroOp::StateBuild { .. }
                | MicroOp::StateProbe { .. }
                | MicroOp::StateFold { .. } => {
                    // Handled by downstream state breaker operators
                }
            }
        }

        Ok(())
    }
}

impl PipelineNode<DataChunk> for VectorVmExecutor {
    fn push(&mut self, mut chunk: DataChunk) -> Result<()> {
        if chunk.selection.is_empty() {
            return Ok(());
        }

        let started = Instant::now();
        let rows_in = chunk.selection.size();
        let outcome = self.eval_chunk(&mut chunk);
        self.stats.chunks += 1;
        self.stats.rows_in += rows_in as u64;
        self.stats.eval_us += started.elapsed().as_micros() as u64;
        outcome?;

        if chunk.selection.is_empty() {
            return Ok(());
        }
        self.stats.rows_out += chunk.selection.size() as u64;

        self.next.push(chunk)
    }

    fn flush(&mut self) -> Result<()> {
        let stats = std::mem::take(&mut self.stats);
        if stats.chunks > 0 {
            tracing::info!(
                target: "vein.audit",
                event = "vm_finished",
                chunks = stats.chunks,
                rows_in = stats.rows_in,
                rows_out = stats.rows_out,
                eval_us = stats.eval_us,
                "vm_finished"
            );
        }
        self.next.flush()
    }
}

impl Transform<DataChunk> for VectorVmExecutor {}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, Float64Array, Int64Array};
    use common::protocol::ScalarConstant;
    use std::sync::Mutex;
    use types::VeinDataType;

    struct CollectNode {
        chunks: Arc<Mutex<Vec<DataChunk>>>,
    }

    impl PipelineNode<DataChunk> for CollectNode {
        fn push(&mut self, chunk: DataChunk) -> Result<()> {
            self.chunks.lock().unwrap().push(chunk);
            Ok(())
        }

        fn flush(&mut self) -> Result<()> {
            Ok(())
        }
    }

    #[test]
    fn test_vector_vm_filter_and_project() {
        // Col0: [10, 20, 30, 40]
        // MicroProgram:
        // R1 = LoadConstant(25)
        // R2 = CmpGt(R0, R1)   -> [false, false, true, true]
        // FilterMask(R2)       -> selects indices 2 and 3
        // R3 = LoadConstant(1)
        // R4 = Add(R0, R3)     -> [11, 21, 31, 41]
        // ProjectRegisters([R4])
        let col0: ArrayRef = Arc::new(Int64Array::from(vec![10, 20, 30, 40]));
        let chunk = DataChunk::from_vectors(vec![col0]);

        let program = MicroProgram {
            register_count: 5,
            instructions: vec![
                MicroOp::LoadConstant {
                    dst: 1,
                    value: ScalarConstant::Int64(25),
                },
                MicroOp::CmpGt {
                    dst: 2,
                    src1: 0,
                    src2: 1,
                },
                MicroOp::FilterMask { bool_reg: 2 },
                MicroOp::LoadConstant {
                    dst: 3,
                    value: ScalarConstant::Int64(1),
                },
                MicroOp::Add {
                    dst: 4,
                    src1: 0,
                    src2: 3,
                },
                MicroOp::ProjectRegisters { out_regs: vec![4] },
            ],
        };

        let collected = Arc::new(Mutex::new(Vec::new()));
        let sink = Box::new(CollectNode {
            chunks: collected.clone(),
        });
        let registry = Arc::new(FunctionRegistry::new());
        let mut vm = VectorVmExecutor::new(Arc::new(program), registry, sink);

        vm.push(chunk).unwrap();
        vm.flush().unwrap();

        let chunks = collected.lock().unwrap();
        assert_eq!(chunks.len(), 1);
        let out_chunk = &chunks[0];
        assert_eq!(out_chunk.selection.size(), 2);
        assert_eq!(out_chunk.selection.get_index(0), 2);
        assert_eq!(out_chunk.selection.get_index(1), 3);

        let out_col = out_chunk
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(out_col.value(2), 31);
        assert_eq!(out_col.value(3), 41);
    }

    /// A constant literal must stay a length-1 broadcast scalar; only a consumer
    /// that genuinely needs a column (here `ProjectRegisters`) may expand it.
    #[test]
    fn test_load_constant_stays_scalar_until_read() {
        let col0: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3]));
        let chunk = DataChunk::from_vectors(vec![col0]);

        let program = MicroProgram {
            register_count: 2,
            instructions: vec![MicroOp::LoadConstant {
                dst: 1,
                value: ScalarConstant::Int64(7),
            }],
        };

        let collected = Arc::new(Mutex::new(Vec::new()));
        let sink = Box::new(CollectNode {
            chunks: collected.clone(),
        });
        let registry = Arc::new(FunctionRegistry::new());
        let mut vm = VectorVmExecutor::new(Arc::new(program), registry, sink);
        vm.push(chunk).unwrap();
        vm.flush().unwrap();

        let literal = vm.registers().get(1).unwrap();
        assert!(literal.is_scalar());
        assert_eq!(literal.physical_len(), 1);
    }

    /// A `Cast` instruction must convert the register it reads, and the
    /// converted column — not the source column — must reach the sink.
    #[test]
    fn test_vector_vm_cast_converts_register() {
        // Col0: Int64 [1, 2, 3]
        // MicroProgram:
        // R1 = Cast(R0, Float64)  -> [1.0, 2.0, 3.0]
        // ProjectRegisters([R1])
        let col0: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3]));
        let chunk = DataChunk::from_vectors(vec![col0]);

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

        let collected = Arc::new(Mutex::new(Vec::new()));
        let sink = Box::new(CollectNode {
            chunks: collected.clone(),
        });
        let registry = Arc::new(FunctionRegistry::new());
        let mut vm = VectorVmExecutor::new(Arc::new(program), registry, sink);

        vm.push(chunk).unwrap();
        vm.flush().unwrap();

        let chunks = collected.lock().unwrap();
        assert_eq!(chunks.len(), 1);
        let out_col = chunks[0]
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("Cast must project a Float64 column");
        assert_eq!(out_col.value(0), 1.0);
        assert_eq!(out_col.value(2), 3.0);
    }
}
