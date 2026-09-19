//! The core Vector VM execution engine.
//!
//! Provides [`VectorVmExecutor`], which interprets a [`MicroProgram`] against
//! incoming in-flight [`DataChunk`]s in a tight, cache-coherent instruction loop.

use std::sync::Arc;
use common::protocol::{MicroOp, MicroProgram};
use exo::AddressBatch;
use common::Result;
use crate::pipeline::{DataChunk, PipelineNode, Transform};
use crate::udf::FunctionRegistry;
use super::ops;
use super::register::RegisterFile;

/// Vector VM Executor node in the physical execution pipeline.
///
/// Holds the pre-allocated virtual register file and compiles/interprets the
/// [`MicroProgram`] instructions on incoming batches without steady-state allocations.
pub struct VectorVmExecutor {
    program: MicroProgram,
    registers: RegisterFile,
    address_registers: Vec<Option<AddressBatch>>,
    registry: Arc<FunctionRegistry>,
    next: Box<dyn PipelineNode<DataChunk>>,
}

impl VectorVmExecutor {
    /// Constructs a new [`VectorVmExecutor`].
    pub fn new(
        program: MicroProgram,
        registry: Arc<FunctionRegistry>,
        next: Box<dyn PipelineNode<DataChunk>>,
    ) -> Self {
        let register_count = program.register_count;
        let registers = RegisterFile::new(register_count);
        Self {
            program,
            address_registers: vec![None; register_count as usize],
            registers,
            registry,
            next,
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
}

fn set_computed(
    registers: &mut RegisterFile,
    address_registers: &mut [Option<AddressBatch>],
    dst: u8,
    array: arrow::array::ArrayRef,
) -> Result<()> {
    address_registers[dst as usize] = None;
    registers.set(dst, array)
}

impl PipelineNode<DataChunk> for VectorVmExecutor {
    fn push(&mut self, mut chunk: DataChunk) -> Result<()> {
        if chunk.selection.is_empty() {
            return Ok(());
        }

        let physical_rows = chunk.physical_len();
        self.registers.mount_chunk(&chunk)?;
        self.address_registers.fill(None);
        for (index, address_batch) in chunk.address_columns.iter().enumerate() {
            if let Some(register) = self.address_registers.get_mut(index) {
                *register = address_batch.clone();
            }
        }

        for op in &self.program.instructions {
            match op {
                MicroOp::LoadConstant { dst, value } => {
                    let arr = ops::scalar_constant_to_array(value, physical_rows)?;
                    set_computed(&mut self.registers, &mut self.address_registers, *dst, arr)?;
                }
                MicroOp::CopyRegister { dst, src } => {
                    let arr = self.registers.get(*src)?.clone();
                    let address_batch = self.address_registers[*src as usize].clone();
                    self.registers.set(*dst, arr)?;
                    self.address_registers[*dst as usize] = address_batch;
                }
                MicroOp::Add { dst, src1, src2 } => {
                    let res = ops::eval_add(
                        self.registers.get(*src1)?,
                        self.registers.get(*src2)?,
                    )?;
                    set_computed(&mut self.registers, &mut self.address_registers, *dst, res)?;
                }
                MicroOp::Sub { dst, src1, src2 } => {
                    let res = ops::eval_sub(
                        self.registers.get(*src1)?,
                        self.registers.get(*src2)?,
                    )?;
                    set_computed(&mut self.registers, &mut self.address_registers, *dst, res)?;
                }
                MicroOp::Mul { dst, src1, src2 } => {
                    let res = ops::eval_mul(
                        self.registers.get(*src1)?,
                        self.registers.get(*src2)?,
                    )?;
                    set_computed(&mut self.registers, &mut self.address_registers, *dst, res)?;
                }
                MicroOp::Div { dst, src1, src2 } => {
                    let res = ops::eval_div(
                        self.registers.get(*src1)?,
                        self.registers.get(*src2)?,
                    )?;
                    set_computed(&mut self.registers, &mut self.address_registers, *dst, res)?;
                }
                MicroOp::Mod { dst, src1, src2 } => {
                    let res = ops::eval_mod(
                        self.registers.get(*src1)?,
                        self.registers.get(*src2)?,
                    )?;
                    set_computed(&mut self.registers, &mut self.address_registers, *dst, res)?;
                }
                MicroOp::Neg { dst, src } => {
                    let res = ops::eval_neg(self.registers.get(*src)?)?;
                    set_computed(&mut self.registers, &mut self.address_registers, *dst, res)?;
                }
                MicroOp::CmpEq { dst, src1, src2 } => {
                    let res = ops::eval_cmp_eq(
                        self.registers.get(*src1)?,
                        self.registers.get(*src2)?,
                    )?;
                    set_computed(&mut self.registers, &mut self.address_registers, *dst, res)?;
                }
                MicroOp::CmpNeq { dst, src1, src2 } => {
                    let res = ops::eval_cmp_neq(
                        self.registers.get(*src1)?,
                        self.registers.get(*src2)?,
                    )?;
                    set_computed(&mut self.registers, &mut self.address_registers, *dst, res)?;
                }
                MicroOp::CmpGt { dst, src1, src2 } => {
                    let res = ops::eval_cmp_gt(
                        self.registers.get(*src1)?,
                        self.registers.get(*src2)?,
                    )?;
                    set_computed(&mut self.registers, &mut self.address_registers, *dst, res)?;
                }
                MicroOp::CmpGte { dst, src1, src2 } => {
                    let res = ops::eval_cmp_gte(
                        self.registers.get(*src1)?,
                        self.registers.get(*src2)?,
                    )?;
                    set_computed(&mut self.registers, &mut self.address_registers, *dst, res)?;
                }
                MicroOp::CmpLt { dst, src1, src2 } => {
                    let res = ops::eval_cmp_lt(
                        self.registers.get(*src1)?,
                        self.registers.get(*src2)?,
                    )?;
                    set_computed(&mut self.registers, &mut self.address_registers, *dst, res)?;
                }
                MicroOp::CmpLte { dst, src1, src2 } => {
                    let res = ops::eval_cmp_lte(
                        self.registers.get(*src1)?,
                        self.registers.get(*src2)?,
                    )?;
                    set_computed(&mut self.registers, &mut self.address_registers, *dst, res)?;
                }
                MicroOp::And { dst, src1, src2 } => {
                    let res = ops::eval_and(
                        self.registers.get(*src1)?,
                        self.registers.get(*src2)?,
                    )?;
                    set_computed(&mut self.registers, &mut self.address_registers, *dst, res)?;
                }
                MicroOp::Or { dst, src1, src2 } => {
                    let res = ops::eval_or(
                        self.registers.get(*src1)?,
                        self.registers.get(*src2)?,
                    )?;
                    set_computed(&mut self.registers, &mut self.address_registers, *dst, res)?;
                }
                MicroOp::Not { dst, src } => {
                    let res = ops::eval_not(self.registers.get(*src)?)?;
                    set_computed(&mut self.registers, &mut self.address_registers, *dst, res)?;
                }
                MicroOp::IsNull { dst, src } => {
                    let res = ops::eval_is_null(self.registers.get(*src)?)?;
                    set_computed(&mut self.registers, &mut self.address_registers, *dst, res)?;
                }
                MicroOp::IsNotNull { dst, src } => {
                    let res = ops::eval_is_not_null(self.registers.get(*src)?)?;
                    set_computed(&mut self.registers, &mut self.address_registers, *dst, res)?;
                }
                MicroOp::FilterMask { bool_reg } => {
                    let mask = self.registers.get(*bool_reg)?;
                    chunk.selection = ops::apply_filter_mask(&chunk.selection, mask)?;
                    if chunk.selection.is_empty() {
                        return Ok(());
                    }
                }
                MicroOp::ProjectRegisters { out_regs } => {
                    chunk.vectors = self.registers.project(out_regs)?;
                    chunk.address_columns = out_regs
                        .iter()
                        .map(|register| self.address_registers.get(*register as usize).cloned().unwrap_or(None))
                        .collect();
                }
                MicroOp::CallUdf { name, arg_regs, dst } => {
                    let mut args = Vec::with_capacity(arg_regs.len());
                    for &reg in arg_regs {
                        args.push(self.registers.get(reg)?.clone());
                    }
                    let res = ops::eval_call_udf(&self.registry, name, &args)?;
                    set_computed(&mut self.registers, &mut self.address_registers, *dst, res)?;
                }
                MicroOp::VectorCosineDistance { dst, src1, src2 } => {
                    let res = ops::eval_cosine_distance(
                        self.registers.get(*src1)?,
                        self.registers.get(*src2)?,
                    )?;
                    set_computed(&mut self.registers, &mut self.address_registers, *dst, res)?;
                }
                MicroOp::VectorL2Distance { dst, src1, src2 } => {
                    let res = ops::eval_l2_distance(
                        self.registers.get(*src1)?,
                        self.registers.get(*src2)?,
                    )?;
                    set_computed(&mut self.registers, &mut self.address_registers, *dst, res)?;
                }
                MicroOp::VectorDotProduct { dst, src1, src2 } => {
                    let res = ops::eval_dot_product(
                        self.registers.get(*src1)?,
                        self.registers.get(*src2)?,
                    )?;
                    set_computed(&mut self.registers, &mut self.address_registers, *dst, res)?;
                }
                MicroOp::StateBuild { .. }
                | MicroOp::StateProbe { .. }
                | MicroOp::StateFold { .. } => {
                    // Handled by downstream state breaker operators
                }
            }
        }

        self.next.push(chunk)
    }

    fn flush(&mut self) -> Result<()> {
        self.next.flush()
    }
}

impl Transform<DataChunk> for VectorVmExecutor {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use arrow::array::{ArrayRef, Int64Array};
    use common::protocol::ScalarConstant;

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
        let mut vm = VectorVmExecutor::new(program, registry, sink);

        vm.push(chunk).unwrap();
        vm.flush().unwrap();

        let chunks = collected.lock().unwrap();
        assert_eq!(chunks.len(), 1);
        let out_chunk = &chunks[0];
        assert_eq!(out_chunk.selection.size(), 2);
        assert_eq!(out_chunk.selection.get_index(0), 2);
        assert_eq!(out_chunk.selection.get_index(1), 3);

        let out_col = out_chunk.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(out_col.value(2), 31);
        assert_eq!(out_col.value(3), 41);
    }
}
