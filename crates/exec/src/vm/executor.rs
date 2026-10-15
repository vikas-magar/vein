//! The core Vector VM execution engine.
//!
//! Provides [`VectorVmExecutor`], which interprets a [`MicroProgram`] against
//! incoming in-flight [`DataChunk`]s in a tight, cache-coherent instruction loop.

use super::ops;
use super::register::RegisterFile;
use crate::operators::sink::DiscardSink;
use crate::pipeline::{DataChunk, DataChunkView, MaterializationReport, PipelineNode, Transform};
use crate::udf::FunctionRegistry;
use arrow::array::ArrayRef;
use common::Result;
use common::protocol::{MicroOp, MicroProgram, RegId};
use std::collections::HashSet;
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
    /// Array conversions for referenced view inputs, excluding computed outputs.
    input_materialization: MaterializationReport,
    /// Time spent converting referenced view input columns.
    input_materialize_us: u64,
}

impl VmStats {
    fn accumulate(&mut self, other: Self) {
        self.chunks = self.chunks.saturating_add(other.chunks);
        self.rows_in = self.rows_in.saturating_add(other.rows_in);
        self.rows_out = self.rows_out.saturating_add(other.rows_out);
        self.eval_us = self.eval_us.saturating_add(other.eval_us);
        self.input_materialization
            .accumulate(other.input_materialization);
        self.input_materialize_us = self
            .input_materialize_us
            .saturating_add(other.input_materialize_us);
    }
}

/// Vector VM Executor node in the physical execution pipeline.
///
/// Holds the pre-allocated virtual register file and compiles/interprets the
/// [`MicroProgram`] instructions on incoming batches without steady-state allocations.
pub struct VectorVmExecutor<B = DataChunk> {
    program: Arc<MicroProgram>,
    registers: RegisterFile,
    registry: Arc<FunctionRegistry>,
    next: Box<dyn PipelineNode<B>>,
    input_registers: Vec<RegId>,
    written_registers: HashSet<RegId>,
    projection: Option<Vec<RegId>>,
    stats: VmStats,
}

impl<B> VectorVmExecutor<B> {
    fn with_next(
        program: Arc<MicroProgram>,
        registry: Arc<FunctionRegistry>,
        next: Box<dyn PipelineNode<B>>,
    ) -> Self {
        let register_count = program.register_count;
        let (input_registers, written_registers, projection) = analyze_registers(&program);
        Self {
            program,
            registers: RegisterFile::new(register_count),
            registry,
            next,
            input_registers,
            written_registers,
            projection,
            stats: VmStats::default(),
        }
    }
}

impl VectorVmExecutor {
    /// Constructs a new [`VectorVmExecutor`].
    pub fn new(
        program: Arc<MicroProgram>,
        registry: Arc<FunctionRegistry>,
        next: Box<dyn PipelineNode<DataChunk>>,
    ) -> Self {
        Self::with_next(program, registry, next)
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

    /// Builds an evaluator with no downstream node.
    ///
    /// Sources use this to evaluate a push-down predicate over a batch of the
    /// predicate's own input columns through the same VM that runs the residual
    /// pipeline: one predicate implementation, one set of kernels.
    pub fn for_evaluation(program: Arc<MicroProgram>, registry: Arc<FunctionRegistry>) -> Self {
        Self::new(program, registry, Box::new(DiscardSink::new()))
    }

    /// Evaluates the mounted program over `chunk` and returns register `reg`,
    /// broadcast to the chunk's row count.
    pub fn evaluate(&mut self, chunk: &mut DataChunk, reg: RegId) -> Result<ArrayRef> {
        self.eval_chunk(chunk)?;
        self.registers.to_array(reg)
    }
}

impl VectorVmExecutor<DataChunkView> {
    /// Constructs a VM pipeline node that accepts and emits reference-backed views.
    pub fn new_view(
        program: Arc<MicroProgram>,
        registry: Arc<FunctionRegistry>,
        next: Box<dyn PipelineNode<DataChunkView>>,
    ) -> Self {
        Self::with_next(program, registry, next)
    }

    /// Borrows the active [`MicroProgram`].
    pub fn program(&self) -> &MicroProgram {
        &self.program
    }

    /// Borrows the register file holding the most recently executed batch.
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

/// Finds registers that need an input value, registers written by the program,
/// and the final requested projection. This runs once when the VM node is built
/// so view batches only materialize input columns that the program reads.
fn analyze_registers(
    program: &MicroProgram,
) -> (Vec<RegId>, HashSet<RegId>, Option<Vec<RegId>>) {
    let mut defined = HashSet::new();
    let mut reads = HashSet::new();
    let mut writes = HashSet::new();
    let mut projection = None;

    macro_rules! read {
        ($register:expr) => {{
            let register = $register;
            if !defined.contains(&register) {
                reads.insert(register);
            }
        }};
    }
    macro_rules! write {
        ($register:expr) => {{
            let register = $register;
            defined.insert(register);
            writes.insert(register);
        }};
    }

    for instruction in &program.instructions {
        match instruction {
            MicroOp::LoadConstant { dst, .. } => write!(*dst),
            MicroOp::CopyRegister { dst, src } => {
                read!(*src);
                write!(*dst);
            }
            MicroOp::Cast { dst, src, .. }
            | MicroOp::Neg { dst, src }
            | MicroOp::Not { dst, src }
            | MicroOp::IsNull { dst, src }
            | MicroOp::IsNotNull { dst, src } => {
                read!(*src);
                write!(*dst);
            }
            MicroOp::Add { dst, src1, src2 }
            | MicroOp::Sub { dst, src1, src2 }
            | MicroOp::Mul { dst, src1, src2 }
            | MicroOp::Div { dst, src1, src2 }
            | MicroOp::Mod { dst, src1, src2 }
            | MicroOp::CmpEq { dst, src1, src2 }
            | MicroOp::CmpNeq { dst, src1, src2 }
            | MicroOp::CmpLt { dst, src1, src2 }
            | MicroOp::CmpLte { dst, src1, src2 }
            | MicroOp::CmpGt { dst, src1, src2 }
            | MicroOp::CmpGte { dst, src1, src2 }
            | MicroOp::And { dst, src1, src2 }
            | MicroOp::Or { dst, src1, src2 }
            | MicroOp::VectorCosineDistance { dst, src1, src2 }
            | MicroOp::VectorL2Distance { dst, src1, src2 }
            | MicroOp::VectorDotProduct { dst, src1, src2 } => {
                read!(*src1);
                read!(*src2);
                write!(*dst);
            }
            MicroOp::Like { dst, src, pattern, .. } => {
                read!(*src);
                read!(*pattern);
                write!(*dst);
            }
            MicroOp::InList { dst, value, candidates, .. } => {
                read!(*value);
                for register in candidates { read!(*register); }
                write!(*dst);
            }
            MicroOp::Case { dst, when_regs, then_regs, else_reg, .. } => {
                for register in when_regs { read!(*register); }
                for register in then_regs { read!(*register); }
                if let Some(register) = else_reg { read!(*register); }
                write!(*dst);
            }
            MicroOp::CallUdf { dst, arg_regs, .. } => {
                for register in arg_regs { read!(*register); }
                write!(*dst);
            }
            MicroOp::FilterMask { bool_reg } => read!(*bool_reg),
            MicroOp::ProjectRegisters { out_regs } => {
                projection = Some(out_regs.clone());
            }
            MicroOp::StateBuild { key_regs, payload_regs, .. } => {
                for register in key_regs { read!(*register); }
                for register in payload_regs { read!(*register); }
            }
            MicroOp::StateProbe { key_regs, out_regs, .. } => {
                for register in key_regs { read!(*register); }
                for register in out_regs { write!(*register); }
            }
            MicroOp::StateFold { agg_regs, .. } => {
                for register in agg_regs {
                    read!(*register);
                    write!(*register);
                }
            }
        }
    }

    let mut input_registers = reads.into_iter().collect::<Vec<_>>();
    input_registers.sort_unstable();
    (input_registers, writes, projection)
}

impl<B> VectorVmExecutor<B> {
    /// Evaluates `chunk` through the program, leaving results in the registers.
    fn eval_chunk(&mut self, chunk: &mut DataChunk) -> Result<()> {
        self.registers.mount_chunk(chunk)?;
        self.eval_mounted_chunk(chunk)
    }

    /// Evaluates a program after its input registers have been mounted.
    fn eval_mounted_chunk(&mut self, chunk: &mut DataChunk) -> Result<()> {
        for op in &self.program.instructions {
            match op {
                MicroOp::LoadConstant { dst, value } => {
                    let scalar = ops::literal_to_scalar(value)?;
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
                MicroOp::Like {
                    dst,
                    src,
                    pattern,
                    negated,
                } => {
                    let res = ops::eval_like(
                        self.registers.get(*src)?,
                        self.registers.get(*pattern)?,
                        *negated,
                    )?;
                    store_result(&mut self.registers, *dst, res)?;
                }
                MicroOp::InList {
                    dst,
                    value,
                    candidates,
                    negated,
                } => {
                    let value = self.registers.get(*value)?.clone();
                    let candidates = candidates
                        .iter()
                        .map(|register| self.registers.get(*register).cloned())
                        .collect::<Result<Vec<_>>>()?;
                    let res = ops::eval_in_list(
                        &value,
                        &candidates,
                        self.registers.chunk_len(),
                        *negated,
                    )?;
                    store_result(&mut self.registers, *dst, res)?;
                }
                MicroOp::Case {
                    dst,
                    when_regs,
                    then_regs,
                    else_reg,
                    return_type,
                } => {
                    let conditions = when_regs
                        .iter()
                        .map(|register| self.registers.get(*register).cloned())
                        .collect::<Result<Vec<_>>>()?;
                    let results = then_regs
                        .iter()
                        .map(|register| self.registers.get(*register).cloned())
                        .collect::<Result<Vec<_>>>()?;
                    let otherwise = else_reg
                        .map(|register| self.registers.get(register).cloned())
                        .transpose()?;
                    let res = ops::eval_case(
                        &conditions,
                        &results,
                        otherwise.as_ref(),
                        &return_type.to_arrow(),
                        self.registers.chunk_len(),
                    )?;
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
                    let mask = self.registers.get(*bool_reg)?;
                    chunk.selection = ops::apply_filter_mask(&chunk.selection, mask)?;
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

    /// The VM is a pass-through: whether the pipeline is done is decided below
    /// it, so the source's question has to reach the node that knows.
    fn finished(&self) -> bool {
        self.next.finished()
    }
}

impl Transform<DataChunk> for VectorVmExecutor {
    fn fork_transform(&self) -> Result<Option<Box<dyn Transform<DataChunk>>>> {
        let Some(next) = self.next.fork_local()? else {
            return Ok(None);
        };
        Ok(Some(Box::new(Self::with_next(
            Arc::clone(&self.program),
            Arc::clone(&self.registry),
            next,
        ))))
    }

    fn combine_transform(self: Box<Self>, canonical: &mut dyn Transform<DataChunk>) -> Result<()> {
        let target = canonical
            .as_any_mut()
            .downcast_mut::<VectorVmExecutor<DataChunk>>()
            .ok_or_else(|| common::VeinError::internal("VM local state has a mismatched target"))?;
        combine_vm_state(self, target)
    }
}

impl PipelineNode<DataChunkView> for VectorVmExecutor<DataChunkView> {
    fn push(&mut self, view: DataChunkView) -> Result<()> {
        if view.num_rows() == 0 {
            return Ok(());
        }

        let started = Instant::now();
        let rows_in = view.num_rows();
        let input_columns = view.num_columns();
        let active_view = view.compact_active_rows()?;
        let row_count = active_view.num_rows();
        let materialize_started = Instant::now();
        let mut report = MaterializationReport::default();
        let mounted = self
            .input_registers
            .iter()
            .copied()
            .filter(|register| (*register as usize) < input_columns)
            .map(|register| {
                active_view
                    .materialize_column_with_report(register as usize)
                    .map(|(array, column_report)| {
                        report.accumulate(column_report);
                        (register, array)
                    })
            })
            .collect::<Result<Vec<_>>>()?;
        self.stats.input_materialization.accumulate(report);
        self.stats.input_materialize_us = self.stats.input_materialize_us.saturating_add(
            u64::try_from(materialize_started.elapsed().as_micros()).unwrap_or(u64::MAX),
        );
        let mut chunk = DataChunk::new(
            Vec::new(),
            common::arrow::SelectionVector::new_dense(row_count),
        );
        self.registers.mount_columns(row_count, mounted)?;
        self.eval_mounted_chunk(&mut chunk)?;
        let result = if chunk.selection.is_empty() {
            None
        } else {
            let output_registers = self
                .projection
                .clone()
                .unwrap_or_else(|| (0..input_columns).map(|index| index as RegId).collect());
            let mut output_columns = Vec::with_capacity(output_registers.len());
            let mut computed = Vec::new();
            for register in output_registers {
                let input_column = (register as usize) < input_columns
                    && !self.written_registers.contains(&register);
                if input_column {
                    output_columns.push(Some(register as usize));
                } else {
                    output_columns.push(None);
                    computed.push(self.registers.to_array(register)?);
                }
            }
            Some(active_view.project_with_arrays(
                &output_columns,
                computed,
                chunk.selection,
            )?)
        };
        self.stats.chunks += 1;
        self.stats.rows_in += rows_in as u64;
        self.stats.eval_us += started.elapsed().as_micros() as u64;
        if let Some(view) = result {
            self.stats.rows_out += view.num_rows() as u64;
            self.next.push(view)?;
        }
        Ok(())
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
                input_shared_columns = stats.input_materialization.shared_columns,
                input_gathered_columns = stats.input_materialization.gathered_columns,
                input_empty_columns = stats.input_materialization.empty_columns,
                input_gathered_output_buffer_bytes = stats.input_materialization.gathered_output_buffer_bytes,
                input_materialize_us = stats.input_materialize_us,
                "vm_finished"
            );
        }
        self.next.flush()
    }

    fn finished(&self) -> bool {
        self.next.finished()
    }

}

impl Transform<DataChunkView> for VectorVmExecutor<DataChunkView> {
    fn fork_transform(&self) -> Result<Option<Box<dyn Transform<DataChunkView>>>> {
        let Some(next) = self.next.fork_local()? else {
            return Ok(None);
        };
        Ok(Some(Box::new(Self::with_next(
            Arc::clone(&self.program),
            Arc::clone(&self.registry),
            next,
        ))))
    }

    fn combine_transform(
        self: Box<Self>,
        canonical: &mut dyn Transform<DataChunkView>,
    ) -> Result<()> {
        let target = canonical
            .as_any_mut()
            .downcast_mut::<VectorVmExecutor<DataChunkView>>()
            .ok_or_else(|| common::VeinError::internal("VM local state has a mismatched target"))?;
        combine_vm_state(self, target)
    }
}

fn combine_vm_state<B: 'static>(
    local: Box<VectorVmExecutor<B>>,
    canonical: &mut VectorVmExecutor<B>,
) -> Result<()> {
    if !Arc::ptr_eq(&local.program, &canonical.program) {
        return Err(common::VeinError::internal(
            "cannot combine VM states for different programs",
        ));
    }
    let VectorVmExecutor { next, stats, .. } = *local;
    next.combine_local(canonical.next.as_mut())?;
    canonical.stats.accumulate(stats);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipeline::{DataChunkView, RowMapping, ViewColumn};
    use arrow::array::{ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray};
    use types::LogicalLiteral;
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

    struct CollectViewNode {
        views: Arc<Mutex<Vec<DataChunkView>>>,
    }

    impl PipelineNode<DataChunkView> for CollectViewNode {
        fn push(&mut self, view: DataChunkView) -> Result<()> {
            self.views.lock().unwrap().push(view);
            Ok(())
        }

        fn flush(&mut self) -> Result<()> {
            Ok(())
        }
    }

    #[test]
    fn view_vm_filters_and_computes_while_preserving_passthrough_arrays() {
        let numbers: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), Some(3), None, Some(4)]));
        let labels: ArrayRef = Arc::new(StringArray::from(vec!["A", "B", "C", "D"]));
        let mapping = Arc::new(RowMapping::identity(Arc::from(vec![4]), 0, 4).unwrap());
        let number_column = ViewColumn::new(
            vec![Arc::clone(&numbers)],
            numbers.data_type().clone(),
            Arc::clone(&mapping),
        )
            .unwrap();
        let label_column = ViewColumn::new(
            vec![Arc::clone(&labels)],
            labels.data_type().clone(),
            mapping,
        )
            .unwrap();
        let view = DataChunkView::new(
            4,
            vec![number_column, label_column],
            common::arrow::SelectionVector::new_sparse(vec![3, 1, 2, 0]),
        )
            .unwrap();
        let program = MicroProgram {
            register_count: 5,
            instructions: vec![
                MicroOp::LoadConstant { dst: 2, value: LogicalLiteral::Int64(2) },
                MicroOp::CmpGt { dst: 3, src1: 0, src2: 2 },
                MicroOp::FilterMask { bool_reg: 3 },
                MicroOp::Add { dst: 4, src1: 0, src2: 2 },
                MicroOp::ProjectRegisters { out_regs: vec![1, 4] },
            ],
        };
        let views = Arc::new(Mutex::new(Vec::new()));
        let mut vm = VectorVmExecutor::new_view(
            Arc::new(program),
            Arc::new(FunctionRegistry::new()),
            Box::new(CollectViewNode { views: Arc::clone(&views) }),
        );

        vm.push(view).expect("view VM pipeline should run");
        assert_eq!(vm.stats.input_materialization.gathered_columns, 1);
        assert_eq!(vm.stats.input_materialization.shared_columns, 0);
        assert!(vm.stats.input_materialization.gathered_output_buffer_bytes > 0);
        vm.flush().expect("view VM pipeline should flush");
        assert_eq!(vm.stats.input_materialization, MaterializationReport::default());

        let views = views.lock().unwrap();
        let output = &views[0];
        assert_eq!(output.num_rows(), 2);
        assert_eq!(output.num_columns(), 2);
        let mut output_labels = Vec::new();
        for row in 0..output.num_rows() {
            let (array, physical_row) = output.resolve(0, row).unwrap().unwrap();
            assert!(Arc::ptr_eq(array, &labels));
            output_labels.push(
                array.as_any().downcast_ref::<StringArray>().unwrap().value(physical_row),
            );
        }
        assert_eq!(output_labels, vec!["D", "B"]);
        let computed = output.materialize_column(1).unwrap();
        let computed = computed.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(computed.values(), &[6, 5]);
        assert_eq!(
            vm.registers().get(1).unwrap().physical_len(),
            0,
            "passthrough output should not be gathered into a VM input register",
        );
    }

    #[test]
    fn view_vm_reports_shared_inputs_once_and_skips_projection_only_inputs() {
        let source: ArrayRef = Arc::new(Int64Array::from(vec![10, 20, 30]));
        for compute in [false, true] {
            let instructions = if compute {
                vec![
                    MicroOp::Add { dst: 1, src1: 0, src2: 0 },
                    MicroOp::ProjectRegisters { out_regs: vec![0, 1] },
                ]
            } else {
                vec![MicroOp::ProjectRegisters { out_regs: vec![0, 0] }]
            };
            let output = Arc::new(Mutex::new(Vec::new()));
            let mut vm = VectorVmExecutor::new_view(
                Arc::new(MicroProgram { register_count: 2, instructions }),
                Arc::new(FunctionRegistry::new()),
                Box::new(CollectViewNode { views: Arc::clone(&output) }),
            );
            for _ in 0..2 {
                vm.push(DataChunkView::from_chunk(DataChunk::from_vectors(vec![Arc::clone(&source)])).unwrap()).unwrap();
            }
            assert_eq!(vm.stats.input_materialization.shared_columns, if compute { 2 } else { 0 });
            assert_eq!(vm.stats.input_materialization.gathered_columns, 0);
            assert_eq!(vm.stats.input_materialization.gathered_output_buffer_bytes, 0);
            let views = output.lock().unwrap();
            assert_eq!(views.len(), 2);
            for view in views.iter() {
                assert!(Arc::ptr_eq(view.resolve(0, 0).unwrap().unwrap().0, &source));
                let values = view.materialize_column(1).unwrap();
                let values = values.as_any().downcast_ref::<Int64Array>().unwrap();
                assert_eq!(values.values(), if compute { &[20, 40, 60] } else { &[10, 20, 30] });
            }
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
                    value: LogicalLiteral::Int64(25),
                },
                MicroOp::CmpGt {
                    dst: 2,
                    src1: 0,
                    src2: 1,
                },
                MicroOp::FilterMask { bool_reg: 2 },
                MicroOp::LoadConstant {
                    dst: 3,
                    value: LogicalLiteral::Int64(1),
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

    #[test]
    fn vector_vm_filter_and_direct_projection_keep_source_array_shared() {
        let source: ArrayRef = Arc::new(Int64Array::from(vec![10, 20, 30, 40]));
        let chunk = DataChunk::from_vectors(vec![Arc::clone(&source)]);
        let program = MicroProgram {
            register_count: 3,
            instructions: vec![
                MicroOp::LoadConstant {
                    dst: 1,
                    value: LogicalLiteral::Int64(25),
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
        let collected = Arc::new(Mutex::new(Vec::new()));
        let sink = Box::new(CollectNode {
            chunks: Arc::clone(&collected),
        });
        let mut vm = VectorVmExecutor::new(
            Arc::new(program),
            Arc::new(FunctionRegistry::new()),
            sink,
        );

        vm.push(chunk).expect("filter and projection should execute");
        vm.flush().expect("projected chunk should flush");

        let chunks = collected.lock().expect("collected chunks");
        assert_eq!(chunks.len(), 1);
        assert!(Arc::ptr_eq(&source, chunks[0].column(0)));
        assert_eq!(chunks[0].selection.size(), 2);
        assert_eq!(chunks[0].selection.get_index(0), 2);
        assert_eq!(chunks[0].selection.get_index(1), 3);
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
                value: LogicalLiteral::Int64(7),
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

    #[test]
    fn vector_vm_executes_like_and_projects_boolean_result() {
        let values: ArrayRef =
            Arc::new(StringArray::from(vec!["POLISHED BRASS", "STEEL", "BRASS"]));
        let chunk = DataChunk::from_vectors(vec![values]);
        let program = MicroProgram {
            register_count: 3,
            instructions: vec![
                MicroOp::LoadConstant {
                    dst: 1,
                    value: LogicalLiteral::String("%BRASS".to_string()),
                },
                MicroOp::Like {
                    dst: 2,
                    src: 0,
                    pattern: 1,
                    negated: false,
                },
                MicroOp::ProjectRegisters { out_regs: vec![2] },
            ],
        };
        let collected = Arc::new(Mutex::new(Vec::new()));
        let sink = Box::new(CollectNode {
            chunks: Arc::clone(&collected),
        });
        let mut vm = VectorVmExecutor::new(
            Arc::new(program),
            Arc::new(FunctionRegistry::new()),
            sink,
        );

        vm.push(chunk).expect("LIKE instruction should execute");
        vm.flush().expect("LIKE result should flush");

        let chunks = collected.lock().unwrap();
        assert_eq!(chunks.len(), 1);
        let result = chunks[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::BooleanArray>()
            .expect("LIKE result is boolean");
        assert_eq!(
            (0..result.len())
                .map(|index| result.value(index))
                .collect::<Vec<_>>(),
            vec![true, false, true]
        );
    }

    #[test]
    fn vector_vm_projects_typed_null_boolean_without_losing_its_type() {
        let input: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3]));
        let chunk = DataChunk::from_vectors(vec![input]);
        let program = MicroProgram {
            register_count: 2,
            instructions: vec![
                MicroOp::LoadConstant {
                    dst: 1,
                    value: LogicalLiteral::Null(VeinDataType::Boolean),
                },
                MicroOp::ProjectRegisters { out_regs: vec![1] },
            ],
        };
        let collected = Arc::new(Mutex::new(Vec::new()));
        let mut vm = VectorVmExecutor::new(
            Arc::new(program),
            Arc::new(FunctionRegistry::new()),
            Box::new(CollectNode {
                chunks: Arc::clone(&collected),
            }),
        );

        vm.push(chunk)
            .expect("projecting a typed NULL boolean should preserve its schema type");
        vm.flush().expect("typed NULL boolean output should flush");

        let chunks = collected.lock().unwrap();
        assert_eq!(chunks.len(), 1);
        let result = chunks[0]
            .column(0)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("projected typed NULL should be boolean");
        assert_eq!(result, &BooleanArray::from(vec![None, None, None]));
    }

    #[test]
    fn vector_vm_executes_in_list_and_case_in_projected_expression() {
        let values: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), Some(2), None, Some(4)]));
        let chunk = DataChunk::from_vectors(vec![values]);
        let program = MicroProgram {
            register_count: 8,
            instructions: vec![
                MicroOp::LoadConstant {
                    dst: 1,
                    value: LogicalLiteral::Int64(2),
                },
                MicroOp::LoadConstant {
                    dst: 2,
                    value: LogicalLiteral::Null(VeinDataType::Int64),
                },
                MicroOp::InList {
                    dst: 3,
                    value: 0,
                    candidates: vec![1, 2],
                    negated: false,
                },
                MicroOp::LoadConstant {
                    dst: 4,
                    value: LogicalLiteral::String("matched".into()),
                },
                MicroOp::LoadConstant {
                    dst: 5,
                    value: LogicalLiteral::String("other".into()),
                },
                MicroOp::Case {
                    dst: 6,
                    when_regs: vec![3],
                    then_regs: vec![4],
                    else_reg: Some(5),
                    return_type: VeinDataType::Utf8,
                },
                MicroOp::ProjectRegisters { out_regs: vec![3, 6] },
            ],
        };
        let collected = Arc::new(Mutex::new(Vec::new()));
        let sink = Box::new(CollectNode { chunks: collected.clone() });
        let mut vm = VectorVmExecutor::new(
            Arc::new(program),
            Arc::new(FunctionRegistry::new()),
            sink,
        );

        vm.push(chunk).expect("conditional instructions should execute");
        vm.flush().expect("conditional results should flush");

        let chunks = collected.lock().unwrap();
        let membership = chunks[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::BooleanArray>()
            .unwrap();
        assert_eq!(membership, &arrow::array::BooleanArray::from(vec![None, Some(true), None, None]));
        let labels = chunks[0].column(1).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(labels, &StringArray::from(vec![Some("other"), Some("matched"), Some("other"), Some("other")]));
    }
}
