use common::protocol::MicroOp;
use common::Result;

use super::VectorVmExecutor;

/// Lists VM operations that preserve or explicitly handle reference-backed values.
/// New micro-operations must opt in before they can execute on a view pipeline.
pub(super) fn supports_view_execution(op: &MicroOp) -> bool {
    matches!(
        op,
        MicroOp::LoadConstant { .. }
            | MicroOp::CopyRegister { .. }
            | MicroOp::Cast { .. }
            | MicroOp::Add { .. }
            | MicroOp::Sub { .. }
            | MicroOp::Mul { .. }
            | MicroOp::Div { .. }
            | MicroOp::Mod { .. }
            | MicroOp::Neg { .. }
            | MicroOp::CmpEq { .. }
            | MicroOp::InList { .. }
            | MicroOp::CmpNeq { .. }
            | MicroOp::CmpGt { .. }
            | MicroOp::CmpGte { .. }
            | MicroOp::CmpLt { .. }
            | MicroOp::CmpLte { .. }
            | MicroOp::Like { .. }
            | MicroOp::And { .. }
            | MicroOp::Or { .. }
            | MicroOp::Not { .. }
            | MicroOp::IsNull { .. }
            | MicroOp::IsNotNull { .. }
            | MicroOp::Case { .. }
            | MicroOp::CallUdf { .. }
            | MicroOp::VectorCosineDistance { .. }
            | MicroOp::VectorL2Distance { .. }
            | MicroOp::VectorDotProduct { .. }
            | MicroOp::FilterMask { .. }
            | MicroOp::ProjectRegisters { .. }
            | MicroOp::StateBuild { .. }
            | MicroOp::StateProbe { .. }
            | MicroOp::StateFold { .. }
    )
}

impl<B> VectorVmExecutor<B> {
    pub(super) fn reject_mapped_instruction_inputs(
        &self,
        instruction_index: usize,
        op: &MicroOp,
    ) -> Result<()> {
        let inputs = self.instruction_inputs.get(instruction_index).ok_or_else(|| {
            common::VeinError::internal("VM instruction input metadata is out of bounds")
        })?;
        for &register in inputs {
            if self.registers.get(register)?.is_mapped() {
                return Err(common::VeinError::unsupported(
                    format!(
                        "VM operator {op:?} has no mapped-input implementation; refusing to gather source-backed values"
                    ),
                    None,
                ));
            }
        }
        Ok(())
    }
}
