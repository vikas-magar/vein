//! Vectorized micro-instruction definitions for Micro-IR.

use serde::{Deserialize, Serialize};
use types::VeinDataType;

use super::constant::ScalarConstant;
use super::program::RegId;
use super::state::{AggOpCode, JoinKind, StateMode};

/// A compact, zero-copy instruction operating on columnar vector registers.
#[derive(
    Debug, Clone, PartialEq, Serialize, Deserialize, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize,
)]
pub enum MicroOp {
    // ── Scalar & Register Constants ──────────────────────────────
    /// Load a scalar constant into a register across all active rows.
    LoadConstant {
        /// Destination register.
        dst: RegId,
        /// Scalar constant value.
        value: ScalarConstant,
    },
    /// Copy reference from one register to another.
    CopyRegister {
        /// Destination register.
        dst: RegId,
        /// Source register.
        src: RegId,
    },

    // ── Type Conversion ──────────────────────────────────────────
    /// Convert a register to another logical type: `dst = cast(src AS target)`.
    ///
    /// The conversion is checked: values that cannot be represented in `target`
    /// (an out-of-range integer, an unparsable string) fail the task rather than
    /// wrapping or truncating silently.
    Cast {
        /// Destination register.
        dst: RegId,
        /// Source register.
        src: RegId,
        /// Logical type to convert the source array to.
        target: VeinDataType,
    },

    // ── Arithmetic Operations (SIMD Vectorized) ──────────────────
    /// Vector addition: `dst = src1 + src2`
    Add {
        /// Destination register.
        dst: RegId,
        /// Left operand register.
        src1: RegId,
        /// Right operand register.
        src2: RegId,
    },
    /// Vector subtraction: `dst = src1 - src2`
    Sub {
        /// Destination register.
        dst: RegId,
        /// Left operand register.
        src1: RegId,
        /// Right operand register.
        src2: RegId,
    },
    /// Vector multiplication: `dst = src1 * src2`
    Mul {
        /// Destination register.
        dst: RegId,
        /// Left operand register.
        src1: RegId,
        /// Right operand register.
        src2: RegId,
    },
    /// Vector division: `dst = src1 / src2`
    Div {
        /// Destination register.
        dst: RegId,
        /// Left operand register.
        src1: RegId,
        /// Right operand register.
        src2: RegId,
    },
    /// Vector modulo: `dst = src1 % src2`
    Mod {
        /// Destination register.
        dst: RegId,
        /// Left operand register.
        src1: RegId,
        /// Right operand register.
        src2: RegId,
    },
    /// Unary negation: `dst = -src`
    Neg {
        /// Destination register.
        dst: RegId,
        /// Source register.
        src: RegId,
    },

    // ── Comparison Operations (Emits Boolean Vector) ─────────────
    /// Vector comparison: `dst = src1 == src2`
    CmpEq {
        /// Destination register.
        dst: RegId,
        /// Left operand register.
        src1: RegId,
        /// Right operand register.
        src2: RegId,
    },
    /// Vector comparison: `dst = src1 != src2`
    CmpNeq {
        /// Destination register.
        dst: RegId,
        /// Left operand register.
        src1: RegId,
        /// Right operand register.
        src2: RegId,
    },
    /// Vector comparison: `dst = src1 < src2`
    CmpLt {
        /// Destination register.
        dst: RegId,
        /// Left operand register.
        src1: RegId,
        /// Right operand register.
        src2: RegId,
    },
    /// Vector comparison: `dst = src1 <= src2`
    CmpLte {
        /// Destination register.
        dst: RegId,
        /// Left operand register.
        src1: RegId,
        /// Right operand register.
        src2: RegId,
    },
    /// Vector comparison: `dst = src1 > src2`
    CmpGt {
        /// Destination register.
        dst: RegId,
        /// Left operand register.
        src1: RegId,
        /// Right operand register.
        src2: RegId,
    },
    /// Vector comparison: `dst = src1 >= src2`
    CmpGte {
        /// Destination register.
        dst: RegId,
        /// Left operand register.
        src1: RegId,
        /// Right operand register.
        src2: RegId,
    },

    // ── Boolean & Logical Operations ─────────────────────────────
    /// Logical AND: `dst = src1 AND src2`
    And {
        /// Destination register.
        dst: RegId,
        /// Left operand register.
        src1: RegId,
        /// Right operand register.
        src2: RegId,
    },
    /// Logical OR: `dst = src1 OR src2`
    Or {
        /// Destination register.
        dst: RegId,
        /// Left operand register.
        src1: RegId,
        /// Right operand register.
        src2: RegId,
    },
    /// Logical NOT: `dst = NOT src`
    Not {
        /// Destination register.
        dst: RegId,
        /// Source operand register.
        src: RegId,
    },
    /// IS NULL check: `dst = is_null(src)`
    IsNull {
        /// Destination register.
        dst: RegId,
        /// Source operand register.
        src: RegId,
    },
    /// IS NOT NULL check: `dst = is_not_null(src)`
    IsNotNull {
        /// Destination register.
        dst: RegId,
        /// Source operand register.
        src: RegId,
    },

    // ── AI Vector & Embedding Operations ─────────────────────────
    /// Cosine distance: `dst = cosine_distance(src1, src2)`
    VectorCosineDistance {
        /// Destination register.
        dst: RegId,
        /// Left embedding register.
        src1: RegId,
        /// Right embedding register.
        src2: RegId,
    },
    /// Euclidean (L2) distance: `dst = l2_distance(src1, src2)`
    VectorL2Distance {
        /// Destination register.
        dst: RegId,
        /// Left embedding register.
        src1: RegId,
        /// Right embedding register.
        src2: RegId,
    },
    /// Dot product: `dst = dot_product(src1, src2)`
    VectorDotProduct {
        /// Destination register.
        dst: RegId,
        /// Left embedding register.
        src1: RegId,
        /// Right embedding register.
        src2: RegId,
    },

    // ── Extensible UDF Invocation ────────────────────────────────
    /// Invoke a registered scalar UDF
    CallUdf {
        /// Canonical name of the UDF.
        name: String,
        /// Input argument registers.
        arg_regs: Vec<RegId>,
        /// Destination register.
        dst: RegId,
    },

    // ── Selection Vector & Projection Assembly ───────────────────
    /// In-place mutation of the chunk's SelectionVector using boolean register `bool_reg`.
    FilterMask {
        /// Register containing boolean mask.
        bool_reg: RegId,
    },
    /// Project specified registers as the output columns of the DataChunk.
    ProjectRegisters {
        /// Registers to project in output column order.
        out_regs: Vec<RegId>,
    },

    // ── State Pipeline Breakers (Joins, Aggregations, Windows) ───
    /// Materialize state for a pipeline breaker.
    StateBuild {
        /// State structure identifier.
        state_id: u32,
        /// Key registers.
        key_regs: Vec<RegId>,
        /// Payload registers.
        payload_regs: Vec<RegId>,
        /// State mode.
        mode: StateMode,
    },
    /// Probe stream against materialized state.
    StateProbe {
        /// State structure identifier.
        state_id: u32,
        /// Key registers.
        key_regs: Vec<RegId>,
        /// Output registers.
        out_regs: Vec<RegId>,
        /// Join flavor.
        join_kind: JoinKind,
    },
    /// Accumulate state in-place.
    StateFold {
        /// State structure identifier.
        state_id: u32,
        /// Registers to aggregate.
        agg_regs: Vec<RegId>,
        /// Aggregate opcode.
        op: AggOpCode,
    },
}
