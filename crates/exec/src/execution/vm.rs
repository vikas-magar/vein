//! Evaluating VIR over the engine's own representation.
//!
//! `02-execution-and-vir-lld.md` §1 makes VIR the execution contract and
//! `01-final-architecture.md` §5 makes the Vector VM its first backend. This
//! module is that backend for the engine representation: it interprets a
//! [`MicroProgram`] against a [`DataChunk`] and leaves the condition in a
//! register.
//!
//! The program is produced once by the planner's bytecode compiler; only the
//! *interpretation* is new. That is the point of VIR being a contract — a second
//! backend does not require a second compiler.
//!
//! Conditions are [`BitMask`]es, which carry validity as well as value bits, so
//! three-valued logic survives `NOT`, `AND` and `OR`. A single bitmap would
//! conflate false with unknown and quietly change query results.
//!
//! The supported op set is the subset a predicate needs over `Int64`. Anything
//! else is refused by name rather than skipped, because a silently skipped
//! instruction produces a wrong answer instead of an error.

use std::sync::Arc;

use common::Result;
use common::protocol::{MicroOp, MicroProgram, RegId};
use error::VeinError;
use types::{LogicalLiteral, VeinDataType};
use vector::{DataChunk, I64Values, Validity, Vector, VectorRef, trim_words, words_for};

use super::Predicate;
use super::register::{BitMask, ColumnRef, RegisterFile, RegisterSlot};

/// Evaluates `program` over `chunk` and returns the condition left in `result`.
pub fn evaluate_condition(
    program: &MicroProgram,
    chunk: &DataChunk,
    result: RegId,
) -> Result<BitMask> {
    let row_count = chunk.row_count();
    let mut file = mount(program, chunk)?;
    for instruction in &program.instructions {
        execute(&mut file, instruction, row_count)?;
    }
    file.mask(result)
        .cloned()
        .ok_or_else(|| {
            VeinError::internal(format!(
                "register R{result} does not hold a condition after the program ran"
            ))
        })
}

/// A register file with `chunk`'s columns mounted, ready to run `program`.
fn mount<'a>(program: &MicroProgram, chunk: &'a DataChunk) -> Result<RegisterFile<'a>> {
    let mut file = RegisterFile::new(program.register_count as usize);
    file.mount(chunk)?;
    Ok(file)
}

/// Runs a whole planner program over `chunk`: expressions, the filter, and the
/// output projection.
///
/// [`evaluate_condition`] answers "which rows match"; this is the entry point a
/// task needs, because a planner program also narrows the selection and picks
/// the output columns (`02-execution-and-vir-lld.md` §4). The projection shares
/// the chunk's columns, so it moves handles rather than data (Rule 3).
///
/// A `ProjectRegisters` that names something other than a mounted column — a
/// literal is materialised as a constant column, which stays constant rather
/// than expanding to one buffer per row. Anything the engine representation
/// cannot carry is refused by name, the same rule the instruction set follows.
pub fn execute_chunk(program: &MicroProgram, chunk: DataChunk) -> Result<DataChunk> {
    let row_count = chunk.row_count();
    let mut file = mount(program, &chunk)?;
    let mut selection = chunk.selection().clone();
    let mut output: Option<Vec<RegId>> = None;
    for instruction in &program.instructions {
        match instruction {
            MicroOp::FilterMask { bool_reg } => {
                let condition = condition(&file, *bool_reg)?;
                selection = selection.intersect_keep(row_count, condition.into_keep(row_count));
            }
            MicroOp::ProjectRegisters { out_regs } => output = Some(out_regs.clone()),
            other => execute(&mut file, other, row_count)?,
        }
    }
    // The projection must be resolved while the registers still view the chunk.
    let projected = match &output {
        Some(registers) => Some(project(&chunk, &file, registers)?),
        None => None,
    };
    drop(file);

    match projected {
        Some(columns) => DataChunk::new(row_count, columns, selection)
            .ok_or_else(|| VeinError::internal("projected columns disagree with the row count")),
        None => Ok(chunk.with_selection(selection)),
    }
}

/// The output columns for `ProjectRegisters`.
///
/// A register that still views a chunk column shares it; a literal register is
/// materialised as a constant column.
fn project(
    chunk: &DataChunk,
    file: &RegisterFile<'_>,
    registers: &[RegId],
) -> Result<Vec<VectorRef>> {
    let mut columns = Vec::with_capacity(registers.len());
    for register in registers {
        columns.push(project_register(chunk, file, *register)?);
    }
    Ok(columns)
}

/// One projected column: the chunk column a register views, or a constant.
///
/// A mounted register is matched to its column by buffer identity, so an
/// aliasing `CopyRegister` is accepted and an overwritten register is refused
/// rather than mis-projected.
fn project_register(
    chunk: &DataChunk,
    file: &RegisterFile<'_>,
    register: RegId,
) -> Result<VectorRef> {
    if let Some(column) = file.column(register) {
        return match column {
            ColumnRef::Owned(vector) => Ok(Arc::clone(vector)),
            ColumnRef::Borrowed(view) => {
                for index in 0..chunk.num_columns() {
                    if let Some(source) = chunk.column(index)
                        && std::ptr::eq(view.vector(), source.as_ref())
                    {
                        return Ok(Arc::clone(source));
                    }
                }
                Err(VeinError::internal(format!(
                    "ProjectRegisters cannot project R{register}: it no longer holds a chunk column"
                )))
            }
        };
    }
    if let Some(value) = file.scalar(register) {
        return Ok(Arc::new(constant_column(value, chunk.row_count())?));
    }
    if let Some(mask) = file.mask(register) {
        return Ok(Arc::new(boolean_column(mask, chunk.row_count())));
    }
    Err(VeinError::internal(format!(
        "ProjectRegisters cannot project R{register}: it holds neither a column, a literal, nor a condition"
    )))
}

/// A nullable boolean column for a condition register.
///
/// The condition's bits are already packed LSB-first, which is the boolean
/// layout, so the value buffer is the bitmap itself. `valid` becomes the null
/// buffer, so a row whose condition is unknown projects as null rather than
/// false (ADR 0003).
fn boolean_column(mask: &BitMask, row_count: u32) -> Vector {
    Vector::from_boolean(
        mask.values.to_vec(),
        Validity::from_bitmap(mask.valid.to_vec()),
        row_count,
    )
}

/// A constant column for a literal the engine representation can carry.
fn constant_column(value: &LogicalLiteral, row_count: u32) -> Result<Vector> {
    let data_type = match value {
        LogicalLiteral::Int64(_) => VeinDataType::Int64,
        LogicalLiteral::String(_) => VeinDataType::Utf8,
        other => {
            return Err(VeinError::internal(format!(
                "a {other:?} literal cannot be projected as a column yet"
            )));
        }
    };
    Ok(Vector::constant(data_type, value.clone(), row_count))
}

/// Runs one instruction.
fn execute(file: &mut RegisterFile, instruction: &MicroOp, row_count: u32) -> Result<()> {
    match instruction {
        MicroOp::LoadConstant { dst, value } => file.set(
            *dst,
            RegisterSlot::Scalar {
                value: value.clone(),
            },
        ),
        MicroOp::CopyRegister { dst, src } => {
            let value = file.get(*src).cloned().ok_or_else(|| absent(*src))?;
            file.set(*dst, value)
        }
        MicroOp::Add { dst, src1, src2 } => {
            arithmetic_into::<Add>(file, *dst, *src1, *src2, row_count)
        }
        MicroOp::Sub { dst, src1, src2 } => {
            arithmetic_into::<Sub>(file, *dst, *src1, *src2, row_count)
        }
        MicroOp::Mul { dst, src1, src2 } => {
            arithmetic_into::<Mul>(file, *dst, *src1, *src2, row_count)
        }
        MicroOp::Div { dst, src1, src2 } => {
            arithmetic_into::<Div>(file, *dst, *src1, *src2, row_count)
        }
        MicroOp::Mod { dst, src1, src2 } => {
            arithmetic_into::<Mod>(file, *dst, *src1, *src2, row_count)
        }
        MicroOp::Neg { dst, src } => negate_into(file, *dst, *src, row_count),
        MicroOp::CmpEq { dst, src1, src2 } => {
            compare_into::<Equal>(file, *dst, *src1, *src2, row_count)
        }
        MicroOp::CmpNeq { dst, src1, src2 } => {
            compare_into::<NotEqual>(file, *dst, *src1, *src2, row_count)
        }
        MicroOp::CmpLt { dst, src1, src2 } => {
            compare_into::<Less>(file, *dst, *src1, *src2, row_count)
        }
        MicroOp::CmpLte { dst, src1, src2 } => {
            compare_into::<LessOrEqual>(file, *dst, *src1, *src2, row_count)
        }
        MicroOp::CmpGt { dst, src1, src2 } => {
            compare_into::<Greater>(file, *dst, *src1, *src2, row_count)
        }
        MicroOp::CmpGte { dst, src1, src2 } => {
            compare_into::<GreaterOrEqual>(file, *dst, *src1, *src2, row_count)
        }
        MicroOp::And { dst, src1, src2 } => {
            let left = condition(file, *src1)?;
            let right = condition(file, *src2)?;
            // True only when both are true; known only when both are known.
            let rest = zip_words(&left.values, &right.values, row_count, |a, b| a & b);
            let mask = BitMask {
                values: rest,
                valid: zip_words(&left.valid, &right.valid, row_count, |a, b| a & b),
            };
            file.set(*dst, RegisterSlot::Mask { mask })
        }
        MicroOp::Or { dst, src1, src2 } => {
            let left = condition(file, *src1)?;
            let right = condition(file, *src2)?;
            // True when either is true; known only when both are known.
            let rest = zip_words(&left.values, &right.values, row_count, |a, b| a | b);
            let mask = BitMask {
                values: rest,
                valid: zip_words(&left.valid, &right.valid, row_count, |a, b| a & b),
            };
            file.set(*dst, RegisterSlot::Mask { mask })
        }
        MicroOp::Not { dst, src } => {
            let condition = condition(file, *src)?;
            // Validity is unchanged: `NOT NULL` is still null.
            let mask = BitMask {
                values: map_words(&condition.values, row_count, |word| !word),
                valid: Arc::clone(&condition.valid),
            };
            file.set(*dst, RegisterSlot::Mask { mask })
        }
        MicroOp::IsNull { dst, src } => {
            let mask = null_condition(file, *src, row_count)?;
            file.set(*dst, RegisterSlot::Mask { mask })
        }
        MicroOp::IsNotNull { dst, src } => {
            let nulls = null_condition(file, *src, row_count)?;
            // `IS NOT NULL` is never null: a null test always has an answer.
            let mask = BitMask {
                values: map_words(&nulls.values, row_count, |word| !word),
                valid: nulls.valid,
            };
            file.set(*dst, RegisterSlot::Mask { mask })
        }
        other => Err(VeinError::internal(format!(
            "the engine-representation VM does not implement {other:?}"
        ))),
    }
}

/// A comparison operator.
///
/// Each operator is a zero-sized type rather than a `fn` pointer, so a kernel
/// monomorphises per operator and the test inlines. A pointer chosen at run time
/// is an indirect call in the innermost loop, and it blocks the auto-vectorising
/// a SIMD kernel needs (Rule 4).
trait Comparison {
    /// Whether `left <op> right`.
    fn test(left: i64, right: i64) -> bool;
}

/// `=`
struct Equal;
impl Comparison for Equal {
    #[inline(always)]
    fn test(left: i64, right: i64) -> bool {
        left == right
    }
}

/// `<>`
struct NotEqual;
impl Comparison for NotEqual {
    #[inline(always)]
    fn test(left: i64, right: i64) -> bool {
        left != right
    }
}

/// `<`
struct Less;
impl Comparison for Less {
    #[inline(always)]
    fn test(left: i64, right: i64) -> bool {
        left < right
    }
}

/// `<=`
struct LessOrEqual;
impl Comparison for LessOrEqual {
    #[inline(always)]
    fn test(left: i64, right: i64) -> bool {
        left <= right
    }
}

/// `>`
struct Greater;
impl Comparison for Greater {
    #[inline(always)]
    fn test(left: i64, right: i64) -> bool {
        left > right
    }
}

/// `>=`
struct GreaterOrEqual;
impl Comparison for GreaterOrEqual {
    #[inline(always)]
    fn test(left: i64, right: i64) -> bool {
        left >= right
    }
}

/// Evaluates one comparison instruction and writes its condition to `dst`.
fn compare_into<C: Comparison>(
    file: &mut RegisterFile,
    dst: RegId,
    src1: RegId,
    src2: RegId,
    row_count: u32,
) -> Result<()> {
    // The operands borrow the register file, so the comparison is evaluated
    // before the result is written back into it.
    let mask = {
        let left = operand(file, src1)?;
        let right = operand(file, src2)?;
        compare::<C>(&left, &right, row_count)
    };
    file.set(dst, RegisterSlot::Mask { mask })
}

/// A binary integer operator.
trait Arithmetic {
    /// The operator's name, for error messages.
    const NAME: &'static str;
    /// `left <op> right`, or `None` on overflow or division by zero.
    fn eval(left: i64, right: i64) -> Option<i64>;
}

/// `+`
struct Add;
impl Arithmetic for Add {
    const NAME: &'static str = "Add";
    #[inline(always)]
    fn eval(left: i64, right: i64) -> Option<i64> {
        left.checked_add(right)
    }
}

/// `-`
struct Sub;
impl Arithmetic for Sub {
    const NAME: &'static str = "Sub";
    #[inline(always)]
    fn eval(left: i64, right: i64) -> Option<i64> {
        left.checked_sub(right)
    }
}

/// `*`
struct Mul;
impl Arithmetic for Mul {
    const NAME: &'static str = "Mul";
    #[inline(always)]
    fn eval(left: i64, right: i64) -> Option<i64> {
        left.checked_mul(right)
    }
}

/// `/`
struct Div;
impl Arithmetic for Div {
    const NAME: &'static str = "Div";
    #[inline(always)]
    fn eval(left: i64, right: i64) -> Option<i64> {
        left.checked_div(right)
    }
}

/// `%`
struct Mod;
impl Arithmetic for Mod {
    const NAME: &'static str = "Mod";
    #[inline(always)]
    fn eval(left: i64, right: i64) -> Option<i64> {
        left.checked_rem(right)
    }
}

/// Evaluates a binary integer operation and writes its column to `dst`.
fn arithmetic_into<A: Arithmetic>(
    file: &mut RegisterFile,
    dst: RegId,
    src1: RegId,
    src2: RegId,
    row_count: u32,
) -> Result<()> {
    let column = {
        let left = operand(file, src1)?;
        let right = operand(file, src2)?;
        arithmetic::<A>(&left, &right, row_count)?
    };
    file.set_owned(dst, column)
}

/// Evaluates unary negation and writes its column to `dst`.
fn negate_into(file: &mut RegisterFile, dst: RegId, src: RegId, row_count: u32) -> Result<()> {
    let column = {
        let source = operand(file, src)?;
        negate(&source, row_count)?
    };
    file.set_owned(dst, column)
}

/// `left <op> right` row by row, as an owned column.
///
/// A null on either side makes the row null, so a null marshal never becomes a
/// value. An overflow or a division by zero fails the query rather than
/// wrapping, which is what the Arrow path does and what
/// `09-testing-and-correctness.md` §4 makes the reference. The result is new
/// data, so the register file owns it (ADR 0004).
fn arithmetic<A: Arithmetic>(
    left: &Operand<'_>,
    right: &Operand<'_>,
    row_count: u32,
) -> Result<VectorRef> {
    computed_column(row_count, |row| {
        match (value_at(left, row), value_at(right, row)) {
            (Some(left), Some(right)) => A::eval(left, right).map(Some).ok_or_else(|| {
                VeinError::internal(format!(
                    "{} overflowed or divided by zero at row {row}",
                    A::NAME
                ))
            }),
            _ => Ok(None),
        }
    })
}

/// `-operand` row by row, as an owned column.
fn negate(operand: &Operand<'_>, row_count: u32) -> Result<VectorRef> {
    computed_column(row_count, |row| match value_at(operand, row) {
        Some(value) => value.checked_neg().map(Some).ok_or_else(|| {
            VeinError::internal(format!("Neg overflowed at row {row}"))
        }),
        None => Ok(None),
    })
}

/// A dense column from one computed value per row, `None` marking a null.
///
/// Every arithmetic kernel shares this: the value loop is the operator's, and
/// the validity bitmap and buffer are built once, here (Rule 38).
fn computed_column<F>(row_count: u32, mut value: F) -> Result<VectorRef>
where
    F: FnMut(u32) -> Result<Option<i64>>,
{
    let mut values = Vec::with_capacity(row_count as usize);
    let mut valid = vec![0u64; words_for(row_count)];
    let mut nullable = false;
    for row in 0..row_count {
        match value(row)? {
            Some(value) => {
                values.push(value);
                valid[row as usize / 64] |= 1u64 << (row % 64);
            }
            None => {
                nullable = true;
                values.push(0);
            }
        }
    }
    let vector = Vector::from_i64(&values);
    Ok(Arc::new(if nullable {
        vector.with_validity(Validity::from_bitmap(valid))
    } else {
        vector
    }))
}

/// One side of a comparison, resolved by encoding once.
///
/// The encoding and the validity are resolved here, before the row loop, so the
/// inner loop indexes a slice instead of re-matching the encoding per row
/// (Rule 4). `02-execution-and-vir-lld.md` §6 states that shape.
enum Operand<'a> {
    /// A constant, or the absence of one for a null or non-integer literal.
    Scalar(Option<i64>),
    /// A borrowed column with its values and validity resolved.
    Column {
        /// The resolved values.
        values: I64Values<'a>,
        /// Which rows are non-null.
        validity: &'a Validity,
    },
}

/// The value of `operand` at `row`, or `None` when the row is unknown.
fn value_at(operand: &Operand<'_>, row: u32) -> Option<i64> {
    match operand {
        Operand::Scalar(value) => *value,
        Operand::Column { values, validity } => {
            if validity.is_valid(row) {
                values.at(row)
            } else {
                None
            }
        }
    }
}

/// Resolves a register as an operand.
///
/// The view borrows the register file, which borrows the chunk, so an operand
/// may read either a mounted column or one the program produced (ADR 0004).
fn operand<'f>(file: &'f RegisterFile<'_>, register: RegId) -> Result<Operand<'f>> {
    if let Some(value) = file.scalar(register) {
        return Ok(Operand::Scalar(match value {
            LogicalLiteral::Int64(value) => Some(*value),
            _ => None,
        }));
    }
    if let Some(column) = file.column(register) {
        let view = column.as_view();
        let values = view.resolve_i64().ok_or_else(|| {
            VeinError::internal(format!(
                "R{register} holds {:?}, which this VM cannot read as Int64",
                view.data_type()
            ))
        })?;
        return Ok(Operand::Column {
            values,
            validity: view.validity(),
        });
    }
    Err(VeinError::internal(format!(
        "R{register} does not hold a scalar or a column"
    )))
}

/// Compares two operands row by row.
///
/// The operand shape is matched once, outside the loop, and the operator is a
/// type parameter, so each comparison compiles to its own inlined loop. A flat
/// column against an integer literal — the shape a predicate actually hits —
/// indexes the slice directly; every other shape falls back to the encoding's
/// own accessor. A null on either side leaves the bit unknown rather than false,
/// so a later `NOT` cannot turn it into a match.
fn compare<C: Comparison>(
    left: &Operand<'_>,
    right: &Operand<'_>,
    row_count: u32,
) -> BitMask {
    // A null (or non-integer) literal on either side leaves every row unknown.
    if matches!(left, Operand::Scalar(None)) || matches!(right, Operand::Scalar(None)) {
        return unknown_mask(row_count);
    }
    match (left, right) {
        (Operand::Scalar(Some(left)), Operand::Scalar(Some(right))) => {
            constant_mask(row_count, C::test(*left, *right))
        }
        (
            Operand::Column {
                values: I64Values::Flat(values),
                validity,
            },
            Operand::Scalar(Some(scalar)),
        ) => flat_scalar::<C>(values, validity, *scalar, row_count, false),
        (
            Operand::Scalar(Some(scalar)),
            Operand::Column {
                values: I64Values::Flat(values),
                validity,
            },
        ) => flat_scalar::<C>(values, validity, *scalar, row_count, true),
        _ => compare_encoded::<C>(left, right, row_count),
    }
}

/// `flat column <op> scalar`, written word by word.
///
/// `scalar_on_left` selects the operand order without a second loop. Rows past
/// the buffer stay unknown rather than being read, so a short buffer fails
/// closed the way [`I64Values::at`] does.
fn flat_scalar<C: Comparison>(
    values: &[i64],
    validity: &Validity,
    scalar: i64,
    row_count: u32,
    scalar_on_left: bool,
) -> BitMask {
    let rows = (row_count as usize).min(values.len());
    let mut bits = vec![0u64; words_for(row_count)];
    let mut known = vec![0u64; words_for(row_count)];
    // A column with no nulls skips the validity test entirely. The flag is
    // loop-invariant, so the branch disappears from the specialised loop.
    let may_be_null = validity.has_nulls();
    for (index, word) in bits.iter_mut().enumerate() {
        let base = index * 64;
        if base >= rows {
            break;
        }
        let end = (base + 64).min(rows);
        let mut set = 0u64;
        let mut valid = 0u64;
        for row in base..end {
            if may_be_null && !validity.is_valid(row as u32) {
                continue;
            }
            valid |= 1u64 << (row - base);
            let value = values[row];
            let matches = if scalar_on_left {
                C::test(scalar, value)
            } else {
                C::test(value, scalar)
            };
            if matches {
                set |= 1u64 << (row - base);
            }
        }
        *word = set;
        known[index] = valid;
    }
    BitMask {
        values: bits.into(),
        valid: known.into(),
    }
}

/// The general path: any encoding, read through [`I64Values::at`].
fn compare_encoded<C: Comparison>(
    left: &Operand<'_>,
    right: &Operand<'_>,
    row_count: u32,
) -> BitMask {
    let mut values = vec![0u64; words_for(row_count)];
    let mut valid = vec![0u64; words_for(row_count)];
    for row in 0..row_count {
        let bit = 1u64 << (row % 64);
        let (Some(left), Some(right)) = (value_at(left, row), value_at(right, row)) else {
            continue;
        };
        valid[row as usize / 64] |= bit;
        if C::test(left, right) {
            values[row as usize / 64] |= bit;
        }
    }
    BitMask {
        values: values.into(),
        valid: valid.into(),
    }
}

/// A condition that is the same for every row: the comparison of two constants.
fn constant_mask(row_count: u32, matches: bool) -> BitMask {
    let mut values = vec![if matches { u64::MAX } else { 0 }; words_for(row_count)];
    trim_words(&mut values, row_count);
    BitMask {
        values: values.into(),
        valid: vec![u64::MAX; words_for(row_count)].into(),
    }
}

/// A condition no row is known to satisfy.
fn unknown_mask(row_count: u32) -> BitMask {
    let words = words_for(row_count);
    BitMask {
        values: vec![0u64; words].into(),
        valid: vec![0u64; words].into(),
    }
}

/// Whether each row is null. A null test itself always has an answer.
fn null_condition<'a>(file: &RegisterFile<'a>, register: RegId, row_count: u32) -> Result<BitMask> {
    let mut values = vec![0u64; words_for(row_count)];
    if let Some(value) = file.scalar(register) {
        if matches!(value, LogicalLiteral::Null(_)) {
            values.fill(u64::MAX);
        }
    } else if let Some(column) = file.column(register) {
        let view = column.as_view();
        for row in 0..row_count {
            if !view.is_valid(row) {
                values[row as usize / 64] |= 1 << (row % 64);
            }
        }
    } else {
        return Err(VeinError::internal(format!(
            "R{register} does not hold a scalar or a column"
        )));
    }
    trim_words(&mut values, row_count);
    Ok(BitMask {
        values: values.into(),
        valid: vec![u64::MAX; words_for(row_count)].into(),
    })
}

/// Borrows a mask register.
fn condition(file: &RegisterFile, register: RegId) -> Result<BitMask> {
    file.mask(register)
        .cloned()
        .ok_or_else(|| VeinError::internal(format!("R{register} does not hold a condition")))
}

/// Combines two masks word by word, trimming to `row_count`.
fn zip_words(left: &[u64], right: &[u64], row_count: u32, op: fn(u64, u64) -> u64) -> std::sync::Arc<[u64]> {
    let mut words = vec![0u64; words_for(row_count)];
    for (index, word) in words.iter_mut().enumerate() {
        let a = left.get(index).copied().unwrap_or(0);
        let b = right.get(index).copied().unwrap_or(0);
        *word = op(a, b);
    }
    trim_words(&mut words, row_count);
    words.into()
}

/// Applies a bitwise function to one mask, trimming to `row_count`.
fn map_words(mask: &[u64], row_count: u32, op: fn(u64) -> u64) -> std::sync::Arc<[u64]> {
    let mut words = vec![0u64; words_for(row_count)];
    for (index, word) in words.iter_mut().enumerate() {
        *word = op(mask.get(index).copied().unwrap_or(0));
    }
    trim_words(&mut words, row_count);
    words.into()
}

fn absent(register: RegId) -> VeinError {
    VeinError::internal(format!("R{register} is not present in the register file"))
}

/// A predicate whose condition is a VIR program.
pub struct ProgramPredicate {
    program: Arc<MicroProgram>,
    result: RegId,
}

impl ProgramPredicate {
    /// Wraps `program`, reading the condition from register `result`.
    pub fn new(program: Arc<MicroProgram>, result: RegId) -> Self {
        Self { program, result }
    }
}

impl Predicate for ProgramPredicate {
    fn evaluate(&self, chunk: &DataChunk) -> Result<BitMask> {
        evaluate_condition(&self.program, chunk, self.result)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use vector::{Encoding, Selection, Validity, Vector, VectorRef};

    use super::*;

    /// A one-column chunk, optionally with nulls (`true` marks a valid row).
    fn chunk_of(values: &[i64], valid: Option<&[bool]>) -> DataChunk {
        let column: VectorRef = Arc::new(Vector::from_i64(values));
        let column = match valid {
            Some(valid) => {
                let mut words = vec![0u64; values.len().div_ceil(64)];
                for (row, keep) in valid.iter().enumerate() {
                    if *keep {
                        words[row / 64] |= 1 << (row % 64);
                    }
                }
                column
                    .as_ref()
                    .clone()
                    .with_validity(Validity::from_bitmap(words))
            }
            None => column.as_ref().clone(),
        };
        DataChunk::new(values.len() as u32, vec![Arc::new(column)], Selection::all())
            .expect("consistent")
    }

    fn program(register_count: RegId, instructions: Vec<MicroOp>) -> MicroProgram {
        MicroProgram {
            register_count,
            instructions,
        }
    }

    /// Rows a filter would keep for `condition`.
    fn kept(condition: &BitMask, row_count: u32) -> Vec<u32> {
        let keep = condition.clone().into_keep(row_count);
        Selection::from_mask(keep.to_vec())
            .iter(row_count)
            .collect()
    }

    /// `column 0 > 20`.
    fn greater_than_20() -> MicroProgram {
        program(
            3,
            vec![
                MicroOp::LoadConstant {
                    dst: 1,
                    value: LogicalLiteral::Int64(20),
                },
                MicroOp::CmpGt {
                    dst: 2,
                    src1: 0,
                    src2: 1,
                },
            ],
        )
    }

    #[test]
    fn a_comparison_selects_the_matching_rows() {
        let chunk = chunk_of(&[10, 20, 30, 40], None);
        let condition = evaluate_condition(&greater_than_20(), &chunk, 2).expect("evaluated");
        assert_eq!(kept(&condition, 4), vec![2, 3]);
    }

    /// A null is unknown, not false: it must not satisfy the predicate.
    #[test]
    fn a_null_row_is_unknown_rather_than_false() {
        let chunk = chunk_of(&[10, 20, 30], Some(&[true, false, true]));
        let condition = evaluate_condition(&greater_than_20(), &chunk, 2).expect("evaluated");
        assert_eq!(kept(&condition, 3), vec![2]);
    }

    /// The regression this representation exists for: `NOT` must not turn an
    /// unknown into a match, or `WHERE NOT (x > 20)` would keep null rows.
    #[test]
    fn not_does_not_promote_unknown_to_true() {
        let chunk = chunk_of(&[10, 20, 30], Some(&[true, false, true]));
        let program = program(
            4,
            vec![
                MicroOp::LoadConstant {
                    dst: 1,
                    value: LogicalLiteral::Int64(20),
                },
                MicroOp::CmpGt {
                    dst: 2,
                    src1: 0,
                    src2: 1,
                },
                MicroOp::Not { dst: 3, src: 2 },
            ],
        );
        let condition = evaluate_condition(&program, &chunk, 3).expect("evaluated");
        // Row 0 is `10 > 20` = false, so NOT is true and it survives. Row 1 is
        // null and must not, even though the inverted bit is set.
        assert_eq!(kept(&condition, 3), vec![0]);
    }

    #[test]
    fn and_requires_both_sides() {
        let chunk = chunk_of(&[10, 20, 30, 40, 50], None);
        let program = program(
            6,
            vec![
                MicroOp::LoadConstant {
                    dst: 2,
                    value: LogicalLiteral::Int64(10),
                },
                MicroOp::CmpGt {
                    dst: 3,
                    src1: 0,
                    src2: 2,
                },
                MicroOp::LoadConstant {
                    dst: 4,
                    value: LogicalLiteral::Int64(40),
                },
                MicroOp::CmpLt {
                    dst: 5,
                    src1: 0,
                    src2: 4,
                },
                MicroOp::And {
                    dst: 1,
                    src1: 3,
                    src2: 5,
                },
            ],
        );
        let condition = evaluate_condition(&program, &chunk, 1).expect("evaluated");
        assert_eq!(kept(&condition, 5), vec![1, 2]);
    }

    #[test]
    fn or_is_true_when_either_side_is() {
        let chunk = chunk_of(&[10, 30, 50], None);
        let program = program(
            6,
            vec![
                MicroOp::LoadConstant {
                    dst: 2,
                    value: LogicalLiteral::Int64(40),
                },
                MicroOp::CmpGt {
                    dst: 3,
                    src1: 0,
                    src2: 2,
                },
                MicroOp::LoadConstant {
                    dst: 4,
                    value: LogicalLiteral::Int64(20),
                },
                MicroOp::CmpLt {
                    dst: 5,
                    src1: 0,
                    src2: 4,
                },
                MicroOp::Or {
                    dst: 1,
                    src1: 3,
                    src2: 5,
                },
            ],
        );
        let condition = evaluate_condition(&program, &chunk, 1).expect("evaluated");
        assert_eq!(kept(&condition, 3), vec![0, 2]);
    }

    #[test]
    fn null_tests_always_have_an_answer() {
        let chunk = chunk_of(&[10, 20, 30], Some(&[true, false, true]));

        let is_null = program(2, vec![MicroOp::IsNull { dst: 1, src: 0 }]);
        let condition = evaluate_condition(&is_null, &chunk, 1).expect("evaluated");
        assert_eq!(kept(&condition, 3), vec![1]);

        let not_null = program(2, vec![MicroOp::IsNotNull { dst: 1, src: 0 }]);
        let condition = evaluate_condition(&not_null, &chunk, 1).expect("evaluated");
        assert_eq!(kept(&condition, 3), vec![0, 2]);
    }

    #[test]
    fn an_unsupported_instruction_is_refused_by_name() {
        let chunk = chunk_of(&[1, 2], None);
        let program = program(
            3,
            vec![MicroOp::CallUdf {
                name: "missing".to_string(),
                arg_regs: vec![0],
                dst: 2,
            }],
        );
        let error =
            evaluate_condition(&program, &chunk, 2).expect_err("CallUdf is not implemented");
        assert!(
            error.to_string().contains("CallUdf"),
            "the error must name the instruction: {error}"
        );
    }

    /// `SELECT id + 1`: an integer expression becomes an owned column the
    /// projection can name.
    #[test]
    fn execute_chunk_projects_an_arithmetic_expression() {
        let ids: VectorRef = Arc::new(Vector::from_i64(&[10, 20, 30]));
        let chunk = DataChunk::new(3, vec![ids], Selection::all()).expect("consistent");
        let program = program(
            3,
            vec![
                MicroOp::LoadConstant {
                    dst: 1,
                    value: LogicalLiteral::Int64(1),
                },
                MicroOp::Add {
                    dst: 2,
                    src1: 0,
                    src2: 1,
                },
                MicroOp::ProjectRegisters { out_regs: vec![2] },
            ],
        );

        let out = execute_chunk(&program, chunk).expect("executed");
        let view = out.view(0).expect("column 0");
        let values = view.resolve_i64().expect("i64");
        let computed: Vec<i64> = view.rows().filter_map(|row| values.at(row)).collect();
        assert_eq!(computed, vec![11, 21, 31]);
    }

    /// A null operand makes the row null; it does not become zero.
    #[test]
    fn arithmetic_propagates_nulls() {
        let chunk = chunk_of(&[10, 20, 30], Some(&[true, false, true]));
        let program = program(
            3,
            vec![
                MicroOp::LoadConstant {
                    dst: 1,
                    value: LogicalLiteral::Int64(5),
                },
                MicroOp::Mul {
                    dst: 2,
                    src1: 0,
                    src2: 1,
                },
                MicroOp::ProjectRegisters { out_regs: vec![2] },
            ],
        );

        let out = execute_chunk(&program, chunk).expect("executed");
        let view = out.view(0).expect("column 0");
        let values = view.resolve_i64().expect("i64");
        let computed: Vec<Option<i64>> = (0..3)
            .map(|row| view.is_valid(row).then(|| values.at(row)).flatten())
            .collect();
        assert_eq!(computed, vec![Some(50), None, Some(150)]);
    }

    /// Overflow fails the query instead of wrapping silently.
    #[test]
    fn arithmetic_overflow_is_reported() {
        let chunk = chunk_of(&[i64::MAX, 1], None);
        let program = program(
            3,
            vec![
                MicroOp::LoadConstant {
                    dst: 1,
                    value: LogicalLiteral::Int64(1),
                },
                MicroOp::Add {
                    dst: 2,
                    src1: 0,
                    src2: 1,
                },
            ],
        );

        let error = execute_chunk(&program, chunk).expect_err("i64::MAX + 1 overflows");
        assert!(error.to_string().contains("Add"), "{error}");
    }

    /// Negation is reachable from a program and produces an owned column.
    #[test]
    fn execute_chunk_negates() {
        let chunk = chunk_of(&[1, -2, 3], None);
        let program = program(
            3,
            vec![
                MicroOp::Neg { dst: 1, src: 0 },
                MicroOp::ProjectRegisters { out_regs: vec![1] },
            ],
        );

        let out = execute_chunk(&program, chunk).expect("executed");
        let view = out.view(0).expect("column 0");
        let values = view.resolve_i64().expect("i64");
        let negated: Vec<i64> = view.rows().filter_map(|row| values.at(row)).collect();
        assert_eq!(negated, vec![-1, 2, -3]);
    }

    /// Every integer operator, against Rust's own semantics for the same rows.
    #[test]
    fn arithmetic_covers_every_operator() {
        let chunk = chunk_of(&[10, 20, 7], None);
        let program = program(
            7,
            vec![
                MicroOp::LoadConstant {
                    dst: 1,
                    value: LogicalLiteral::Int64(3),
                },
                MicroOp::Sub {
                    dst: 2,
                    src1: 0,
                    src2: 1,
                },
                MicroOp::Div {
                    dst: 3,
                    src1: 0,
                    src2: 1,
                },
                MicroOp::Mod {
                    dst: 4,
                    src1: 0,
                    src2: 1,
                },
                MicroOp::Mul {
                    dst: 5,
                    src1: 2,
                    src2: 1,
                },
                MicroOp::ProjectRegisters {
                    out_regs: vec![2, 3, 4, 5],
                },
            ],
        );

        let out = execute_chunk(&program, chunk).expect("executed");
        let read = |index: usize| -> Vec<i64> {
            let view = out.view(index).expect("column");
            let values = view.resolve_i64().expect("i64");
            view.rows().filter_map(|row| values.at(row)).collect()
        };
        assert_eq!(read(0), vec![7, 17, 4], "Sub");
        assert_eq!(read(1), vec![3, 6, 2], "Div");
        assert_eq!(read(2), vec![1, 2, 1], "Mod");
        assert_eq!(read(3), vec![21, 51, 12], "Mul of (Sub, literal)");
    }

    /// Division by zero is reported rather than producing a value.
    #[test]
    fn division_by_zero_is_reported() {
        let chunk = chunk_of(&[1, 2], None);
        let program = program(
            3,
            vec![
                MicroOp::LoadConstant {
                    dst: 1,
                    value: LogicalLiteral::Int64(0),
                },
                MicroOp::Div {
                    dst: 2,
                    src1: 0,
                    src2: 1,
                },
            ],
        );

        let error = execute_chunk(&program, chunk).expect_err("division by zero");
        assert!(error.to_string().contains("Div"), "{error}");
    }

    /// A condition projects as a boolean column, so `SELECT x > 5` is
    /// expressible instead of refused.
    #[test]
    fn execute_chunk_projects_a_condition() {
        let chunk = chunk_of(&[1, 9, 3, 8], None);
        let program = program(
            3,
            vec![
                MicroOp::LoadConstant {
                    dst: 1,
                    value: LogicalLiteral::Int64(5),
                },
                MicroOp::CmpGt {
                    dst: 2,
                    src1: 0,
                    src2: 1,
                },
                MicroOp::ProjectRegisters { out_regs: vec![2] },
            ],
        );

        let out = execute_chunk(&program, chunk).expect("executed");
        let column = out.column(0).expect("column 0");
        assert_eq!(*column.data_type(), VeinDataType::Boolean);
        let Encoding::Flat { values, .. } = column.encoding() else {
            panic!("expected a flat boolean");
        };
        assert_eq!(values.as_slice()[0], 0b0000_1010, "rows 1 and 3 are true");
    }

    /// A row whose condition is unknown projects as null, not as false.
    #[test]
    fn a_projected_condition_keeps_unknown_rows_null() {
        let chunk = chunk_of(&[1, 9], Some(&[true, false]));
        let program = program(
            3,
            vec![
                MicroOp::LoadConstant {
                    dst: 1,
                    value: LogicalLiteral::Int64(5),
                },
                MicroOp::CmpGt {
                    dst: 2,
                    src1: 0,
                    src2: 1,
                },
                MicroOp::ProjectRegisters { out_regs: vec![2] },
            ],
        );

        let out = execute_chunk(&program, chunk).expect("executed");
        let column = out.column(0).expect("column 0");
        assert!(column.is_valid(0), "1 > 5 is known false");
        assert!(!column.is_valid(1), "a null operand is unknown");
    }

    #[test]
    fn a_result_register_that_is_not_a_condition_is_refused() {
        let chunk = chunk_of(&[1, 2], None);
        let program = program(
            2,
            vec![MicroOp::LoadConstant {
                dst: 1,
                value: LogicalLiteral::Int64(5),
            }],
        );
        assert!(evaluate_condition(&program, &chunk, 1).is_err());
    }

    /// A whole planner-shaped program — expression, filter, projection — on the
    /// engine's own representation.
    #[test]
    fn execute_chunk_filters_and_projects() {
        let ids: VectorRef = Arc::new(Vector::from_i64(&[10, 20, 30, 40]));
        let labels: VectorRef = Arc::new(Vector::from_utf8(&["a", "b", "c", "d"]));
        let chunk = DataChunk::new(4, vec![ids, labels], Selection::all()).expect("consistent");
        let program = program(
            3,
            vec![
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
        );

        let out = execute_chunk(&program, chunk).expect("executed");
        assert_eq!(out.num_columns(), 1);
        assert_eq!(out.active_rows(), 2);
        let view = out.view(0).expect("column 0");
        let values = view.resolve_i64().expect("i64");
        let kept: Vec<i64> = view.rows().filter_map(|row| values.at(row)).collect();
        assert_eq!(kept, vec![30, 40]);
    }

    /// Projection shares the source column rather than copying it.
    #[test]
    fn execute_chunk_projects_by_handle() {
        let ids: VectorRef = Arc::new(Vector::from_i64(&[1, 2]));
        let chunk = DataChunk::new(2, vec![ids.clone()], Selection::all()).expect("consistent");
        let program = program(1, vec![MicroOp::ProjectRegisters { out_regs: vec![0] }]);

        let out = execute_chunk(&program, chunk).expect("executed");
        assert!(Arc::ptr_eq(out.column(0).expect("column 0"), &ids));
    }

    /// A literal register projects as a constant column, which stays constant
    /// instead of expanding to one buffer per row.
    #[test]
    fn execute_chunk_materialises_a_literal_projection() {
        let chunk = chunk_of(&[1, 2, 3], None);
        let program = program(
            2,
            vec![
                MicroOp::LoadConstant {
                    dst: 1,
                    value: LogicalLiteral::Int64(7),
                },
                MicroOp::ProjectRegisters { out_regs: vec![1] },
            ],
        );

        let out = execute_chunk(&program, chunk).expect("executed");
        assert_eq!(out.num_columns(), 1);
        assert_eq!(out.active_rows(), 3);
        let column = out.column(0).expect("column 0");
        assert!(
            matches!(column.encoding(), Encoding::Constant(_)),
            "a literal must not be expanded into a buffer"
        );
        assert_eq!(
            out.view(0).and_then(|view| view.resolve_i64()).and_then(|values| values.at(2)),
            Some(7)
        );
    }

    /// A literal the engine representation cannot carry is refused by name.
    #[test]
    fn execute_chunk_refuses_a_literal_it_cannot_project() {
        let chunk = chunk_of(&[1, 2], None);
        let program = program(
            2,
            vec![
                MicroOp::LoadConstant {
                    dst: 1,
                    value: LogicalLiteral::Float64(1.0),
                },
                MicroOp::ProjectRegisters { out_regs: vec![1] },
            ],
        );

        let error = execute_chunk(&program, chunk).expect_err("Float64 is not a column type yet");
        assert!(error.to_string().contains("Float64"), "{error}");
    }

    /// A `CopyRegister` alias still names the column it views, matched by buffer
    /// identity rather than by register number.
    #[test]
    fn execute_chunk_projects_an_aliased_column() {
        let ids: VectorRef = Arc::new(Vector::from_i64(&[1, 2]));
        let chunk = DataChunk::new(2, vec![ids.clone()], Selection::all()).expect("consistent");
        let program = program(
            2,
            vec![
                MicroOp::CopyRegister { dst: 1, src: 0 },
                MicroOp::ProjectRegisters { out_regs: vec![1] },
            ],
        );

        let out = execute_chunk(&program, chunk).expect("executed");
        assert!(Arc::ptr_eq(out.column(0).expect("column 0"), &ids));
    }
}
