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
use types::LogicalLiteral;
use vector::{DataChunk, I64Values, Vector, VectorView, trim_words, words_for};

use super::Predicate;
use super::register::{BitMask, RegisterFile, RegisterSlot};

/// Evaluates `program` over `chunk` and returns the condition left in `result`.
pub fn evaluate_condition(
    program: &MicroProgram,
    chunk: &DataChunk,
    result: RegId,
) -> Result<BitMask> {
    let row_count = chunk.row_count();
    let mut file = RegisterFile::new(program.register_count as usize);
    file.mount(chunk)?;
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
        MicroOp::CmpEq { dst, src1, src2 }
        | MicroOp::CmpNeq { dst, src1, src2 }
        | MicroOp::CmpLt { dst, src1, src2 }
        | MicroOp::CmpLte { dst, src1, src2 }
        | MicroOp::CmpGt { dst, src1, src2 }
        | MicroOp::CmpGte { dst, src1, src2 } => {
            let left = operand(file, *src1)?;
            let right = operand(file, *src2)?;
            let mask = compare(&left, &right, row_count, comparison(instruction));
            file.set(*dst, RegisterSlot::Mask { mask })
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

/// Which comparison an instruction denotes.
fn comparison(instruction: &MicroOp) -> fn(i64, i64) -> bool {
    match instruction {
        MicroOp::CmpEq { .. } => |left, right| left == right,
        MicroOp::CmpNeq { .. } => |left, right| left != right,
        MicroOp::CmpLt { .. } => |left, right| left < right,
        MicroOp::CmpLte { .. } => |left, right| left <= right,
        MicroOp::CmpGt { .. } => |left, right| left > right,
        _ => |left, right| left >= right,
    }
}

/// One side of an operation, resolved by encoding once.
enum Operand<'a> {
    /// A constant, or the absence of one for a null or non-integer literal.
    Scalar(Option<i64>),
    /// A column with its values already resolved.
    Column {
        /// The column, needed for its validity.
        vector: &'a Vector,
        /// The resolved values.
        values: I64Values<'a>,
    },
}

impl Operand<'_> {
    /// The value at `row`, or `None` when the row is null.
    fn at(&self, row: u32) -> Option<i64> {
        match self {
            Operand::Scalar(value) => *value,
            Operand::Column { vector, values } => {
                if vector.is_valid(row) {
                    values.at(row)
                } else {
                    None
                }
            }
        }
    }
}

/// Resolves a register as an operand.
fn operand(file: &RegisterFile, register: RegId) -> Result<Operand<'_>> {
    if let Some(value) = file.scalar(register) {
        return Ok(Operand::Scalar(match value {
            LogicalLiteral::Int64(value) => Some(*value),
            _ => None,
        }));
    }
    if let Some(vector) = file.vector(register) {
        let column: &Vector = vector.as_ref();
        let values = VectorView::all(column).resolve_i64().ok_or_else(|| {
            VeinError::internal(format!(
                "R{register} holds {:?}, which this VM cannot read as Int64",
                column.data_type()
            ))
        })?;
        return Ok(Operand::Column {
            vector: column,
            values,
        });
    }
    Err(VeinError::internal(format!(
        "R{register} does not hold a scalar or a column"
    )))
}

/// Compares two operands row by row.
///
/// A null on either side leaves the bit unknown rather than false, so a later
/// `NOT` cannot turn it into a match.
fn compare(
    left: &Operand<'_>,
    right: &Operand<'_>,
    row_count: u32,
    test: fn(i64, i64) -> bool,
) -> BitMask {
    let mut values = vec![0u64; words_for(row_count)];
    let mut valid = vec![0u64; words_for(row_count)];
    for row in 0..row_count {
        let bit = 1u64 << (row % 64);
        let (Some(left), Some(right)) = (left.at(row), right.at(row)) else {
            continue;
        };
        valid[row as usize / 64] |= bit;
        if test(left, right) {
            values[row as usize / 64] |= bit;
        }
    }
    BitMask {
        values: values.into(),
        valid: valid.into(),
    }
}

/// Whether each row is null. A null test itself always has an answer.
fn null_condition(file: &RegisterFile, register: RegId, row_count: u32) -> Result<BitMask> {
    let mut values = vec![0u64; words_for(row_count)];
    if let Some(value) = file.scalar(register) {
        if matches!(value, LogicalLiteral::Null(_)) {
            values.fill(u64::MAX);
        }
    } else if let Some(vector) = file.vector(register) {
        for row in 0..row_count {
            if vector.is_null(row) {
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

    use vector::{Selection, Validity, VectorRef};

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

    fn program(register_count: u8, instructions: Vec<MicroOp>) -> MicroProgram {
        MicroProgram {
            register_count,
            instructions,
        }
    }

    /// Rows a filter would keep for `condition`.
    fn kept(condition: &BitMask, row_count: u32) -> Vec<u32> {
        let keep = condition.keep(row_count);
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
            vec![MicroOp::Add {
                dst: 2,
                src1: 0,
                src2: 0,
            }],
        );
        let error = evaluate_condition(&program, &chunk, 2).expect_err("Add is not implemented");
        assert!(
            error.to_string().contains("Add"),
            "the error must name the instruction: {error}"
        );
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
}
