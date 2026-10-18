use common::Result;
use common::protocol::JoinKind;
use error::VeinError;
use parser::ast::expr::BinaryOp;
use parser::ast::pipe_op::JoinType;
use plan::schema::VeinSchema;
use plan::LogicalExpr;

type JoinKeyOrdinals = (Vec<u32>, Vec<u32>);

pub(super) fn flatten_conjunction<'a>(
    expression: &'a LogicalExpr,
    output: &mut Vec<&'a LogicalExpr>,
) {
    if let LogicalExpr::BinaryOp {
        left,
        op: BinaryOp::And,
        right,
        ..
    } = expression
    {
        flatten_conjunction(left, output);
        flatten_conjunction(right, output);
    } else {
        output.push(expression);
    }
}

fn is_column_equality(expression: &LogicalExpr) -> bool {
    matches!(
        expression,
        LogicalExpr::BinaryOp {
            left,
            op: BinaryOp::Eq,
            right,
            ..
        } if matches!(left.as_ref(), LogicalExpr::Column(_))
            && matches!(right.as_ref(), LogicalExpr::Column(_))
    )
}

/// Extract all supported direct-column equalities that compare probe and build
/// inputs. Other conjuncts remain residual predicates, so same-side equalities,
/// expression equalities, and non-equality conditions still participate in the
/// complete `ON` test. A hash join requires at least one extracted key; joins
/// with only residual conditions need a different physical algorithm.
pub(super) fn extract_equi_join_keys(
    conjuncts: &[&LogicalExpr],
    probe: &VeinSchema,
    build: &VeinSchema,
) -> Result<(JoinKeyOrdinals, Vec<LogicalExpr>)> {
    let mut probe_keys = Vec::new();
    let mut build_keys = Vec::new();
    let mut residual = Vec::new();

    for conjunct in conjuncts {
        if is_column_equality(conjunct)
            && let Ok((probe_key, build_key)) = equality_keys(Some(conjunct), probe, build) {
                let probe_type = probe.fields[probe_key as usize].data_type.hash_join_key_type();
                let build_type = build.fields[build_key as usize].data_type.hash_join_key_type();
                if probe_type.is_some() && probe_type == build_type {
                    probe_keys.push(probe_key);
                    build_keys.push(build_key);
                    continue;
                }
            }
        residual.push((*conjunct).clone());
    }

    if probe_keys.is_empty() {
        return Err(VeinError::unsupported(
            "hash join requires at least one equality between direct columns from opposite inputs with matching supported hash-key types; expression-only and non-equi joins are not supported",
            None,
        ));
    }

    Ok(((probe_keys, build_keys), residual))
}

/// Maps each parser join flavor to its physical-plan counterpart.
pub(super) fn join_kind(join_type: &JoinType) -> JoinKind {
    match join_type {
        JoinType::Inner => JoinKind::Inner,
        JoinType::Left => JoinKind::Left,
        JoinType::Right => JoinKind::Right,
        JoinType::Full => JoinKind::Full,
        JoinType::Semi => JoinKind::Semi,
        JoinType::Anti => JoinKind::Anti,
        JoinType::NullAwareAnti => JoinKind::NullAwareAnti,
        JoinType::Single => JoinKind::Single,
        JoinType::Cross => JoinKind::Cross,
    }
}

/// The two key columns of an equality `ON`.
///
/// Resolved by column id, never by name: only the id distinguishes `a.id` from
/// `b.id` in the self-join `ON a.id = b.id`, and resolving both sides against the
/// same positions would compare the wrong columns and return wrong rows.
pub(super) fn equality_keys(
    on: Option<&LogicalExpr>,
    probe: &VeinSchema,
    build: &VeinSchema,
) -> Result<(u32, u32)> {
    let Some(LogicalExpr::BinaryOp {
                 left,
                 right,
                 op: BinaryOp::Eq,
                 ..
             }) = on
    else {
        return Err(VeinError::planning(
            "a join needs a single equality ON predicate, or USING columns",
            None,
        ));
    };
    let (LogicalExpr::Column(left), LogicalExpr::Column(right)) = (left.as_ref(), right.as_ref())
    else {
        return Err(VeinError::unsupported(
            "join keys must be columns; expressions and constant keys are not supported yet",
            None,
        ));
    };
    let left_probe = probe.position_of_id(left.id);
    let left_build = build.position_of_id(left.id);
    let right_probe = probe.position_of_id(right.id);
    let right_build = build.position_of_id(right.id);

    match ((left_probe, left_build), (right_probe, right_build)) {
        ((Some(probe_key), None), (None, Some(build_key)))
        | ((None, Some(build_key)), (Some(probe_key), None)) => {
            Ok((probe_key as u32, build_key as u32))
        }
        _ => {
            let describe_schema = |schema: &VeinSchema| {
                schema
                    .fields
                    .iter()
                    .map(|field| format!("{}#{}", field.name, field.id.0))
                    .collect::<Vec<_>>()
                    .join(", ")
            };
            Err(VeinError::planning(
                format!(
                    "join keys '{}#{}' and '{}#{}' must resolve to one column from each input; \
                     check ON or USING against probe schema [{}] and build schema [{}]",
                    left.name,
                    left.id.0,
                    right.name,
                    right.id.0,
                    describe_schema(probe),
                    describe_schema(build),
                ),
                None,
            ))
        }
    }
}
