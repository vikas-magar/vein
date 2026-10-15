use std::sync::Arc;

use common::Result;
use common::protocol::JoinOutput;
use error::VeinError;
use plan::schema::VeinSchema;
use plan::LogicalExpr;

pub(super) fn position(schema: &VeinSchema, name: &str) -> Result<u32> {
    schema
        .position_by_name(name)
        .map(|index| index as u32)
        .ok_or_else(|| {
            VeinError::planning(
                format!("join key column '{name}' is not in the input"),
                None,
            )
        })
}

/// Maps every declared join output to its physical input position.
/// Probe-first preserves `USING` semantics. Column pruning narrows the logical
/// join schema to parent-required outputs, so a missing declared field violates
/// the plan contract and must fail here rather than disappear silently.
pub(super) fn available_join_outputs(
    schema: &VeinSchema,
    probe: &VeinSchema,
    build: &VeinSchema,
) -> Result<(Vec<JoinOutput>, VeinSchema)> {
    let mut outputs = Vec::with_capacity(schema.fields.len());
    let mut fields = Vec::with_capacity(schema.fields.len());
    for field in &schema.fields {
        let probe_index = probe.position_of_id(field.id).or_else(|| {
            probe.fields.iter().position(|candidate| {
                candidate.name == field.name && candidate.qualifier == field.qualifier
            })
        });
        if let Some(index) = probe_index {
            let index = u32::try_from(index)
                .map_err(|_| VeinError::planning("join probe input has too many columns", None))?;
            outputs.push(JoinOutput::Probe(index));
            fields.push(Arc::clone(field));
            continue;
        }
        let build_index = build.position_of_id(field.id).or_else(|| {
            build.fields.iter().position(|candidate| {
                candidate.name == field.name && candidate.qualifier == field.qualifier
            })
        });
        if let Some(index) = build_index {
            let index = u32::try_from(index)
                .map_err(|_| VeinError::planning("join build input has too many columns", None))?;
            outputs.push(JoinOutput::Build(index));
            fields.push(Arc::clone(field));
            continue;
        }

        return Err(VeinError::planning(
            format!(
                "join output column '{}' (ColumnId({})) is missing from both physical inputs",
                field.name, field.id.0
            ),
            None,
        ));
    }
    Ok((outputs, VeinSchema::new(fields)))
}

/// The output layout: each field of the join's schema, probe side matched first.
///
/// Probe-first is what makes a `USING` key appear once — the binder leaves it on
/// the left — rather than twice.
#[allow(dead_code)]
pub(super) fn join_outputs(
    exprs: Option<&[LogicalExpr]>,
    schema: &VeinSchema,
    probe: &VeinSchema,
    build: &VeinSchema,
) -> Result<Vec<JoinOutput>> {
    let Some(exprs) = exprs else {
        return schema
            .fields
            .iter()
            .map(|field| {
                if let Some(index) = probe.position_of_id(field.id) {
                    Ok(JoinOutput::Probe(index as u32))
                } else if let Some(index) = build.position_of_id(field.id) {
                    Ok(JoinOutput::Build(index as u32))
                } else if let Some(index) = side_of(probe, field) {
                    Ok(JoinOutput::Probe(index as u32))
                } else if let Some(index) = side_of(build, field) {
                    Ok(JoinOutput::Build(index as u32))
                } else {
                    Err(VeinError::planning(
                        format!("join output column {} comes from neither input", field.name),
                        None,
                    ))
                }
            })
            .collect();
    };

    // A projection above the join: its expressions name the source columns, so
    // they resolve by id — the only thing that separates a self-join's two
    // columns of the same name.
    exprs
        .iter()
        .map(|expr| {
            let LogicalExpr::Column(column) = expr else {
                return Err(VeinError::unsupported(
                    "a projection above a join must select plain columns: an expression \
                     needs a program after the join, which a task does not carry yet",
                    None,
                ));
            };
            if let Some(index) = probe.position_of_id(column.id) {
                Ok(JoinOutput::Probe(index as u32))
            } else if let Some(index) = build.position_of_id(column.id) {
                Ok(JoinOutput::Build(index as u32))
            } else {
                Err(VeinError::planning(
                    format!(
                        "join output column {} comes from neither input",
                        column.name
                    ),
                    None,
                ))
            }
        })
        .collect()
}

/// The position of `field` in `schema`, matched by name and qualifier.
#[allow(dead_code)]
fn side_of(schema: &VeinSchema, field: &plan::schema::ResolvedColumnRef) -> Option<usize> {
    schema.fields.iter().position(|candidate| {
        candidate.name == field.name && candidate.qualifier == field.qualifier
    })
}
