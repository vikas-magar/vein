use std::sync::Arc;

use common::Result;
use common::protocol::JoinOutput;
use error::VeinError;
use plan::schema::VeinSchema;

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
        // Bound identities are authoritative. Name fallback can select a
        // same-named column from the opposite side of a join.
        let probe_index = probe.position_of_id(field.id);
        if let Some(index) = probe_index {
            let index = u32::try_from(index)
                .map_err(|_| VeinError::planning("join probe input has too many columns", None))?;
            outputs.push(JoinOutput::Probe(index));
            fields.push(Arc::clone(field));
            continue;
        }
        let build_index = build.position_of_id(field.id);
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
