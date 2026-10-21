//! Schema projection and field-to-Parquet column mapping.

use std::sync::Arc;

use exec::runtime::filter::RuntimeFilterSet;
use arrow::datatypes::{DataType as ArrowDataType, Schema, SchemaRef};
use common::Result;
use common::protocol::{ARROW_DICTIONARY_ENCODING, ColumnEncodingHint, ScanPredicate};
use error::VeinError;
use types::{ResolvedColumnRef, VeinDataType, VeinSchema};

/// Reader columns and output positions for one projected scan batch.
pub(super) struct ScanProjection {
    /// Union of output and filter roots, retained for scan diagnostics.
    pub(super) read_roots: Vec<usize>,
    /// Parquet roots needed in the final output batch, in source order.
    pub(super) output_roots: Vec<usize>,
    /// Output columns' positions in the source-ordered output projection.
    pub(super) output_positions: Vec<usize>,
}

/// Selects the dictionary encoding hints understood by the Parquet provider.
pub(super) fn dictionary_column_indices(hints: &[ColumnEncodingHint]) -> Vec<u32> {
    hints
        .iter()
        .filter(|hint| hint.encoding.as_str() == ARROW_DICTIONARY_ENCODING)
        .map(|hint| hint.column_index)
        .collect()
}

/// Resolves narrow predicate inputs separately from the final output projection.
pub(super) fn scan_projection(
    arrow_schema: &Schema,
    projected: &VeinSchema,
    pushdown: Option<&ScanPredicate>,
    runtime_filters: &RuntimeFilterSet,
    path: &str,
) -> Result<ScanProjection> {
    let mut fields = projected.fields.clone();
    if let Some(pushdown) = pushdown {
        for field in pushdown.stages.iter().flat_map(|stage| &stage.input.fields) {
            add_unique_field(&mut fields, field);
        }
    }
    if let Some(binding) = runtime_filters.bind(projected) {
        for field in binding.columns() {
            add_unique_field(&mut fields, field);
        }
    }

    let mut roots = fields
        .iter()
        .map(|field| {
            let index = root_indices(arrow_schema, std::slice::from_ref(field), path)?;
            Ok((index[0], field.name.as_str()))
        })
        .collect::<Result<Vec<_>>>()?;
    roots.sort_unstable_by_key(|(index, _)| *index);
    let read_roots = roots.iter().map(|(index, _)| *index).collect::<Vec<_>>();
    let projected_roots = projected
        .fields
        .iter()
        .map(|field| root_indices(arrow_schema, std::slice::from_ref(field), path).map(|roots| roots[0]))
        .collect::<Result<Vec<_>>>()?;
    let mut output_roots = projected_roots.clone();
    output_roots.sort_unstable();
    output_roots.dedup();
    let output_positions = projected_roots
        .iter()
        .map(|root| output_roots.binary_search(root).map_err(|_| {
            VeinError::internal("output column is absent from its Parquet projection")
        }))
        .collect::<Result<Vec<_>>>()?;
    Ok(ScanProjection { read_roots, output_roots, output_positions })
}

fn add_unique_field(fields: &mut Vec<ResolvedColumnRef>, field: &ResolvedColumnRef) {
    if !fields.iter().any(|existing| existing.name == field.name) {
        fields.push(Arc::clone(field));
    }
}

pub(super) fn dictionary_schema_hint(
    parquet_schema: &SchemaRef,
    projected: &VeinSchema,
    dictionary_columns: &[u32],
) -> Result<SchemaRef> {
    let mut dictionary_names = Vec::with_capacity(dictionary_columns.len());
    for &index in dictionary_columns {
        let column = projected.fields.get(index as usize).ok_or_else(|| {
            VeinError::internal(format!(
                "dictionary column index {index} is outside the projected schema"
            ))
        })?;
        if column.data_type != VeinDataType::Utf8 {
            return Err(VeinError::internal(format!(
                "dictionary encoding is only supported for Utf8 grouping columns, got {:?}",
                column.data_type
            )));
        }
        dictionary_names.push(column.name.as_str());
    }

    let dictionary_type = ArrowDataType::Dictionary(
        Box::new(ArrowDataType::Int32),
        Box::new(ArrowDataType::Utf8),
    );
    let fields = parquet_schema
        .fields()
        .iter()
        .map(|field| {
            if dictionary_names.contains(&field.name().as_str()) {
                Arc::new(
                    field
                        .as_ref()
                        .clone()
                        .with_data_type(dictionary_type.clone()),
                )
            } else {
                Arc::clone(field)
            }
        })
        .collect::<Vec<_>>();
    for name in dictionary_names {
        if !parquet_schema
            .fields()
            .iter()
            .any(|field| field.name() == name)
        {
            return Err(VeinError::internal(format!(
                "dictionary projected column '{name}' is missing from the Parquet schema"
            )));
        }
    }
    Ok(Arc::new(Schema::new_with_metadata(
        fields,
        parquet_schema.metadata().clone(),
    )))
}

/// Maps `fields` onto their Parquet root column indices, in source order.
///
/// Both the output projection and a push-down predicate address columns the same
/// way, so both go through here: a column the file does not carry, one that is
/// ambiguous, or a set that is not in source order is a planning bug, not a
/// partial read.
pub(super) fn root_indices(
    arrow_schema: &arrow::datatypes::Schema,
    fields: &[ResolvedColumnRef],
    path: &str,
) -> Result<Vec<usize>> {
    let mut root_indices = Vec::with_capacity(fields.len());
    for field in fields {
        let mut matches = arrow_schema
            .fields()
            .iter()
            .enumerate()
            .filter(|(_, candidate)| candidate.name() == &field.name);
        let Some((index, _)) = matches.next() else {
            return Err(VeinError::internal(format!(
                "projected Parquet column '{}' is missing from '{}'",
                field.name, path
            )));
        };
        if matches.next().is_some() {
            return Err(VeinError::internal(format!(
                "projected Parquet column '{}' is ambiguous in '{}'",
                field.name, path
            )));
        }
        root_indices.push(index);
    }
    if root_indices.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(VeinError::internal(format!(
            "projected columns are not in source schema order for '{}'",
            path
        )));
    }
    Ok(root_indices)
}
