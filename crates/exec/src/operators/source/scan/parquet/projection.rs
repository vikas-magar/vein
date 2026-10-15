//! Schema projection and field-to-Parquet column mapping.

use std::sync::Arc;

use arrow::datatypes::{DataType as ArrowDataType, Schema, SchemaRef};
use common::Result;
use error::VeinError;
use types::{ResolvedColumnRef, VeinDataType, VeinSchema};

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
