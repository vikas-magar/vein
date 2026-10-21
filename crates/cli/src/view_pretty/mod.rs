use arrow::util::display::{ArrayFormatter, FormatOptions};
use common::{Result, VeinError};
use exec::pipeline::DataChunkView;

/// Formats only the requested result views; source arrays are read in place.
pub(super) fn pretty_format_views(batches: &[DataChunkView]) -> Result<String> {
    let Some(first) = batches.first() else {
        return Ok(String::new());
    };
    let schema = first
        .schema()
        .ok_or_else(|| VeinError::internal("result view is missing its schema"))?;
    let mut rows = Vec::new();
    let mut widths = schema
        .fields()
        .iter()
        .map(|field| field.name().chars().count())
        .collect::<Vec<_>>();

    for batch in batches {
        if batch
            .schema()
            .is_none_or(|actual| actual.as_ref() != schema.as_ref())
        {
            return Err(VeinError::internal(
                "result schema changed while formatting pretty output",
            ));
        }
        let options = FormatOptions::default();
        let formatters = (0..schema.fields().len())
            .map(|column| {
                batch
                    .source_arrays_for_column(column)?
                    .iter()
                    .map(|array| {
                        ArrayFormatter::try_new(array.as_ref(), &options).map_err(|error| {
                            VeinError::internal(format!("prepare result value formatter: {error}"))
                        })
                    })
                    .collect::<Result<Vec<_>>>()
            })
            .collect::<Result<Vec<_>>>()?;
        for row in 0..batch.num_rows() {
            let mut values = Vec::with_capacity(schema.fields().len());
            for column in 0..schema.fields().len() {
                let value = match batch.source_row(column, row)? {
                    Some((source, source_row)) => formatters[column]
                        .get(source)
                        .ok_or_else(|| VeinError::internal("result source formatter is missing"))?
                        .value(source_row)
                        .to_string(),
                    None => "NULL".to_owned(),
                };
                widths[column] = widths[column].max(value.chars().count());
                values.push(value);
            }
            rows.push(values);
        }
    }

    let mut output = String::new();
    let headers = schema
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect::<Vec<_>>();
    write_border(&mut output, &widths);
    write_row(&mut output, &headers, &widths);
    write_border(&mut output, &widths);
    for row in &rows {
        write_row(&mut output, row, &widths);
    }
    write_border(&mut output, &widths);
    Ok(output)
}

fn write_border(output: &mut String, widths: &[usize]) {
    output.push('+');
    for width in widths {
        output.extend(std::iter::repeat_n('-', width.saturating_add(2)));
        output.push('+');
    }
    output.push('\n');
}

fn write_row(output: &mut String, values: &[String], widths: &[usize]) {
    output.push('|');
    for (value, width) in values.iter().zip(widths) {
        output.push(' ');
        output.push_str(value);
        output.extend(std::iter::repeat_n(
            ' ',
            width
                .saturating_sub(value.chars().count())
                .saturating_add(1),
        ));
        output.push('|');
    }
    output.push('\n');
}

#[cfg(test)]
mod tests;
