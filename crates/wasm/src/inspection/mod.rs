use serde::Serialize;

#[derive(Serialize)]
#[cfg(target_arch = "wasm32")]
pub(crate) struct Column {
    pub name: String,
    pub qualifier: Option<String>,
    pub data_type: String,
}

#[derive(Serialize)]
#[cfg(target_arch = "wasm32")]
pub(crate) struct Function {
    pub name: String,
    pub signature: String,
}

#[derive(Serialize)]
pub(crate) struct Diagnostic {
    pub message: String,
    pub category: String,
    pub from: Option<usize>,
    pub to: Option<usize>,
}

impl Diagnostic {
    pub fn from_error(query: &str, error: &error::VeinError) -> Self {
        let span = error.source_span().filter(|span| span.start <= span.end
            && query.is_char_boundary(span.start) && query.is_char_boundary(span.end));
        Self {
            message: error.to_string(), category: error.category().to_string(),
            from: span.map(|span| query[..span.start].encode_utf16().count()),
            to: span.map(|span| query[..span.end].encode_utf16().count()),
        }
    }
}

#[derive(Default, Serialize)]
#[cfg(target_arch = "wasm32")]
pub(crate) struct Inspection {
    pub columns: Vec<Column>,
    pub functions: Vec<Function>,
    pub diagnostics: Vec<Diagnostic>,
}

#[cfg(target_arch = "wasm32")]
pub(crate) async fn inspect(engine: &engine::local::LocalQueryEngine, query: &str, prefix: &str) -> Inspection {
    let full = engine.inspect_query(query).await;
    let mut output = Inspection::default();
    if let Err(error) = &full { output.diagnostics.push(Diagnostic::from_error(query, error)); }
    let schema = if prefix == query { full.ok() }
        else if prefix.trim().is_empty() { None }
        else { engine.inspect_query(prefix).await.ok() };
    if let Some(schema) = schema {
        output.columns = schema.fields.iter().map(|column| Column {
            name: column.name.clone(), qualifier: column.qualifier.clone(),
            data_type: format!("{:?}", column.data_type),
        }).collect();
    }
    output.functions = engine.function_signatures().into_iter().map(|(name, signature)| Function {
        name: name.to_owned(),
        signature: format!("{:?}({}) → {:?}", signature.kind,
            signature.arg_types.iter().map(|value| format!("{value:?}")).collect::<Vec<_>>().join(", "),
            signature.return_type),
    }).collect();
    output
}

#[cfg(test)]
mod tests;
