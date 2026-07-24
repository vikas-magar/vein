use crate::ast::pipe_op::{JoinClause, PipeOperator};
use crate::token::Span;

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Query {
    pub source: TableSource,
    pub joins: Vec<JoinClause>,
    pub pipes: Vec<PipeOperator>,
}

impl Query {
    pub fn source_span(&self) -> Span {
        Span::default()
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum TableSource {
    Table { name: String, alias: Option<String> },
    ObjectStore { url: String, inline_options: Vec<(String, String)>, alias: Option<String> },
    Subquery { query: Box<Query>, alias: Option<String> },
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum SetOpKind {
    Union,
    Intersect,
    Except,
}
