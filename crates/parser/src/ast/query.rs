use crate::ast::pipe_op::{JoinClause, PipeOperator};
use crate::token::Span;
use std::borrow::Cow;

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Query<'a> {
    #[serde(borrow)]
    pub source: TableSource<'a>,
    #[serde(borrow)]
    pub joins: Vec<JoinClause<'a>>,
    #[serde(borrow)]
    pub pipes: Vec<PipeOperator<'a>>,
}

impl<'a> Query<'a> {
    pub fn source_span(&self) -> Span {
        Span::default()
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum TableSource<'a> {
    /// A base table reference. The name is a borrow of the input for simple
    /// identifiers, or an owned join of the parts for qualified names (e.g. `a.b`).
    Table {
        #[serde(borrow)]
        name: Cow<'a, str>,
        #[serde(borrow)]
        alias: Option<&'a str>,
    },
    ObjectStore {
        #[serde(borrow)]
        url: &'a str,
        #[serde(borrow)]
        inline_options: Vec<(&'a str, &'a str)>,
        #[serde(borrow)]
        alias: Option<&'a str>,
    },
    Subquery {
        #[serde(borrow)]
        query: Box<Query<'a>>,
        #[serde(borrow)]
        alias: Option<&'a str>,
    },
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum SetOpKind {
    Union,
    Intersect,
    Except,
}
