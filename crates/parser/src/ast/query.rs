use crate::ast::pipe_op::PipeOperator;
use std::borrow::Cow;

/// A parsed top-level statement: either a plain query or an `EXPLAIN`-wrapped
/// query that should be planned but not executed.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum QueryNode<'a> {
    /// A plain query to execute.
    Plain {
        /// The wrapped query.
        #[serde(borrow)]
        query: Query<'a>,
    },
    /// An `EXPLAIN <query>` statement: bind and display the plan only.
    Explain {
        /// The wrapped query.
        #[serde(borrow)]
        query: Box<Query<'a>>,
    },
}

/// A parsed query: a source and a pipe pipeline.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Query<'a> {
    /// The table source.

    #[serde(borrow)]
    pub source: TableSource<'a>,
    /// The pipeline operators.

    #[serde(borrow)]
    pub pipes: Vec<PipeOperator<'a>>,
}

/// The left-hand side of a query: a table, object store, or subquery.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum TableSource<'a> {
    /// A base table reference. The name is a borrow of the input for simple
    /// identifiers, or an owned join of the parts for qualified names (e.g. `a.b`).
    Table {
        /// The table name.
        #[serde(borrow)]
        name: Cow<'a, str>,
        /// The optional alias.
        #[serde(borrow)]
        alias: Option<&'a str>,
    },
    /// An object store source referenced by URL, with inline options.
    ObjectStore {
        /// The store URL.
        #[serde(borrow)]
        url: &'a str,
        /// Inline options (e.g. format).
        #[serde(borrow)]
        inline_options: Vec<(&'a str, &'a str)>,
        /// The optional alias.
        #[serde(borrow)]
        alias: Option<&'a str>,
    },
    /// A subquery source.
    Subquery {
        /// The inner query.
        #[serde(borrow)]
        query: Box<Query<'a>>,
        /// The optional alias.
        #[serde(borrow)]
        alias: Option<&'a str>,
    },
}

/// The kind of a `UNION`/`INTERSECT`/`EXCEPT` set operation.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum SetOpKind {
    /// UNION
    Union,
    /// INTERSECT
    Intersect,
    /// EXCEPT
    Except,
}
