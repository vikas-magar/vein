use crate::ast::expr::SpannedExpr;
use crate::ast::query::{Query, SetOpKind};

/// The join flavor used by a `JOIN` clause.
#[derive(Debug, Clone, Copy, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum JoinType {
    /// INNER JOIN
    Inner,
    /// LEFT JOIN
    Left,
    /// RIGHT JOIN
    Right,
    /// FULL JOIN
    Full,
    /// CROSS JOIN
    Cross,
}

/// A single pipe operator (`|>`) in a query pipeline.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum PipeOperator<'a> {
    /// `|> SELECT ...`
    Select {
        /// The selected items.

        #[serde(borrow)]
        items: Vec<SelectItem<'a>>,
    },
    /// `|> WHERE ...`
    Where {
        /// The filter predicate.

        #[serde(borrow)]
        predicate: SpannedExpr<'a>,
    },
    /// `|> QUALIFY <window predicate>`
    Qualify {
        /// The window-function predicate.

        #[serde(borrow)]
        predicate: SpannedExpr<'a>,
    },
    /// `|> AGGREGATE BY ... [SUM(...)]`
    Aggregate {
        /// Grouping sets. A normal `GROUP BY a, b` is represented as `[[a, b]]`;
        /// an aggregate without `GROUP BY` is represented as `[[]]`.

        #[serde(borrow)]
        grouping_sets: Vec<Vec<OrderByExpr<'a>>>,
        /// The aggregate expressions.

        #[serde(borrow)]
        aggregates: Vec<AggregateExpr<'a>>,
    },
    /// `|> ORDER BY ...`
    OrderBy {
        /// The sort keys.

        #[serde(borrow)]
        order_bys: Vec<OrderByExpr<'a>>,
    },
    /// `|> LIMIT n [OFFSET m]`
    Limit {
        /// The maximum number of rows.
        limit: u64,
        /// The number of rows to skip.
        offset: u64,
    },
    /// `|> EXTEND ...`
    Extend {
        /// The computed columns to add.

        #[serde(borrow)]
        items: Vec<SelectItem<'a>>,
    },
    /// `|> SET col = expr, ...`
    Set {
        /// The column assignments.

        #[serde(borrow)]
        assignments: Vec<(&'a str, SpannedExpr<'a>)>,
    },
    /// `|> DROP col, ...`
    Drop {
        /// The columns to remove.

        #[serde(borrow)]
        columns: Vec<&'a str>,
    },
    /// `|> RENAME a TO b, ...`
    Rename {
        /// The source/target name pairs.

        #[serde(borrow)]
        renames: Vec<(&'a str, &'a str)>,
    },
    /// `|> AS alias`
    As {
        /// The pipeline alias.

        #[serde(borrow)]
        alias: &'a str,
    },
    /// `|> DISTINCT`
    Distinct,
    /// `|> JOIN ...`
    Join {
        /// The join clause.

        #[serde(borrow)]
        join: JoinClause<'a>,
    },
    /// `|> CALL name(args...)`
    Call {
        /// The called function name.

        #[serde(borrow)]
        name: &'a str,
        /// The function arguments.

        #[serde(borrow)]
        args: Vec<SpannedExpr<'a>>,
    },
    /// `|> TABLESAMPLE method(percent)`
    TableSample {
        /// The sampling method.

        #[serde(borrow)]
        method: &'a str,
        /// The sampling percentage.
        percent: f64,
    },
    /// `|> ASSERT condition MESSAGE '...'?`
    Assert {
        /// The condition expression to check.

        #[serde(borrow)]
        condition: SpannedExpr<'a>,
        /// An optional human-readable failure message.

        #[serde(borrow)]
        message: Option<&'a str>,
    },
    /// `|> UNION | INTERSECT | EXCEPT subquery [ALL]`
    SetOp {
        /// The set operation kind.
        kind: SetOpKind,
        /// The right-hand query.

        #[serde(borrow)]
        right: Box<Query<'a>>,
        /// Whether `ALL` was specified.
        all: bool,
    },
    /// `|> PIVOT SUM(amount) AS total BY category IN ('a', 'b')`
    Pivot {
        /// The aggregate expression to pivot (e.g. `SUM(amount)`).
        #[serde(borrow)]
        aggregate: SpannedExpr<'a>,
        /// The optional output alias for the pivoted column(s).
        #[serde(borrow)]
        alias: Option<&'a str>,
        /// The column whose distinct values become new columns.
        #[serde(borrow)]
        value_column: &'a str,
        /// The explicit pivot values.
        #[serde(borrow)]
        values: Vec<SpannedExpr<'a>>,
    },
    /// `|> UNPIVOT [ALL] amount BY category IN (a, b)`
    Unpivot {
        /// The generated value column name.
        #[serde(borrow)]
        value_name: &'a str,
        /// The generated name column name.
        #[serde(borrow)]
        name_name: &'a str,
        /// The columns to unpivot.
        #[serde(borrow)]
        columns: Vec<&'a str>,
        /// Whether `ALL` (include nulls) was specified.
        include_nulls: bool,
    },
    /// `|> WITH ...` (commonly used to add a CTE prior to a query)
    With {
        /// The common table expressions.

        #[serde(borrow)]
        ctes: Vec<CommonTableExpr<'a>>,
    },
    /// `|> WRITE TO target`
    Write {
        /// The write target.

        #[serde(borrow)]
        target: WriteTarget<'a>,
    },
    /// `|> FORK (pipeline1, pipeline2, ...)`
    Fork {
        /// The forked pipelines.

        #[serde(borrow)]
        pipelines: Vec<Vec<PipeOperator<'a>>>,
    },
    /// `|> TEE (pipeline1, pipeline2, ...)`
    Tee {
        /// The teed pipelines.

        #[serde(borrow)]
        pipelines: Vec<Vec<PipeOperator<'a>>>,
    },
}

/// A projection item: an expression plus an optional alias.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct SelectItem<'a> {
    /// The projected expression.

    #[serde(borrow)]
    pub expr: SpannedExpr<'a>,
    /// The optional output column alias.

    #[serde(borrow)]
    pub alias: Option<&'a str>,
}

/// An aggregate expression with an optional result alias.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct AggregateExpr<'a> {
    /// The aggregate call expression.

    #[serde(borrow)]
    pub expr: SpannedExpr<'a>,
    /// The optional result column alias.

    #[serde(borrow)]
    pub alias: Option<&'a str>,
}

/// A sort key with direction and null ordering.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct OrderByExpr<'a> {
    /// The expression to order by.

    #[serde(borrow)]
    pub expr: SpannedExpr<'a>,
    /// Whether the sort is ascending.
    pub asc: bool,
    /// Whether nulls sort first.
    pub nulls_first: bool,
}

/// A window frame specification (`ROWS BETWEEN ... AND ...`).
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct WindowFrame {
    /// The frame unit (`ROWS`/`RANGE`/`GROUPS`).
    pub kind: WindowFrameKind,
    /// The frame start bound.
    pub start: WindowFrameBound,
    /// The optional frame end bound.
    pub end: Option<WindowFrameBound>,
}

/// The unit a window frame is measured in.
#[derive(Debug, Clone, Copy, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum WindowFrameKind {
    /// ROWS
    Rows,
    /// RANGE
    Range,
    /// GROUPS
    Groups,
}

/// One side of a window frame.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum WindowFrameBound {
    /// UNBOUNDED PRECEDING
    UnboundedPreceding,
    /// n PRECEDING
    Preceding(u64),
    /// CURRENT ROW
    CurrentRow,
    /// n FOLLOWING
    Following(u64),
    /// UNBOUNDED FOLLOWING
    UnboundedFollowing,
}

/// A `JOIN` clause: the right side plus its predicate.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct JoinClause<'a> {
    /// The join flavor.
    pub join_type: JoinType,
    /// The right-hand query.

    #[serde(borrow)]
    pub right: Box<Query<'a>>,
    /// The `ON` predicate.

    #[serde(borrow)]
    pub predicate: Option<SpannedExpr<'a>>,
    /// The `USING` columns.

    #[serde(borrow)]
    pub using: Vec<&'a str>,
}

/// The target of a `WRITE TO` pipe: a table plus inline options.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct WriteTarget<'a> {
    /// The destination table name.

    #[serde(borrow)]
    pub table: &'a str,
    /// Inline write options (e.g. format).

    #[serde(borrow)]
    pub options: Vec<(&'a str, &'a str)>,
}

/// A common table expression bound to an alias.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct CommonTableExpr<'a> {
    /// The CTE alias.

    #[serde(borrow)]
    pub alias: &'a str,
    /// The optional explicit column names.

    #[serde(borrow)]
    pub columns: Option<Vec<&'a str>>,
    /// The CTE body query.

    #[serde(borrow)]
    pub query: Box<Query<'a>>,
}
