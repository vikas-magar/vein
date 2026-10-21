use crate::ast::pipe_op::PipeOperator;
use crate::parser::test_utils::utils::*;

#[test]
fn test_limit_only() {
    let q = parse_query("FROM t |> LIMIT 5").expect("test should succeed");
    if let PipeOperator::Limit { limit, offset } = q.pipes[0] {
        assert_eq!(limit, 5);
        assert_eq!(offset, 0);
    } else {
        panic!("expected Limit");
    }
}

#[test]
fn test_limit_with_offset() {
    let q = parse_query("FROM t |> LIMIT 10 OFFSET 20").expect("test should succeed");
    if let PipeOperator::Limit { limit, offset } = q.pipes[0] {
        assert_eq!(limit, 10);
        assert_eq!(offset, 20);
    } else {
        panic!("expected Limit");
    }
}

#[test]
fn test_distinct() {
    let q = parse_query("FROM t |> DISTINCT").expect("test should succeed");
    assert_eq!(q.pipes[0], PipeOperator::Distinct);
}

#[test]
fn test_set() {
    let q = parse_query("FROM t |> SET salary = 0").expect("test should succeed");
    if let PipeOperator::Set { assignments } = &q.pipes[0] {
        assert_eq!(assignments.len(), 1);
        assert_eq!(assignments[0].0, "salary");
    } else {
        panic!("expected Set");
    }
}

#[test]
fn test_drop() {
    let q = parse_query("FROM t |> DROP temp_col").expect("test should succeed");
    if let PipeOperator::Drop { columns } = &q.pipes[0] {
        assert_eq!(columns, &["temp_col"]);
    } else {
        panic!("expected Drop");
    }
}

#[test]
fn test_rename() {
    let q = parse_query("FROM t |> RENAME old_name AS new_name").expect("test should succeed");
    if let PipeOperator::Rename { renames } = &q.pipes[0] {
        assert_eq!(renames.len(), 1);
        assert_eq!(renames[0], ("old_name", "new_name"));
    } else {
        panic!("expected Rename");
    }
}

#[test]
fn test_assert_simple() {
    let q = parse_query("FROM t |> ASSERT age >= 0").expect("test should succeed");
    assert!(matches!(q.pipes[0], PipeOperator::Assert { .. }));
}

#[test]
fn test_write() {
    let q = parse_query("FROM t |> WRITE results_table").expect("test should succeed");
    assert!(matches!(q.pipes[0], PipeOperator::Write { .. }));
}

#[test]
fn test_tablesample() {
    let q = parse_query("FROM t |> TABLESAMPLE bernoulli(25.0)").expect("test should succeed");
    if let PipeOperator::TableSample { method, percent } = &q.pipes[0] {
        assert_eq!(*method, "bernoulli");
        assert!((*percent - 25.0).abs() < f64::EPSILON);
    } else {
        panic!("expected TableSample");
    }
}

#[test]
fn test_pivot() {
    let q = parse_query("FROM t |> PIVOT SUM(amount) AS total BY category IN ('a', 'b')")
        .expect("test should succeed");
    if let PipeOperator::Pivot {
        aggregate,
        alias,
        value_column,
        values,
    } = &q.pipes[0]
    {
        assert_eq!(*alias, Some("total"));
        assert_eq!(*value_column, "category");
        assert_eq!(values.len(), 2);
        let _ = aggregate;
    } else {
        panic!("expected Pivot");
    }
}

#[test]
fn test_unpivot() {
    let q = parse_query("FROM t |> UNPIVOT ALL amount BY category IN (a, b)")
        .expect("test should succeed");
    if let PipeOperator::Unpivot {
        value_name,
        name_name,
        columns,
        include_nulls,
    } = &q.pipes[0]
    {
        assert_eq!(*value_name, "amount");
        assert_eq!(*name_name, "category");
        assert_eq!(columns, &["a", "b"]);
        assert!(*include_nulls);
    } else {
        panic!("expected Unpivot");
    }
}

#[test]
fn test_parse_fork_positive() {
    let q = parse_query("FROM t |> FORK ( |> WHERE x = 1, |> SELECT y )")
        .expect("test should succeed");
    if let PipeOperator::Fork { pipelines } = &q.pipes[0] {
        assert_eq!(pipelines.len(), 2);
        assert!(matches!(pipelines[0][0], PipeOperator::Where { .. }));
        assert!(matches!(pipelines[1][0], PipeOperator::Select { .. }));
    } else {
        panic!("Expected FORK");
    }
}

#[test]
fn test_parse_fork_negative_missing_paren() {
    let err = parse_query("FROM t |> FORK |> WHERE x = 1").expect_err("test should fail");
    assert!(err.to_string().contains("Expected LParen"));
}

#[test]
fn test_parse_tee_positive() {
    let q =
        parse_query("FROM t |> TEE ( |> DROP x ) |> LIMIT 10").expect("test should succeed");
    assert_eq!(q.pipes.len(), 2);
    assert!(matches!(q.pipes[0], PipeOperator::Tee { .. }));
    assert!(matches!(q.pipes[1], PipeOperator::Limit { .. }));
}

#[test]
fn test_fork_empty_pipeline_is_legal_noop() {
    let q = parse_query("FROM t |> FORK ()").expect("test should succeed");
    if let PipeOperator::Fork { pipelines } = &q.pipes[0] {
        assert_eq!(pipelines.len(), 1);
        assert!(pipelines[0].is_empty());
    } else {
        panic!("expected Fork");
    }
}

#[test]
fn test_tee_empty_pipeline_is_legal_noop() {
    let q = parse_query("FROM t |> TEE ()").expect("test should succeed");
    if let PipeOperator::Tee { pipelines } = &q.pipes[0] {
        assert_eq!(pipelines.len(), 1);
        assert!(pipelines[0].is_empty());
    } else {
        panic!("expected Tee");
    }
}
