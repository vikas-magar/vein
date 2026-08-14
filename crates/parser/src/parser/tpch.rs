#[cfg(test)]
#[allow(non_snake_case)]
mod tests {
    use crate::ast::expr::Expr;
    use crate::ast::pipe_op::PipeOperator;
    use crate::ast::query::{Query, TableSource};
    use crate::parser::{ParseError, Parser};

    fn parse_query(input: &str) -> Result<Query<'_>, ParseError> {
        Parser::new(input).parse_query()
    }

    // TPC-H Q6: Forecasting Revenue Change
    //
    // SELECT SUM(l_extendedprice * l_discount) AS revenue
    // FROM lineitem
    // WHERE l_shipdate >= DATE '1994-01-01'
    //   AND l_shipdate < DATE '1995-01-01'
    //   AND l_discount BETWEEN 0.05 AND 0.07
    //   AND l_quantity < 24
    #[test]
    fn test_tpch_q6() {
        let q = parse_query("\
            FROM lineitem \
            |> WHERE l_shipdate >= DATE '1994-01-01' \
               AND l_shipdate < DATE '1995-01-01' \
               AND l_discount BETWEEN 0.05 AND 0.07 \
               AND l_quantity < 24 \
            |> AGGREGATE sum(l_extendedprice * l_discount) AS revenue \
        ").unwrap();
        assert_eq!(q.source, TableSource::Table { name: "lineitem".into(), alias: None });
        assert_eq!(q.pipes.len(), 2);
        assert!(matches!(q.pipes[0], PipeOperator::Where { .. }), "expected Where pipe");
        assert!(matches!(q.pipes[1], PipeOperator::Aggregate { .. }), "expected Aggregate pipe");

        // Verify WHERE contains BETWEEN and typed string
        if let PipeOperator::Where { predicate } = &q.pipes[0] {
            assert!(
                contains_between(predicate),
                "WHERE predicate should contain BETWEEN(l_discount, 0.05, 0.07)"
            );
            assert!(
                contains_typed_string(predicate),
                "WHERE predicate should contain typed string (DATE '1994-01-01')"
            );
        }
    }

    // TPC-H Q4: Order Priority Checking
    //
    // SELECT o_orderpriority, COUNT(*) AS order_count
    // FROM orders
    // WHERE o_orderdate >= DATE '1993-07-01'
    //   AND o_orderdate < DATE '1993-10-01'
    //   AND EXISTS (
    //     SELECT * FROM lineitem
    //     WHERE l_orderkey = o_orderkey AND l_commitdate < l_receiptdate
    //   )
    // GROUP BY o_orderpriority
    // ORDER BY o_orderpriority
    #[test]
    fn test_tpch_q4() {
        let q = parse_query("\
            FROM orders \
            |> WHERE o_orderdate >= DATE '1993-07-01' \
               AND o_orderdate < DATE '1993-10-01' \
               AND EXISTS (FROM lineitem |> WHERE l_orderkey = o_orderkey AND l_commitdate < l_receiptdate) \
        ").unwrap();
        assert_eq!(q.source, TableSource::Table { name: "orders".into(), alias: None });
        assert_eq!(q.pipes.len(), 1);
        assert!(matches!(q.pipes[0], PipeOperator::Where { .. }), "expected Where pipe");

        if let PipeOperator::Where { predicate } = &q.pipes[0] {
            assert!(
                contains_exists(predicate),
                "WHERE predicate should contain EXISTS subquery"
            );
        }
    }

    // TPC-H Q12: Shipping Modes and Order Priority
    //
    // SELECT l_shipmode,
    //        SUM(CASE WHEN o_orderpriority = '1-URGENT' THEN 1 ELSE 0 END),
    //        SUM(CASE WHEN o_orderpriority <> '1-URGENT' THEN 1 ELSE 0 END)
    // FROM orders, lineitem
    // WHERE o_orderkey = l_orderkey
    //   AND l_shipmode IN ('MAIL', 'SHIP')
    //   AND l_commitdate < l_receiptdate
    //   AND l_shipdate < l_commitdate
    //   AND l_receiptdate >= DATE '1994-01-01'
    //   AND l_receiptdate < DATE '1995-01-01'
    // GROUP BY l_shipmode
    // ORDER BY l_shipmode
    #[test]
    fn test_tpch_q12() {
        let q = parse_query("\
            FROM orders \
            |> JOIN lineitem ON o_orderkey = l_orderkey \
            |> WHERE l_shipmode IN ('MAIL', 'SHIP') \
               AND l_commitdate < l_receiptdate \
               AND l_receiptdate >= DATE '1994-01-01' \
        ").unwrap();
        assert_eq!(q.source, TableSource::Table { name: "orders".into(), alias: None });
        assert_eq!(q.pipes.len(), 2);
        assert!(matches!(q.pipes[0], PipeOperator::Join { .. }), "expected Join pipe");
        assert!(matches!(q.pipes[1], PipeOperator::Where { .. }), "expected Where pipe");

        if let PipeOperator::Where { predicate } = &q.pipes[1] {
            assert!(
                contains_in_list(predicate),
                "WHERE predicate should contain IN list ('MAIL', 'SHIP')"
            );
        }
    }

    // TPC-H Q19: Discounted Revenue
    //
    // SELECT SUM(l_extendedprice * (1 - l_discount)) AS revenue
    // FROM lineitem, part
    // WHERE (p_brand = 'Brand#12'
    //   AND p_container IN ('SM CASE', 'SM BOX')
    //   AND l_quantity >= 1 AND l_quantity <= 11
    //   AND p_size BETWEEN 1 AND 5
    //   AND l_shipmode IN ('AIR', 'AIR REG'))
    //   OR (p_brand = 'Brand#23'
    //   AND p_container IN ('MED BAG', 'MED BOX')
    //   AND l_quantity >= 10 AND l_quantity <= 20
    //   AND p_size BETWEEN 1 AND 10
    //   AND l_shipmode IN ('AIR', 'AIR REG'))
    #[test]
    fn test_tpch_q19() {
        let q = parse_query("\
            FROM lineitem \
            |> JOIN part ON p_partkey = l_partkey \
            |> WHERE (p_brand = 'Brand#12' \
               AND p_container IN ('SM CASE', 'SM BOX') \
               AND l_quantity >= 1 AND l_quantity <= 11 \
               AND p_size BETWEEN 1 AND 5 \
               AND l_shipmode IN ('AIR', 'AIR REG')) \
               OR (p_brand = 'Brand#23' \
               AND p_container IN ('MED BAG', 'MED BOX') \
               AND l_quantity >= 10 AND l_quantity <= 20 \
               AND p_size BETWEEN 1 AND 10 \
               AND l_shipmode IN ('AIR', 'AIR REG')) \
            |> AGGREGATE sum(l_extendedprice * (1 - l_discount)) AS revenue \
        ").unwrap();
        assert_eq!(q.source, TableSource::Table { name: "lineitem".into(), alias: None });
        assert_eq!(q.pipes.len(), 3);
        assert!(matches!(q.pipes[0], PipeOperator::Join { .. }), "expected Join pipe");
        assert!(matches!(q.pipes[1], PipeOperator::Where { .. }), "expected Where pipe");
        assert!(matches!(q.pipes[2], PipeOperator::Aggregate { .. }), "expected Aggregate pipe");
    }

    // --- helper functions ---

    fn contains_between(expr: &crate::ast::expr::SpannedExpr<'_>) -> bool {
        match &expr.node {
            Expr::Between { .. } => true,
            Expr::BinaryOp { left, right, .. } => {
                contains_between(left.as_ref()) || contains_between(right.as_ref())
            }
            _ => false,
        }
    }

    fn contains_typed_string(expr: &crate::ast::expr::SpannedExpr<'_>) -> bool {
        match &expr.node {
            Expr::TypedString { .. } => true,
            Expr::BinaryOp { left, right, .. } => {
                contains_typed_string(left.as_ref()) || contains_typed_string(right.as_ref())
            }
            _ => false,
        }
    }

    fn contains_exists(expr: &crate::ast::expr::SpannedExpr<'_>) -> bool {
        match &expr.node {
            Expr::Exists { .. } => true,
            Expr::BinaryOp { left, right, .. } => {
                contains_exists(left.as_ref()) || contains_exists(right.as_ref())
            }
            _ => false,
        }
    }

    fn contains_in_list(expr: &crate::ast::expr::SpannedExpr<'_>) -> bool {
        match &expr.node {
            Expr::InList { .. } => true,
            Expr::BinaryOp { left, right, .. } => {
                contains_in_list(left.as_ref()) || contains_in_list(right.as_ref())
            }
            _ => false,
        }
    }

    // TPC-H Q1: Pricing Summary Report
    //
    // SELECT l_returnflag, l_linestatus, sum(l_quantity) as sum_qty,
    // sum(l_extendedprice) as sum_base_price, sum(l_extendedprice*(1-l_discount)) as sum_disc_price,
    // sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge, avg(l_quantity) as avg_qty,
    // avg(l_extendedprice) as avg_price, avg(l_discount) as avg_disc, count(*) as count_order
    // FROM lineitem
    // WHERE l_shipdate <= date '1998-12-01' - interval '90' day
    // GROUP BY l_returnflag, l_linestatus
    // ORDER BY l_returnflag, l_linestatus
    #[test]
    fn test_tpch_q1() {
        let q = parse_query("\
            FROM lineitem \
            |> WHERE l_shipdate <= DATE '1998-09-02' \
            |> AGGREGATE \
                sum(l_quantity) AS sum_qty, \
                sum(l_extendedprice) AS sum_base_price, \
                sum(l_extendedprice * (1 - l_discount)) AS sum_disc_price, \
                sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge, \
                avg(l_quantity) AS avg_qty, \
                avg(l_extendedprice) AS avg_price, \
                avg(l_discount) AS avg_disc, \
                count(*) AS count_order \
               GROUP BY l_returnflag, l_linestatus \
            |> ORDER l_returnflag, l_linestatus \
        ").unwrap();
        
        assert_eq!(q.source, TableSource::Table { name: "lineitem".into(), alias: None });
        assert_eq!(q.pipes.len(), 3);
        
        assert!(matches!(q.pipes[0], PipeOperator::Where { .. }));
        assert!(matches!(q.pipes[1], PipeOperator::Aggregate { .. }));
        assert!(matches!(q.pipes[2], PipeOperator::OrderBy { .. }));
        
        if let PipeOperator::Aggregate { group_by, aggregates } = &q.pipes[1] {
            assert_eq!(group_by.len(), 2, "l_returnflag and l_linestatus should be in group_by");
            assert_eq!(aggregates.len(), 8, "All 8 aggregate functions should be in aggregates");
            
            assert_eq!(aggregates[0].alias.as_deref(), Some("sum_qty"));
            assert_eq!(aggregates[7].alias.as_deref(), Some("count_order"));
        }
    }
}
