# TPC-H Pipe Query Examples

## Generate the Data

The TPC-H Parquet dataset in this repository was generated at scale factor `0.01` with one Parquet part per table:

```bash
tpchgen-cli parquet --scale-factor 0.10 --parts 3 --output-dir data/tpch --no-progress --quiet
```

Tables are discovered beneath `/Users/anvi/Next/vein/data/tpch/`, for example `orders/orders.1.parquet` and
`lineitem/lineitem.1.parquet`.

Run a query from the repository root with:

```bash
VEIN_DATA_DIR=/Users/anvi/Next/vein/data/tpch cargo run -p cli -- "FROM orders |> WHERE o_orderkey = 1 |> SELECT o_orderkey, o_orderdate, o_totalprice"
```

## Queries

Look up one order:

```bash
VEIN_DATA_DIR=/Users/anvi/Next/vein/data/tpch cargo run -p cli -- "
FROM orders
|> WHERE o_orderkey = 1
|> SELECT o_orderkey, o_orderdate, o_orderstatus, o_totalprice
"
```

Inspect the line items belonging to an order:

```bash
VEIN_DATA_DIR=/Users/anvi/Next/vein/data/tpch cargo run -p cli -- "
FROM lineitem
|> WHERE l_orderkey = 1
|> SELECT l_orderkey, l_partkey, l_quantity, l_extendedprice
"
```

Look up a part:

```bash
VEIN_DATA_DIR=/Users/anvi/Next/vein/data/tpch cargo run -p cli -- "
FROM part
|> WHERE p_partkey = 1
|> SELECT p_partkey, p_name, p_brand, p_retailprice
"
```

Look up a customer:

```bash
VEIN_DATA_DIR=/Users/anvi/Next/vein/data/tpch cargo run -p cli -- "
FROM customer
|> WHERE c_custkey = 1
|> SELECT c_custkey, c_name, c_mktsegment
"
```

Truncate an order date to calendar boundaries:

```bash
VEIN_DATA_DIR=/Users/anvi/Next/vein/data/tpch cargo run -p cli -- "
FROM orders
|> WHERE o_orderkey = 1
|> SELECT
    o_orderdate,
    TRUNCATE(o_orderdate, 'MONTH') AS order_month,
    TRUNCATE(o_orderdate, 'QUARTER') AS order_quarter,
    TRUNCATE(o_orderdate, 'YEAR') AS order_year
"
```

Expected result: `o_orderdate = 1996-01-02`; all three truncated values are `1996-01-01`.

Truncate shipment dates to the start of their ISO week (Monday):

```bash
VEIN_DATA_DIR=/Users/anvi/Next/vein/data/tpch cargo run -p cli -- "
FROM lineitem
|> WHERE l_orderkey = 1
|> SELECT l_shipdate, TRUNCATE(l_shipdate, 'WEEK') AS ship_week
|> LIMIT 5
"
```

For example, `1996-03-13` truncates to `1996-03-11`.

Combine predicates while keeping the result selective:

```bash
VEIN_DATA_DIR=/Users/anvi/Next/vein/data/tpch cargo run -p cli -- "
FROM customer
|> WHERE c_custkey = 1 AND c_mktsegment = 'BUILDING'
|> SELECT c_custkey, c_name, c_mktsegment
"
```

Global aggregate over a matching order:

```bash
VEIN_DATA_DIR=/Users/anvi/Next/vein/data/tpch cargo run -p cli -- "
FROM orders
|> WHERE o_orderkey = 1
|> AGGREGATE count(o_orderkey) AS order_count, sum(o_totalprice) AS total_price
"
```

Expected result: one row with `order_count = 1` and `total_price = 172799.49`.

Filter aggregate results:

```bash
VEIN_DATA_DIR=/Users/anvi/Next/vein/data/tpch cargo run -p cli -- "
FROM orders
|> AGGREGATE count(o_orderkey) AS order_count GROUP BY o_orderstatus
|> WHERE order_count > 50000
"
```

Expected result: two rows, for `O` (`56175`) and `F` (`55872`).

Empty global aggregate over no matching rows:

```bash
VEIN_DATA_DIR=/Users/anvi/Next/vein/data/tpch cargo run -p cli -- "
FROM orders
|> WHERE o_orderkey = -1
|> AGGREGATE count(o_orderkey) AS order_count, sum(o_totalprice) AS total_price
"
```

Expected result: one row with `order_count = 0` and `total_price = NULL`.

Vector similarity over TPC-H order comments (reference query):

```bash
VEIN_DATA_DIR=/Users/anvi/Next/vein/data/tpch cargo run -p cli -- "
FROM orders
|> EXTEND EMBED(o_comment) AS comment_vector
|> EXTEND EMBED('urgent delivery for a priority customer') AS query_vector
|> EXTEND VECTOR_DISTANCE(comment_vector, query_vector) AS distance
|> SELECT o_orderkey, o_orderdate, distance
|> ORDER distance
|> LIMIT 10
"
```

TPC-H has no stored vector columns, so this example embeds the order comments and a search phrase. `VECTOR_DISTANCE`
uses cosine distance. This is a reference query; `EMBED` does not have a runtime implementation yet, so it cannot run
end to end until that built-in is implemented.

Equivalent CLI invocation:

```bash
VEIN_DATA_DIR=/Users/anvi/Next/vein/data/tpch cargo run -p cli -- "FROM lineitem |> WHERE l_orderkey = 1 |> SELECT l_orderkey, l_partkey, l_quantity, l_extendedprice"
```

## Current Execution Scope

These examples exercise TPC-H Parquet table discovery, filtering, projection, aggregation, and filtering aggregate
results. Projection above aggregates and other aggregate post-operators remain unsupported. Joins support only
restricted equality-key forms. Queries such as TPC-H Q1 and Q3 still contain unsupported combinations. `LIMIT` is
supported only in placements that preserve pipe semantics.

## TPC-H Query Suite (Q1-Q22)

The following are Pipe-style reference translations of the 22 TPC-H query shapes. They are included to document
coverage and guide implementation; many require aggregation, joins, subqueries, scalar functions, or ordering that
the current local physical engine does not execute yet. They are not claimed to be runnable benchmark queries.

### Q1: Pricing Summary Report

```bash
VEIN_DATA_DIR=/Users/anvi/Next/vein/data/tpch cargo run -p cli -- "
FROM lineitem
|> WHERE l_shipdate <= DATE '1998-12-01' - INTERVAL '90' DAY
|> AGGREGATE
    sum(l_quantity) AS sum_qty,
    sum(l_extendedprice) AS sum_base_price,
    sum(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
    avg(l_quantity) AS avg_qty,
    avg(l_extendedprice) AS avg_price,
    avg(l_discount) AS avg_disc,
    count(*) AS count_order
   GROUP BY l_returnflag, l_linestatus
|> ORDER l_returnflag, l_linestatus
"
```

### Q2: Minimum Cost Supplier

```bash
VEIN_DATA_DIR=/Users/anvi/Next/vein/data/tpch cargo run -p cli -- "
FROM part
|> JOIN partsupp ON p_partkey = ps_partkey
|> JOIN supplier ON ps_suppkey = s_suppkey
|> JOIN nation ON s_nationkey = n_nationkey
|> JOIN region ON n_regionkey = r_regionkey
|> WHERE p_size = 15 AND p_type LIKE '%BRASS' AND r_name = 'EUROPE'
|> WHERE ps_supplycost = (FROM partsupp |> WHERE ps_partkey = p_partkey |> AGGREGATE min(ps_supplycost))
|> SELECT s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment
|> ORDER s_acctbal DESC, n_name, s_name, p_partkey
"
```

### Q3: Shipping Priority

```bash
VEIN_DATA_DIR=/Users/anvi/Next/vein/data/tpch cargo run -p cli -- "
FROM customer
|> JOIN orders ON c_custkey = o_custkey
|> JOIN lineitem ON o_orderkey = l_orderkey
|> WHERE c_mktsegment = 'BUILDING' AND o_orderdate < DATE '1995-03-15' AND l_shipdate > DATE '1995-03-15'
|> AGGREGATE sum(l_extendedprice * (1 - l_discount)) AS revenue
   GROUP BY l_orderkey, o_orderdate, o_shippriority
|> ORDER revenue DESC, o_orderdate
"
```

### Q4: Order Priority Checking

```bash
VEIN_DATA_DIR=/Users/anvi/Next/vein/data/tpch cargo run -p cli -- "
FROM orders
|> WHERE o_orderdate >= DATE '1993-07-01' AND o_orderdate < DATE '1993-10-01'
   AND EXISTS (FROM lineitem |> WHERE l_orderkey = o_orderkey AND l_commitdate < l_receiptdate)
|> AGGREGATE count(*) AS order_count GROUP BY o_orderpriority
|> ORDER o_orderpriority
"
```

### Q5: Local Supplier Volume

```bash
VEIN_DATA_DIR=/Users/anvi/Next/vein/data/tpch cargo run -p cli -- "
FROM customer
|> JOIN orders ON c_custkey = o_custkey
|> JOIN lineitem ON o_orderkey = l_orderkey
|> JOIN supplier ON l_suppkey = s_suppkey AND c_nationkey = s_nationkey
|> JOIN nation ON s_nationkey = n_nationkey
|> JOIN region ON n_regionkey = r_regionkey
|> WHERE r_name = 'ASIA' AND o_orderdate >= DATE '1994-01-01' AND o_orderdate < DATE '1995-01-01'
|> AGGREGATE sum(l_extendedprice * (1 - l_discount)) AS revenue GROUP BY n_name
|> ORDER revenue DESC
"
```

### Q6: Forecasting Revenue Change

```bash
VEIN_DATA_DIR=/Users/anvi/Next/vein/data/tpch cargo run -p cli -- "
FROM lineitem
|> WHERE l_shipdate >= DATE '1994-01-01' AND l_shipdate < DATE '1995-01-01'
   AND l_discount BETWEEN 0.05 AND 0.07 AND l_quantity < 24
|> AGGREGATE sum(l_extendedprice * l_discount) AS revenue
"
```

### Q7: Volume Shipping

```bash
VEIN_DATA_DIR=/Users/anvi/Next/vein/data/tpch cargo run -p cli -- "
FROM supplier
|> JOIN lineitem ON s_suppkey = l_suppkey
|> JOIN orders ON l_orderkey = o_orderkey
|> JOIN customer ON o_custkey = c_custkey
|> JOIN nation AS supp_nation ON s_nationkey = supp_nation.n_nationkey
|> JOIN nation AS cust_nation ON c_nationkey = cust_nation.n_nationkey
|> WHERE ((supp_nation.n_name = 'FRANCE' AND cust_nation.n_name = 'GERMANY')
   OR (supp_nation.n_name = 'GERMANY' AND cust_nation.n_name = 'FRANCE'))
   AND l_shipdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
|> EXTEND year(l_shipdate) AS shipping_year,
   CASE WHEN supp_nation.n_name = 'FRANCE' THEN 'FRANCE' ELSE 'GERMANY' END AS supp_country
|> AGGREGATE sum(l_extendedprice * (1 - l_discount)) AS revenue
   GROUP BY supp_country, shipping_year
|> ORDER supp_country, shipping_year
"
```

### Q8: National Market Share

```bash
VEIN_DATA_DIR=/Users/anvi/Next/vein/data/tpch cargo run -p cli -- "
FROM part
|> JOIN lineitem ON p_partkey = l_partkey
|> JOIN supplier ON l_suppkey = s_suppkey
|> JOIN orders ON l_orderkey = o_orderkey
|> JOIN customer ON o_custkey = c_custkey
|> JOIN nation AS cust_nation ON c_nationkey = cust_nation.n_nationkey
|> JOIN region ON cust_nation.n_regionkey = r_regionkey
|> JOIN nation AS supp_nation ON s_nationkey = supp_nation.n_nationkey
|> WHERE r_name = 'AMERICA' AND p_type = 'ECONOMY ANODIZED STEEL'
   AND o_orderdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
|> EXTEND year(o_orderdate) AS order_year
|> AGGREGATE sum(CASE WHEN supp_nation.n_name = 'BRAZIL'
   THEN l_extendedprice * (1 - l_discount) ELSE 0 END) /
   sum(l_extendedprice * (1 - l_discount)) AS market_share GROUP BY order_year
|> ORDER order_year
"
```

### Q9: Product Type Profit Measure

```bash
VEIN_DATA_DIR=/Users/anvi/Next/vein/data/tpch cargo run -p cli -- "
FROM lineitem
|> JOIN part ON l_partkey = p_partkey
|> JOIN supplier ON l_suppkey = s_suppkey
|> JOIN partsupp ON l_partkey = ps_partkey AND l_suppkey = ps_suppkey
|> JOIN orders ON l_orderkey = o_orderkey
|> JOIN nation ON s_nationkey = n_nationkey
|> WHERE p_name LIKE '%green%'
|> EXTEND year(o_orderdate) AS order_year,
   l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity AS amount
|> AGGREGATE sum(amount) AS sum_profit GROUP BY n_name, order_year
|> ORDER n_name, order_year DESC
"
```

### Q10: Returned Item Reporting

```bash
VEIN_DATA_DIR=/Users/anvi/Next/vein/data/tpch cargo run -p cli -- "
FROM customer
|> JOIN orders ON c_custkey = o_custkey
|> JOIN lineitem ON o_orderkey = l_orderkey
|> JOIN nation ON c_nationkey = n_nationkey
|> WHERE o_orderdate >= DATE '1993-10-01' AND o_orderdate < DATE '1994-01-01'
   AND l_returnflag = 'R'
|> AGGREGATE sum(l_extendedprice * (1 - l_discount)) AS revenue
   GROUP BY c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment
|> ORDER revenue DESC
"
```

### Q11: Important Stock Identification

```bash
VEIN_DATA_DIR=/Users/anvi/Next/vein/data/tpch cargo run -p cli -- "
FROM partsupp
|> JOIN supplier ON ps_suppkey = s_suppkey
|> JOIN nation ON s_nationkey = n_nationkey
|> WHERE n_name = 'GERMANY'
|> EXTEND ps_supplycost * ps_availqty AS value
|> AGGREGATE sum(value) AS total_value GROUP BY ps_partkey
|> WHERE total_value > (FROM partsupp |> JOIN supplier ON ps_suppkey = s_suppkey
   |> JOIN nation ON s_nationkey = n_nationkey |> WHERE n_name = 'GERMANY'
   |> AGGREGATE sum(ps_supplycost * ps_availqty) * 0.0001)
|> ORDER total_value DESC
"
```

### Q12: Shipping Modes and Order Priority

```bash
VEIN_DATA_DIR=/Users/anvi/Next/vein/data/tpch cargo run -p cli -- "
FROM orders
|> JOIN lineitem ON o_orderkey = l_orderkey
|> WHERE l_shipmode IN ('MAIL', 'SHIP') AND l_commitdate < l_receiptdate
   AND l_shipdate < l_commitdate
   AND l_receiptdate >= DATE '1994-01-01' AND l_receiptdate < DATE '1995-01-01'
|> AGGREGATE
   sum(CASE WHEN o_orderpriority IN ('1-URGENT', '2-HIGH') THEN 1 ELSE 0 END) AS high_line_count,
   sum(CASE WHEN o_orderpriority NOT IN ('1-URGENT', '2-HIGH') THEN 1 ELSE 0 END) AS low_line_count
   GROUP BY l_shipmode
|> ORDER l_shipmode
"
```

### Q13: Customer Distribution

```bash
VEIN_DATA_DIR=/Users/anvi/Next/vein/data/tpch cargo run -p cli -- "
FROM customer
|> LEFT JOIN orders ON c_custkey = o_custkey AND o_comment NOT LIKE '%special%requests%'
|> AGGREGATE count(o_orderkey) AS c_count GROUP BY c_custkey
|> AGGREGATE count(*) AS custdist GROUP BY c_count
|> ORDER custdist DESC, c_count DESC
"
```

### Q14: Promotion Effect

```bash
VEIN_DATA_DIR=/Users/anvi/Next/vein/data/tpch cargo run -p cli -- "
FROM lineitem
|> JOIN part ON l_partkey = p_partkey
|> WHERE l_shipdate >= DATE '1995-09-01' AND l_shipdate < DATE '1995-10-01'
|> AGGREGATE 100.0 * sum(CASE WHEN p_type LIKE 'PROMO%'
   THEN l_extendedprice * (1 - l_discount) ELSE 0 END) /
   sum(l_extendedprice * (1 - l_discount)) AS promo_revenue
"
```

### Q15: Top Supplier

```bash
VEIN_DATA_DIR=/Users/anvi/Next/vein/data/tpch cargo run -p cli -- "
FROM lineitem
|> WHERE l_shipdate >= DATE '1996-01-01' AND l_shipdate < DATE '1996-04-01'
|> AGGREGATE sum(l_extendedprice * (1 - l_discount)) AS total_revenue GROUP BY l_suppkey
|> WHERE total_revenue = (FROM lineitem
   |> WHERE l_shipdate >= DATE '1996-01-01' AND l_shipdate < DATE '1996-04-01'
   |> AGGREGATE sum(l_extendedprice * (1 - l_discount)) AS supplier_revenue GROUP BY l_suppkey
   |> AGGREGATE max(supplier_revenue))
|> JOIN supplier ON s_suppkey = l_suppkey
|> SELECT s_suppkey, s_name, s_address, s_phone, total_revenue
|> ORDER s_suppkey
"
```

### Q16: Parts/Supplier Relationship

```bash
VEIN_DATA_DIR=/Users/anvi/Next/vein/data/tpch cargo run -p cli -- "
FROM partsupp
|> JOIN part ON ps_partkey = p_partkey
|> WHERE p_brand <> 'Brand#45' AND p_type NOT LIKE 'MEDIUM POLISHED%'
   AND p_size IN (49, 14, 23, 45, 19, 3, 36, 9)
   AND ps_suppkey NOT IN (FROM supplier |> WHERE s_comment LIKE '%Customer%Complaints%'
      |> SELECT s_suppkey)
|> AGGREGATE count(DISTINCT ps_suppkey) AS supplier_count
   GROUP BY p_brand, p_type, p_size
|> ORDER supplier_count DESC, p_brand, p_type, p_size
"
```

### Q17: Small-Quantity-Order Revenue

```bash
VEIN_DATA_DIR=/Users/anvi/Next/vein/data/tpch cargo run -p cli -- "
FROM lineitem
|> JOIN part ON l_partkey = p_partkey
|> WHERE p_brand = 'Brand#23' AND p_container = 'MED BOX'
   AND l_quantity < (FROM lineitem |> WHERE l_partkey = p_partkey
      |> AGGREGATE avg(l_quantity) * 0.2)
|> AGGREGATE sum(l_extendedprice) / 7.0 AS avg_yearly
"
```

### Q18: Large Volume Customer

```bash
VEIN_DATA_DIR=/Users/anvi/Next/vein/data/tpch cargo run -p cli -- "
FROM customer
|> JOIN orders ON c_custkey = o_custkey
|> JOIN lineitem ON o_orderkey = l_orderkey
|> WHERE o_orderkey IN (FROM lineitem |> AGGREGATE sum(l_quantity) AS total_qty
   GROUP BY l_orderkey |> WHERE total_qty > 300)
|> AGGREGATE sum(l_quantity) AS total_qty GROUP BY c_name, c_custkey,
   o_orderkey, o_orderdate, o_totalprice
|> ORDER o_totalprice DESC, o_orderdate
"
```

### Q19: Discounted Revenue

```bash
VEIN_DATA_DIR=/Users/anvi/Next/vein/data/tpch cargo run -p cli -- "
FROM lineitem
|> JOIN part ON p_partkey = l_partkey
|> WHERE ((p_brand = 'Brand#12' AND p_container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
   AND l_quantity BETWEEN 1 AND 11 AND p_size BETWEEN 1 AND 5)
   OR (p_brand = 'Brand#23' AND p_container IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
   AND l_quantity BETWEEN 10 AND 20 AND p_size BETWEEN 1 AND 10)
   OR (p_brand = 'Brand#34' AND p_container IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
   AND l_quantity BETWEEN 20 AND 30 AND p_size BETWEEN 1 AND 15))
   AND l_shipmode IN ('AIR', 'AIR REG') AND l_shipinstruct = 'DELIVER IN PERSON'
|> AGGREGATE sum(l_extendedprice * (1 - l_discount)) AS revenue
"
```

### Q20: Potential Part Promotion

```bash
VEIN_DATA_DIR=/Users/anvi/Next/vein/data/tpch cargo run -p cli -- "
FROM supplier
|> JOIN nation ON s_nationkey = n_nationkey
|> WHERE n_name = 'CANADA'
   AND s_suppkey IN (FROM partsupp |> JOIN part ON ps_partkey = p_partkey
      |> WHERE p_name LIKE 'forest%'
      |> WHERE ps_availqty > (FROM lineitem
         |> WHERE l_partkey = ps_partkey AND l_suppkey = ps_suppkey
            AND l_shipdate >= DATE '1994-01-01' AND l_shipdate < DATE '1995-01-01'
         |> AGGREGATE sum(l_quantity) * 0.5))
|> SELECT s_name, s_address
|> ORDER s_name
"
```

### Q21: Suppliers Who Kept Orders Waiting

```bash
VEIN_DATA_DIR=/Users/anvi/Next/vein/data/tpch cargo run -p cli -- "
FROM supplier
|> JOIN lineitem AS l1 ON s_suppkey = l1.l_suppkey
|> JOIN orders ON l1.l_orderkey = o_orderkey
|> JOIN nation ON s_nationkey = n_nationkey
|> WHERE n_name = 'SAUDI ARABIA' AND o_orderstatus = 'F'
   AND l1.l_receiptdate > l1.l_commitdate
   AND EXISTS (FROM lineitem AS l2 |> WHERE l2.l_orderkey = l1.l_orderkey
      AND l2.l_suppkey <> l1.l_suppkey)
   AND NOT EXISTS (FROM lineitem AS l3 |> WHERE l3.l_orderkey = l1.l_orderkey
      AND l3.l_suppkey <> l1.l_suppkey AND l3.l_receiptdate > l3.l_commitdate)
|> AGGREGATE count(*) AS numwait GROUP BY s_name
|> ORDER numwait DESC, s_name
"
```

### Q22: Global Sales Opportunity

```bash
VEIN_DATA_DIR=/Users/anvi/Next/vein/data/tpch cargo run -p cli -- "
FROM customer
|> EXTEND substring(c_phone, 1, 2) AS country_code
|> WHERE country_code IN ('13', '31', '23', '29', '30', '18', '17')
   AND c_acctbal > (FROM customer
      |> WHERE c_acctbal > 0 AND substring(c_phone, 1, 2) IN ('13', '31', '23', '29', '30', '18', '17')
      |> AGGREGATE avg(c_acctbal))
   AND NOT EXISTS (FROM orders |> WHERE o_custkey = c_custkey)
|> AGGREGATE count(*) AS numcust, sum(c_acctbal) AS totacctbal GROUP BY country_code
|> ORDER country_code
"
```
