-- name: q1-pricing-summary
SELECT l_returnflag, l_linestatus,
       sum(l_quantity) AS sum_qty,
       sum(l_extendedprice) AS sum_base_price,
       sum(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
       sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
       avg(l_quantity) AS avg_qty,
       avg(l_extendedprice) AS avg_price,
       avg(l_discount) AS avg_disc,
       count(*) AS count_order
FROM read_parquet('{{DATA_ROOT}}/lineitem/*.parquet')
WHERE l_shipdate <= DATE '1998-12-01' - INTERVAL '90' DAY
GROUP BY l_returnflag, l_linestatus
ORDER BY l_returnflag, l_linestatus

-- name: q2-minimum-cost-supplier
SELECT s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment
FROM read_parquet('{{DATA_ROOT}}/part/*.parquet') AS part
JOIN read_parquet('{{DATA_ROOT}}/partsupp/*.parquet') AS partsupp
  ON part.p_partkey = partsupp.ps_partkey
JOIN read_parquet('{{DATA_ROOT}}/supplier/*.parquet') AS supplier
  ON partsupp.ps_suppkey = supplier.s_suppkey
JOIN read_parquet('{{DATA_ROOT}}/nation/*.parquet') AS nation
  ON supplier.s_nationkey = nation.n_nationkey
JOIN read_parquet('{{DATA_ROOT}}/region/*.parquet') AS region
  ON nation.n_regionkey = region.r_regionkey
WHERE part.p_size = 15
  AND part.p_type LIKE '%BRASS'
  AND region.r_name = 'EUROPE'
  AND partsupp.ps_supplycost = (
    SELECT min(candidate.ps_supplycost)
    FROM read_parquet('{{DATA_ROOT}}/partsupp/*.parquet') AS candidate
    WHERE candidate.ps_partkey = part.p_partkey
  )
ORDER BY s_acctbal DESC, n_name, s_name, p_partkey

-- name: q3-shipping-priority
SELECT lineitem.l_orderkey, orders.o_orderdate, orders.o_shippriority,
       sum(lineitem.l_extendedprice * (1 - lineitem.l_discount)) AS revenue
FROM read_parquet('{{DATA_ROOT}}/customer/*.parquet') AS customer
JOIN read_parquet('{{DATA_ROOT}}/orders/*.parquet') AS orders
  ON customer.c_custkey = orders.o_custkey
JOIN read_parquet('{{DATA_ROOT}}/lineitem/*.parquet') AS lineitem
  ON orders.o_orderkey = lineitem.l_orderkey
WHERE customer.c_mktsegment = 'BUILDING'
  AND orders.o_orderdate < DATE '1995-03-15'
  AND lineitem.l_shipdate > DATE '1995-03-15'
GROUP BY lineitem.l_orderkey, orders.o_orderdate, orders.o_shippriority
ORDER BY revenue DESC, orders.o_orderdate

-- name: q4-order-priority-checking
SELECT orders.o_orderpriority, count(*) AS order_count
FROM read_parquet('{{DATA_ROOT}}/orders/*.parquet') AS orders
WHERE orders.o_orderdate >= DATE '1993-07-01'
  AND orders.o_orderdate < DATE '1993-10-01'
  AND EXISTS (
    SELECT 1 FROM read_parquet('{{DATA_ROOT}}/lineitem/*.parquet') AS lineitem
    WHERE lineitem.l_orderkey = orders.o_orderkey
      AND lineitem.l_commitdate < lineitem.l_receiptdate
  )
GROUP BY orders.o_orderpriority
ORDER BY orders.o_orderpriority

-- name: q5-local-supplier-volume
SELECT nation.n_name,
       sum(lineitem.l_extendedprice * (1 - lineitem.l_discount)) AS revenue
FROM read_parquet('{{DATA_ROOT}}/customer/*.parquet') AS customer
JOIN read_parquet('{{DATA_ROOT}}/orders/*.parquet') AS orders
  ON customer.c_custkey = orders.o_custkey
JOIN read_parquet('{{DATA_ROOT}}/lineitem/*.parquet') AS lineitem
  ON orders.o_orderkey = lineitem.l_orderkey
JOIN read_parquet('{{DATA_ROOT}}/supplier/*.parquet') AS supplier
  ON lineitem.l_suppkey = supplier.s_suppkey
 AND customer.c_nationkey = supplier.s_nationkey
JOIN read_parquet('{{DATA_ROOT}}/nation/*.parquet') AS nation
  ON supplier.s_nationkey = nation.n_nationkey
JOIN read_parquet('{{DATA_ROOT}}/region/*.parquet') AS region
  ON nation.n_regionkey = region.r_regionkey
WHERE region.r_name = 'ASIA'
  AND orders.o_orderdate >= DATE '1994-01-01'
  AND orders.o_orderdate < DATE '1995-01-01'
GROUP BY nation.n_name
ORDER BY revenue DESC

-- name: q6-forecast-revenue-change
SELECT sum(l_extendedprice * l_discount) AS revenue
FROM read_parquet('{{DATA_ROOT}}/lineitem/*.parquet')
WHERE l_shipdate >= DATE '1994-01-01'
  AND l_shipdate < DATE '1995-01-01'
  AND l_discount BETWEEN 0.05 AND 0.07
  AND l_quantity < 24

-- name: q7-volume-shipping
SELECT CASE WHEN supp_nation.n_name = 'FRANCE' THEN 'FRANCE' ELSE 'GERMANY' END AS supp_country,
       EXTRACT(YEAR FROM lineitem.l_shipdate) AS shipping_year,
       sum(lineitem.l_extendedprice * (1 - lineitem.l_discount)) AS revenue
FROM read_parquet('{{DATA_ROOT}}/supplier/*.parquet') AS supplier
JOIN read_parquet('{{DATA_ROOT}}/lineitem/*.parquet') AS lineitem
  ON supplier.s_suppkey = lineitem.l_suppkey
JOIN read_parquet('{{DATA_ROOT}}/orders/*.parquet') AS orders
  ON lineitem.l_orderkey = orders.o_orderkey
JOIN read_parquet('{{DATA_ROOT}}/customer/*.parquet') AS customer
  ON orders.o_custkey = customer.c_custkey
JOIN read_parquet('{{DATA_ROOT}}/nation/*.parquet') AS supp_nation
  ON supplier.s_nationkey = supp_nation.n_nationkey
JOIN read_parquet('{{DATA_ROOT}}/nation/*.parquet') AS cust_nation
  ON customer.c_nationkey = cust_nation.n_nationkey
WHERE ((supp_nation.n_name = 'FRANCE' AND cust_nation.n_name = 'GERMANY')
    OR (supp_nation.n_name = 'GERMANY' AND cust_nation.n_name = 'FRANCE'))
  AND lineitem.l_shipdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
GROUP BY supp_country, shipping_year
ORDER BY supp_country, shipping_year

-- name: q8-national-market-share
SELECT EXTRACT(YEAR FROM orders.o_orderdate) AS order_year,
       sum(CASE WHEN supp_nation.n_name = 'BRAZIL'
                THEN lineitem.l_extendedprice * (1 - lineitem.l_discount)
                ELSE 0 END)
       / sum(lineitem.l_extendedprice * (1 - lineitem.l_discount)) AS market_share
FROM read_parquet('{{DATA_ROOT}}/part/*.parquet') AS part
JOIN read_parquet('{{DATA_ROOT}}/lineitem/*.parquet') AS lineitem
  ON part.p_partkey = lineitem.l_partkey
JOIN read_parquet('{{DATA_ROOT}}/supplier/*.parquet') AS supplier
  ON lineitem.l_suppkey = supplier.s_suppkey
JOIN read_parquet('{{DATA_ROOT}}/orders/*.parquet') AS orders
  ON lineitem.l_orderkey = orders.o_orderkey
JOIN read_parquet('{{DATA_ROOT}}/customer/*.parquet') AS customer
  ON orders.o_custkey = customer.c_custkey
JOIN read_parquet('{{DATA_ROOT}}/nation/*.parquet') AS cust_nation
  ON customer.c_nationkey = cust_nation.n_nationkey
JOIN read_parquet('{{DATA_ROOT}}/region/*.parquet') AS region
  ON cust_nation.n_regionkey = region.r_regionkey
JOIN read_parquet('{{DATA_ROOT}}/nation/*.parquet') AS supp_nation
  ON supplier.s_nationkey = supp_nation.n_nationkey
WHERE region.r_name = 'AMERICA'
  AND part.p_type = 'ECONOMY ANODIZED STEEL'
  AND orders.o_orderdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
GROUP BY order_year
ORDER BY order_year

-- name: q9-product-type-profit
SELECT nation.n_name,
       EXTRACT(YEAR FROM orders.o_orderdate) AS order_year,
       sum(lineitem.l_extendedprice * (1 - lineitem.l_discount)
           - partsupp.ps_supplycost * lineitem.l_quantity) AS sum_profit
FROM read_parquet('{{DATA_ROOT}}/lineitem/*.parquet') AS lineitem
JOIN read_parquet('{{DATA_ROOT}}/part/*.parquet') AS part
  ON lineitem.l_partkey = part.p_partkey
JOIN read_parquet('{{DATA_ROOT}}/supplier/*.parquet') AS supplier
  ON lineitem.l_suppkey = supplier.s_suppkey
JOIN read_parquet('{{DATA_ROOT}}/partsupp/*.parquet') AS partsupp
  ON lineitem.l_partkey = partsupp.ps_partkey
 AND lineitem.l_suppkey = partsupp.ps_suppkey
JOIN read_parquet('{{DATA_ROOT}}/orders/*.parquet') AS orders
  ON lineitem.l_orderkey = orders.o_orderkey
JOIN read_parquet('{{DATA_ROOT}}/nation/*.parquet') AS nation
  ON supplier.s_nationkey = nation.n_nationkey
WHERE part.p_name LIKE '%green%'
GROUP BY nation.n_name, order_year
ORDER BY nation.n_name, order_year DESC

-- name: q10-returned-item-reporting
SELECT customer.c_custkey, customer.c_name, customer.c_acctbal, customer.c_phone,
       nation.n_name, customer.c_address, customer.c_comment,
       sum(lineitem.l_extendedprice * (1 - lineitem.l_discount)) AS revenue
FROM read_parquet('{{DATA_ROOT}}/customer/*.parquet') AS customer
JOIN read_parquet('{{DATA_ROOT}}/orders/*.parquet') AS orders
  ON customer.c_custkey = orders.o_custkey
JOIN read_parquet('{{DATA_ROOT}}/lineitem/*.parquet') AS lineitem
  ON orders.o_orderkey = lineitem.l_orderkey
JOIN read_parquet('{{DATA_ROOT}}/nation/*.parquet') AS nation
  ON customer.c_nationkey = nation.n_nationkey
WHERE orders.o_orderdate >= DATE '1993-10-01'
  AND orders.o_orderdate < DATE '1994-01-01'
  AND lineitem.l_returnflag = 'R'
GROUP BY customer.c_custkey, customer.c_name, customer.c_acctbal, customer.c_phone,
         nation.n_name, customer.c_address, customer.c_comment
ORDER BY revenue DESC

-- name: q11-important-stock-identification
SELECT partsupp.ps_partkey,
       sum(partsupp.ps_supplycost * partsupp.ps_availqty) AS total_value
FROM read_parquet('{{DATA_ROOT}}/partsupp/*.parquet') AS partsupp
JOIN read_parquet('{{DATA_ROOT}}/supplier/*.parquet') AS supplier
  ON partsupp.ps_suppkey = supplier.s_suppkey
JOIN read_parquet('{{DATA_ROOT}}/nation/*.parquet') AS nation
  ON supplier.s_nationkey = nation.n_nationkey
WHERE nation.n_name = 'GERMANY'
GROUP BY partsupp.ps_partkey
HAVING sum(partsupp.ps_supplycost * partsupp.ps_availqty) > (
  SELECT sum(ps2.ps_supplycost * ps2.ps_availqty) * 0.0001
  FROM read_parquet('{{DATA_ROOT}}/partsupp/*.parquet') AS ps2
  JOIN read_parquet('{{DATA_ROOT}}/supplier/*.parquet') AS supplier2
    ON ps2.ps_suppkey = supplier2.s_suppkey
  JOIN read_parquet('{{DATA_ROOT}}/nation/*.parquet') AS nation2
    ON supplier2.s_nationkey = nation2.n_nationkey
  WHERE nation2.n_name = 'GERMANY'
)
ORDER BY total_value DESC

-- name: q12-shipping-modes-priority
SELECT lineitem.l_shipmode,
       sum(CASE WHEN orders.o_orderpriority IN ('1-URGENT', '2-HIGH') THEN 1 ELSE 0 END) AS high_line_count,
       sum(CASE WHEN orders.o_orderpriority NOT IN ('1-URGENT', '2-HIGH') THEN 1 ELSE 0 END) AS low_line_count
FROM read_parquet('{{DATA_ROOT}}/orders/*.parquet') AS orders
JOIN read_parquet('{{DATA_ROOT}}/lineitem/*.parquet') AS lineitem
  ON orders.o_orderkey = lineitem.l_orderkey
WHERE lineitem.l_shipmode IN ('MAIL', 'SHIP')
  AND lineitem.l_commitdate < lineitem.l_receiptdate
  AND lineitem.l_shipdate < lineitem.l_commitdate
  AND lineitem.l_receiptdate >= DATE '1994-01-01'
  AND lineitem.l_receiptdate < DATE '1995-01-01'
GROUP BY lineitem.l_shipmode
ORDER BY lineitem.l_shipmode

-- name: q13-customer-distribution
SELECT order_counts.c_count, count(*) AS custdist
FROM (
  SELECT customer.c_custkey, count(orders.o_orderkey) AS c_count
  FROM read_parquet('{{DATA_ROOT}}/customer/*.parquet') AS customer
  LEFT JOIN read_parquet('{{DATA_ROOT}}/orders/*.parquet') AS orders
    ON customer.c_custkey = orders.o_custkey
   AND orders.o_comment NOT LIKE '%special%requests%'
  GROUP BY customer.c_custkey
) AS order_counts
GROUP BY order_counts.c_count
ORDER BY custdist DESC, order_counts.c_count DESC

-- name: q14-promotion-effect
SELECT 100.0 * sum(CASE WHEN part.p_type LIKE 'PROMO%'
                        THEN lineitem.l_extendedprice * (1 - lineitem.l_discount)
                        ELSE 0 END)
       / sum(lineitem.l_extendedprice * (1 - lineitem.l_discount)) AS promo_revenue
FROM read_parquet('{{DATA_ROOT}}/lineitem/*.parquet') AS lineitem
JOIN read_parquet('{{DATA_ROOT}}/part/*.parquet') AS part
  ON lineitem.l_partkey = part.p_partkey
WHERE lineitem.l_shipdate >= DATE '1995-09-01'
  AND lineitem.l_shipdate < DATE '1995-10-01'

-- name: q15-top-supplier
WITH revenue AS (
  SELECT lineitem.l_suppkey,
         sum(lineitem.l_extendedprice * (1 - lineitem.l_discount)) AS total_revenue
  FROM read_parquet('{{DATA_ROOT}}/lineitem/*.parquet') AS lineitem
  WHERE lineitem.l_shipdate >= DATE '1996-01-01'
    AND lineitem.l_shipdate < DATE '1996-04-01'
  GROUP BY lineitem.l_suppkey
),
max_revenue AS (
  SELECT max(total_revenue) AS total_revenue FROM revenue
)
SELECT supplier.s_suppkey, supplier.s_name, supplier.s_address, supplier.s_phone,
       revenue.total_revenue
FROM revenue
JOIN max_revenue ON revenue.total_revenue = max_revenue.total_revenue
JOIN read_parquet('{{DATA_ROOT}}/supplier/*.parquet') AS supplier
  ON supplier.s_suppkey = revenue.l_suppkey
ORDER BY supplier.s_suppkey

-- name: q16-parts-supplier-relationship
SELECT part.p_brand, part.p_type, part.p_size,
       count(DISTINCT partsupp.ps_suppkey) AS supplier_count
FROM read_parquet('{{DATA_ROOT}}/partsupp/*.parquet') AS partsupp
JOIN read_parquet('{{DATA_ROOT}}/part/*.parquet') AS part
  ON partsupp.ps_partkey = part.p_partkey
WHERE part.p_brand <> 'Brand#45'
  AND part.p_type NOT LIKE 'MEDIUM POLISHED%'
  AND part.p_size IN (49, 14, 23, 45, 19, 3, 36, 9)
  AND partsupp.ps_suppkey NOT IN (
    SELECT supplier.s_suppkey
    FROM read_parquet('{{DATA_ROOT}}/supplier/*.parquet') AS supplier
    WHERE supplier.s_comment LIKE '%Customer%Complaints%'
  )
GROUP BY part.p_brand, part.p_type, part.p_size
ORDER BY supplier_count DESC, part.p_brand, part.p_type, part.p_size

-- name: q17-small-quantity-order-revenue
SELECT sum(lineitem.l_extendedprice) / 7.0 AS avg_yearly
FROM read_parquet('{{DATA_ROOT}}/lineitem/*.parquet') AS lineitem
JOIN read_parquet('{{DATA_ROOT}}/part/*.parquet') AS part
  ON lineitem.l_partkey = part.p_partkey
WHERE part.p_brand = 'Brand#23'
  AND part.p_container = 'MED BOX'
  AND lineitem.l_quantity < (
    SELECT avg(lineitem2.l_quantity) * 0.2
    FROM read_parquet('{{DATA_ROOT}}/lineitem/*.parquet') AS lineitem2
    WHERE lineitem2.l_partkey = part.p_partkey
  )

-- name: q18-large-volume-customer
SELECT customer.c_name, customer.c_custkey, orders.o_orderkey, orders.o_orderdate,
       orders.o_totalprice, sum(lineitem.l_quantity) AS total_qty
FROM read_parquet('{{DATA_ROOT}}/customer/*.parquet') AS customer
JOIN read_parquet('{{DATA_ROOT}}/orders/*.parquet') AS orders
  ON customer.c_custkey = orders.o_custkey
JOIN read_parquet('{{DATA_ROOT}}/lineitem/*.parquet') AS lineitem
  ON orders.o_orderkey = lineitem.l_orderkey
WHERE orders.o_orderkey IN (
  SELECT lineitem2.l_orderkey
  FROM read_parquet('{{DATA_ROOT}}/lineitem/*.parquet') AS lineitem2
  GROUP BY lineitem2.l_orderkey
  HAVING sum(lineitem2.l_quantity) > 300
)
GROUP BY customer.c_name, customer.c_custkey, orders.o_orderkey,
         orders.o_orderdate, orders.o_totalprice
ORDER BY orders.o_totalprice DESC, orders.o_orderdate

-- name: q19-discounted-revenue
SELECT sum(lineitem.l_extendedprice * (1 - lineitem.l_discount)) AS revenue
FROM read_parquet('{{DATA_ROOT}}/lineitem/*.parquet') AS lineitem
JOIN read_parquet('{{DATA_ROOT}}/part/*.parquet') AS part
  ON part.p_partkey = lineitem.l_partkey
WHERE (
    (part.p_brand = 'Brand#12'
      AND part.p_container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
      AND lineitem.l_quantity BETWEEN 1 AND 11
      AND part.p_size BETWEEN 1 AND 5)
 OR (part.p_brand = 'Brand#23'
      AND part.p_container IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
      AND lineitem.l_quantity BETWEEN 10 AND 20
      AND part.p_size BETWEEN 1 AND 10)
 OR (part.p_brand = 'Brand#34'
      AND part.p_container IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
      AND lineitem.l_quantity BETWEEN 20 AND 30
      AND part.p_size BETWEEN 1 AND 15)
  )
  AND lineitem.l_shipmode IN ('AIR', 'AIR REG')
  AND lineitem.l_shipinstruct = 'DELIVER IN PERSON'

-- name: q20-potential-part-promotion
SELECT supplier.s_name, supplier.s_address
FROM read_parquet('{{DATA_ROOT}}/supplier/*.parquet') AS supplier
JOIN read_parquet('{{DATA_ROOT}}/nation/*.parquet') AS nation
  ON supplier.s_nationkey = nation.n_nationkey
WHERE nation.n_name = 'CANADA'
  AND supplier.s_suppkey IN (
    SELECT partsupp.ps_suppkey
    FROM read_parquet('{{DATA_ROOT}}/partsupp/*.parquet') AS partsupp
    JOIN read_parquet('{{DATA_ROOT}}/part/*.parquet') AS part
      ON partsupp.ps_partkey = part.p_partkey
    WHERE part.p_name LIKE 'forest%'
      AND partsupp.ps_availqty > (
        SELECT sum(lineitem.l_quantity) * 0.5
        FROM read_parquet('{{DATA_ROOT}}/lineitem/*.parquet') AS lineitem
        WHERE lineitem.l_partkey = partsupp.ps_partkey
          AND lineitem.l_suppkey = partsupp.ps_suppkey
          AND lineitem.l_shipdate >= DATE '1994-01-01'
          AND lineitem.l_shipdate < DATE '1995-01-01'
      )
  )
ORDER BY supplier.s_name

-- name: q21-suppliers-kept-orders-waiting
SELECT supplier.s_name, count(*) AS numwait
FROM read_parquet('{{DATA_ROOT}}/supplier/*.parquet') AS supplier
JOIN read_parquet('{{DATA_ROOT}}/lineitem/*.parquet') AS l1
  ON supplier.s_suppkey = l1.l_suppkey
JOIN read_parquet('{{DATA_ROOT}}/orders/*.parquet') AS orders
  ON l1.l_orderkey = orders.o_orderkey
JOIN read_parquet('{{DATA_ROOT}}/nation/*.parquet') AS nation
  ON supplier.s_nationkey = nation.n_nationkey
WHERE nation.n_name = 'SAUDI ARABIA'
  AND orders.o_orderstatus = 'F'
  AND l1.l_receiptdate > l1.l_commitdate
  AND EXISTS (
    SELECT 1 FROM read_parquet('{{DATA_ROOT}}/lineitem/*.parquet') AS l2
    WHERE l2.l_orderkey = l1.l_orderkey
      AND l2.l_suppkey <> l1.l_suppkey
  )
  AND NOT EXISTS (
    SELECT 1 FROM read_parquet('{{DATA_ROOT}}/lineitem/*.parquet') AS l3
    WHERE l3.l_orderkey = l1.l_orderkey
      AND l3.l_suppkey <> l1.l_suppkey
      AND l3.l_receiptdate > l3.l_commitdate
  )
GROUP BY supplier.s_name
ORDER BY numwait DESC, supplier.s_name

-- name: q22-global-sales-opportunity
SELECT substring(customer.c_phone, 1, 2) AS country_code,
       count(*) AS numcust,
       sum(customer.c_acctbal) AS totacctbal
FROM read_parquet('{{DATA_ROOT}}/customer/*.parquet') AS customer
WHERE substring(customer.c_phone, 1, 2) IN ('13', '31', '23', '29', '30', '18', '17')
  AND customer.c_acctbal > (
    SELECT avg(customer2.c_acctbal)
    FROM read_parquet('{{DATA_ROOT}}/customer/*.parquet') AS customer2
    WHERE customer2.c_acctbal > 0
      AND substring(customer2.c_phone, 1, 2) IN ('13', '31', '23', '29', '30', '18', '17')
  )
  AND NOT EXISTS (
    SELECT 1 FROM read_parquet('{{DATA_ROOT}}/orders/*.parquet') AS orders
    WHERE orders.o_custkey = customer.c_custkey
  )
GROUP BY country_code
ORDER BY country_code
