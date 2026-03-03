-- TPC-H Vector Queries for SeekDB (tpch10_4 schema)
-- tpch10_4: 纯向量查询使用 part_vector / partsupp_vector，其他查询使用非 vector 表 (part, partsupp)
-- Placeholder: {VECTOR} will be replaced with actual vector string
-- Placeholder: {LIMIT} will be replaced with limit value
--
-- Schema: part, partsupp (无向量列), part_vector, partsupp_vector
-- partsupp_vector: ps_partkey, ps_suppkey, ps_image_embedding vector(96), ps_text_embedding vector(768)

--Q1
SELECT
    l_returnflag,
    l_linestatus,
    SUM(l_quantity) AS sum_qty,
    SUM(l_extendedprice) AS sum_base_price,
    SUM(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
    SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
    AVG(l_quantity) AS avg_qty,
    AVG(l_extendedprice) AS avg_price,
    AVG(l_discount) AS avg_disc,
    COUNT(*) AS count_order
FROM
    lineitem
WHERE
    l_shipdate <= DATE '1994-12-01' - INTERVAL 90 DAY
GROUP BY
    l_returnflag,
    l_linestatus
ORDER BY
    l_returnflag,
    l_linestatus;

--Q2
SELECT * FROM (
    SELECT 
        s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment,
        RANK() OVER (PARTITION BY p_partkey ORDER BY ps_supplycost ASC) as rk
    FROM 
        part p
        JOIN partsupp ps ON p.p_partkey = ps.ps_partkey
        JOIN supplier s ON s.s_suppkey = ps.ps_suppkey
        JOIN nation n ON s.s_nationkey = n.n_nationkey
        JOIN region r ON n.n_regionkey = r.r_regionkey
    WHERE 
        p_size = 15 
        AND p_type LIKE '%BRASS' 
        AND r_name = 'EUROPE'
) t
WHERE rk = 1
ORDER BY s_acctbal DESC, n_name, s_name, p_partkey
LIMIT 100;

-- Query Q3: Customer Order Priority (使用 partsupp_vector 做向量检索，JOIN 非 vector 表)
--Q3

SELECT
    l_orderkey,
    o_orderdate,
    o_shippriority
FROM
    customer,
    orders,
    lineitem,
    partsupp ps,
    partsupp_vector psv
WHERE
    c_mktsegment = 'HOUSEHOLD'
    AND c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND o_orderdate < DATE '1995-03-14'
    AND l_shipdate > DATE '1995-03-14'
    AND ps.ps_partkey = l_partkey
    AND ps.ps_suppkey = l_suppkey
    AND psv.ps_partkey = ps.ps_partkey
    AND psv.ps_suppkey = ps.ps_suppkey
    AND l2_distance(psv.ps_text_embedding, '{VECTOR}') < 0.925
ORDER BY
    l2_distance(psv.ps_text_embedding, '{VECTOR}') APPROXIMATE
LIMIT {LIMIT}

--Q4
SELECT /*+ PARALLEL(8) */
    o_orderpriority,
    COUNT(*) AS order_count
FROM
    orders
WHERE
    o_orderdate >= DATE '1993-07-01' 
    AND o_orderdate < DATE '1993-07-01' + INTERVAL 3 MONTH
    AND EXISTS (
        SELECT
            *
        FROM
            lineitem
        WHERE
            l_orderkey = o_orderkey
            AND l_commitdate < l_receiptdate
    )
GROUP BY
    o_orderpriority
ORDER BY
    o_orderpriority;

-- Query Q5: Supplier Revenue (使用 partsupp_vector)
--Q5
SELECT
    n_name
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region,
    partsupp ps,
    partsupp_vector psv
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'MIDDLE EAST'
    AND o_orderdate >= DATE '1993-01-01'
    AND o_orderdate < DATE '1993-01-01' + INTERVAL 1 YEAR
    AND ps.ps_partkey = l_partkey
    AND ps.ps_suppkey = l_suppkey
    AND psv.ps_partkey = ps.ps_partkey
    AND psv.ps_suppkey = ps.ps_suppkey
    AND l2_distance(psv.ps_text_embedding, '{VECTOR}') < 0.925
ORDER BY
    l2_distance(psv.ps_text_embedding, '{VECTOR}') APPROXIMATE
LIMIT {LIMIT}

--Q6
SELECT /*+ PARALLEL(8) */
    SUM(l_extendedprice * l_discount) AS revenue
FROM
    lineitem
WHERE
    l_shipdate >= DATE '1994-01-01'
    AND l_shipdate < DATE '1994-01-01' + INTERVAL '1' YEAR
    AND l_discount BETWEEN 0.06 - 0.01 AND 0.06 + 0.01
    AND l_quantity < 24;

--Q7
SELECT /*+ PARALLEL(8) LEADING(n1, s, l, o, c, n2) USE_HASH(s l o c n2) */
    supp_nation,
    cust_nation,
    l_year,
    SUM(volume) AS revenue
FROM
    (
        SELECT
            n1.n_name AS supp_nation,
            n2.n_name AS cust_nation,
            EXTRACT(YEAR FROM l_shipdate) AS l_year,
            l_extendedprice * (1 - l_discount) AS volume
        FROM
            supplier s,
            lineitem l,
            orders o,
            customer c,
            nation n1,
            nation n2
        WHERE
            s.s_suppkey = l.l_suppkey
            AND o.o_orderkey = l.l_orderkey
            AND c.c_custkey = o.o_custkey
            AND s.s_nationkey = n1.n_nationkey
            AND c.c_nationkey = n2.n_nationkey
            AND (
                (n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY')
                OR (n1.n_name = 'GERMANY' AND n2.n_name = 'FRANCE')
            )
            AND l.l_shipdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
    ) AS shipping
GROUP BY
    supp_nation,
    cust_nation,
    l_year
ORDER BY
    supp_nation,
    cust_nation,
    l_year;

-- Query Q8: Market Share (使用 partsupp_vector)
--Q8

SELECT
    o_year,
    SUM(CASE
        WHEN nation = 'KENYA' THEN volume
        ELSE 0
    END) / SUM(volume) AS mkt_share
FROM
    (
        SELECT
            EXTRACT(YEAR FROM o_orderdate) AS o_year,
            l_extendedprice * (1 - l_discount) AS volume,
            n2.n_name AS nation
        FROM
            part,
            supplier,
            lineitem,
            orders,
            customer,
            nation n1,
            nation n2,
            region,
            partsupp ps,
            partsupp_vector psv
        WHERE
            p_partkey = l_partkey
            AND s_suppkey = l_suppkey
            AND ps.ps_partkey = l_partkey
            AND ps.ps_suppkey = l_suppkey
            AND psv.ps_partkey = ps.ps_partkey
            AND psv.ps_suppkey = ps.ps_suppkey
            AND l_orderkey = o_orderkey
            AND o_custkey = c_custkey
            AND c_nationkey = n1.n_nationkey
            AND n1.n_regionkey = r_regionkey
            AND r_name = 'MIDDLE EAST'
            AND s_nationkey = n2.n_nationkey
            AND o_orderdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
            AND p_type = 'ECONOMY BRUSHED BRASS'
            AND l2_distance(psv.ps_text_embedding, '{VECTOR}') < 0.925
        ORDER BY
            l2_distance(psv.ps_text_embedding, '{VECTOR}') APPROXIMATE
        LIMIT {LIMIT}
    ) AS all_nations
GROUP BY
    o_year
ORDER BY
    o_year
LIMIT 1

-- Query Q9: Product Type Profit Measure (使用 partsupp_vector)
--Q9

SELECT
    nation,
    o_year,
    SUM(amount) AS sum_profit
FROM
    (
        SELECT
            n_name AS nation,
            EXTRACT(YEAR FROM o_orderdate) AS o_year,
            l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity AS amount
        FROM
            part,
            supplier,
            lineitem,
            partsupp ps,
            partsupp_vector psv,
            orders,
            nation
        WHERE
            s_suppkey = l_suppkey
            AND ps.ps_suppkey = l_suppkey
            AND ps.ps_partkey = l_partkey
            AND p_partkey = l_partkey
            AND psv.ps_partkey = ps.ps_partkey
            AND psv.ps_suppkey = ps.ps_suppkey
            AND o_orderkey = l_orderkey
            AND s_nationkey = n_nationkey
            AND p_name LIKE '%almond%'
            AND l2_distance(psv.ps_text_embedding, '{VECTOR}') < 0.925
    ) AS profit
GROUP BY
    nation,
    o_year
ORDER BY
    nation,
    o_year DESC
LIMIT 1

-- Query Q10: Customer Return (使用 partsupp_vector)
--Q10

SELECT
    c_custkey,
    c_name,
    c_acctbal,
    n_name,
    c_address,
    c_phone,
    c_comment
FROM
    customer,
    orders,
    lineitem,
    nation,
    partsupp ps,
    partsupp_vector psv
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND o_orderdate >= DATE '1993-11-01'
    AND o_orderdate < DATE '1993-11-01' + INTERVAL 3 MONTH
    AND l_returnflag = 'R'
    AND c_nationkey = n_nationkey
    AND ps.ps_partkey = l_partkey
    AND ps.ps_suppkey = l_suppkey
    AND psv.ps_partkey = ps.ps_partkey
    AND psv.ps_suppkey = ps.ps_suppkey
    AND l2_distance(psv.ps_text_embedding, '{VECTOR}') < 0.925
ORDER BY
    l2_distance(psv.ps_text_embedding, '{VECTOR}') APPROXIMATE
LIMIT {LIMIT}

-- Query Q11: Supplier Parts (使用 partsupp_vector，纯向量相关)
--Q11

SELECT
    psv.ps_partkey
FROM
    partsupp_vector psv,
    supplier,
    nation
WHERE
    psv.ps_suppkey = s_suppkey
    AND s_nationkey = n_nationkey
    AND n_name = 'ARGENTINA'
    AND l2_distance(psv.ps_text_embedding, '{VECTOR}') < 0.925
ORDER BY
    l2_distance(psv.ps_text_embedding, '{VECTOR}') APPROXIMATE
LIMIT {LIMIT}

-- Query Q12: Shipping Modes (使用 partsupp_vector)
--Q12

SELECT
    l_shipmode
FROM
    orders,
    lineitem,
    partsupp ps,
    partsupp_vector psv
WHERE
    o_orderkey = l_orderkey
    AND l_shipmode IN ('RAIL', 'SHIP')
    AND l_commitdate < l_receiptdate
    AND l_shipdate < l_commitdate
    AND l_receiptdate >= DATE '1994-01-01'
    AND l_receiptdate < DATE '1994-01-01' + INTERVAL 1 YEAR
    AND ps.ps_partkey = l_partkey
    AND ps.ps_suppkey = l_suppkey
    AND psv.ps_partkey = ps.ps_partkey
    AND psv.ps_suppkey = ps.ps_suppkey
    AND l2_distance(psv.ps_text_embedding, '{VECTOR}') < 0.925
ORDER BY
    l2_distance(psv.ps_text_embedding, '{VECTOR}') APPROXIMATE
LIMIT {LIMIT}

--Q13
SELECT /*+ PARALLEL(8) */
    c_count,
    COUNT(*) AS custdist
FROM
    (
        SELECT
            c_custkey,
            COUNT(o_orderkey) AS c_count
        FROM
            customer 
            LEFT OUTER JOIN orders ON 
                c_custkey = o_custkey
                AND o_comment NOT LIKE '%special%requests%'
        GROUP BY
            c_custkey
    ) AS c_orders
GROUP BY
    c_count
ORDER BY
    custdist DESC,
    c_count DESC;

--Q14
SELECT /*+ PARALLEL(8) */
    100.00 * SUM(CASE
        WHEN p.p_type LIKE 'PROMO%'
            THEN l.l_extendedprice * (1 - l.l_discount)
        ELSE 0
    END) / SUM(l.l_extendedprice * (1 - l.l_discount)) AS promo_revenue
FROM
    lineitem l,
    part p
WHERE
    l.l_partkey = p.p_partkey
    AND l.l_shipdate >= DATE '1995-09-01'
    AND l.l_shipdate < DATE '1995-09-01' + INTERVAL '1' MONTH;

--Q15
CREATE OR REPLACE VIEW revenue_s (supplier_no, total_revenue) AS
    SELECT
        l_suppkey,
        SUM(l_extendedprice * (1 - l_discount))
    FROM
        lineitem
    WHERE
        l_shipdate >= DATE '1996-01-01'
        AND l_shipdate < DATE '1996-01-01' + INTERVAL 3 MONTH
    GROUP BY
        l_suppkey;

SELECT /*+ PARALLEL(8) */
    s_suppkey,
    s_name,
    s_address,
    s_phone,
    total_revenue
FROM
    supplier,
    revenue_s
WHERE
    s_suppkey = supplier_no
    AND total_revenue = (
        SELECT
            MAX(total_revenue)
        FROM
            revenue_s
    )
ORDER BY
    s_suppkey;

DROP VIEW revenue_s;

--Q16
SELECT /*+ PARALLEL(8) USE_HASH(p ps) */
    p_brand,
    p_type,
    p_size,
    COUNT(DISTINCT ps_suppkey) AS supplier_cnt
FROM
    partsupp ps,
    part p
WHERE
    p.p_partkey = ps.ps_partkey
    AND p.p_brand <> 'Brand#45'
    AND p.p_type NOT LIKE 'MEDIUM POLISHED%'
    AND p.p_size IN (49, 14, 23, 45, 19, 3, 36, 9)
    AND ps.ps_suppkey NOT IN (
        SELECT
            s_suppkey
        FROM
            supplier
        WHERE
            s_comment LIKE '%Customer%Complaints%'
    )
GROUP BY
    p_brand,
    p_type,
    p_size
ORDER BY
    supplier_cnt DESC,
    p_brand,
    p_type,
    p_size;

--Q17
SELECT /*+ PARALLEL(8) */
    SUM(l.l_extendedprice) / 7.0 AS avg_yearly
FROM
    lineitem l,
    part p
WHERE
    p.p_partkey = l.l_partkey
    AND p.p_brand = 'Brand#23'
    AND p.p_container = 'MED BOX'
    AND l.l_quantity < (
        SELECT
            0.2 * AVG(l2.l_quantity)
        FROM
            lineitem l2
        WHERE
            l2.l_partkey = p.p_partkey
    );

-- Query Q18: Large Volume Customer (使用 partsupp_vector)
--Q18
SELECT
    c_name,
    c_custkey,
    o_orderkey,
    o_orderdate,
    o_totalprice,
    l_quantity
FROM
    customer,
    orders,
    lineitem,
    partsupp ps,
    partsupp_vector psv
WHERE
    o_orderkey IN (
        SELECT
            l_orderkey
        FROM
            lineitem
        GROUP BY
            l_orderkey
        HAVING
            SUM(l_quantity) > 260
    )
    AND c_custkey = o_custkey
    AND o_orderkey = l_orderkey
    AND ps.ps_partkey = l_partkey
    AND ps.ps_suppkey = l_suppkey
    AND psv.ps_partkey = ps.ps_partkey
    AND psv.ps_suppkey = ps.ps_suppkey
    AND l2_distance(psv.ps_text_embedding, '{VECTOR}') < 0.925
ORDER BY
    l2_distance(psv.ps_text_embedding, '{VECTOR}') APPROXIMATE
LIMIT {LIMIT}

--Q19
SELECT /*+ PARALLEL(8) USE_HASH(l p) */
    SUM(l_extendedprice * (1 - l_discount)) AS revenue
FROM
    lineitem l,
    part p
WHERE
    p.p_partkey = l.l_partkey
    AND (
        (
            p.p_brand = 'Brand#12'
            AND p.p_container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
            AND l.l_quantity >= 1
            AND l.l_quantity <= 1 + 10
            AND p.p_size BETWEEN 1 AND 5
            AND l.l_shipmode IN ('AIR', 'AIR REG')
            AND l.l_shipinstruct = 'DELIVER IN PERSON'
        )
        OR
        (
            p.p_brand = 'Brand#23'
            AND p.p_container IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
            AND l.l_quantity >= 10
            AND l.l_quantity <= 10 + 10
            AND p.p_size BETWEEN 1 AND 10
            AND l.l_shipmode IN ('AIR', 'AIR REG')
            AND l.l_shipinstruct = 'DELIVER IN PERSON'
        )
        OR
        (
            p.p_brand = 'Brand#34'
            AND p.p_container IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
            AND l.l_quantity >= 20
            AND l.l_quantity <= 20 + 10
            AND p.p_size BETWEEN 1 AND 15
            AND l.l_shipmode IN ('AIR', 'AIR REG')
            AND l.l_shipinstruct = 'DELIVER IN PERSON'
        )
    );

-- Query Q20: Supplier Parts Availability (使用 partsupp_vector)
--Q20

SELECT
    s_name,
    s_address,
    n_name
FROM
    supplier,
    nation
WHERE
    s_suppkey IN (
        SELECT
            psv.ps_suppkey
        FROM
            partsupp_vector psv,
            partsupp ps,
            (
                SELECT
                    l_partkey AS agg_partkey,
                    l_suppkey AS agg_suppkey,
                    0.5 * SUM(l_quantity) AS agg_quantity
                FROM
                    lineitem
                WHERE
                    l_shipdate >= DATE '1993-01-01'
                    AND l_shipdate < DATE '1993-01-01' + INTERVAL '1' YEAR
                GROUP BY
                    l_partkey,
                    l_suppkey
            ) agg_lineitem
        WHERE
            agg_partkey = psv.ps_partkey
            AND agg_suppkey = psv.ps_suppkey
            AND psv.ps_partkey = ps.ps_partkey
            AND psv.ps_suppkey = ps.ps_suppkey
            AND ps.ps_partkey IN (
                SELECT
                    p_partkey
                FROM
                    part
                WHERE
                    p_name LIKE 'almond%'
            )
            AND ps_availqty > agg_quantity
            AND l2_distance(psv.ps_text_embedding, '{VECTOR}') < 0.925
    )
    AND s_nationkey = n_nationkey
    AND n_name = 'ALGERIA'
ORDER BY
    s_name
LIMIT 1

--Q21
SELECT /*+ PARALLEL(8) LEADING(n, s, l1, o) USE_HASH(s l1 o) */
    s_name,
    COUNT(*) AS numwait
FROM
    supplier s,
    lineitem l1,
    orders o,
    nation n
WHERE
    s.s_suppkey = l1.l_suppkey
    AND o.o_orderkey = l1.l_orderkey
    AND o.o_orderstatus = 'F'
    AND l1.l_receiptdate > l1.l_commitdate
    AND EXISTS (
        SELECT
            1
        FROM
            lineitem l2
        WHERE
            l2.l_orderkey = l1.l_orderkey
            AND l2.l_suppkey <> l1.l_suppkey
    )
    AND NOT EXISTS (
        SELECT
            1
        FROM
            lineitem l3
        WHERE
            l3.l_orderkey = l1.l_orderkey
            AND l3.l_suppkey <> l1.l_suppkey
            AND l3.l_receiptdate > l3.l_commitdate
    )
    AND s.s_nationkey = n.n_nationkey
    AND n.n_name = 'SAUDI ARABIA'
GROUP BY
    s_name
ORDER BY
    numwait DESC,
    s_name
LIMIT 100;

--Q22
SELECT /*+ PARALLEL(8) */
    cntrycode,
    COUNT(*) AS numcust,
    SUM(c_acctbal) AS totacctbal
FROM
    (
        SELECT
            SUBSTRING(c_phone, 1, 2) AS cntrycode,
            c_acctbal
        FROM
            customer c
        WHERE
            SUBSTRING(c_phone, 1, 2) IN ('13', '31', '23', '29', '30', '18', '17')
            AND c_acctbal > (
                SELECT
                    AVG(c_acctbal)
                FROM
                    customer
                WHERE
                    c_acctbal > 0.00
                    AND SUBSTRING(c_phone, 1, 2) IN ('13', '31', '23', '29', '30', '18', '17')
            )
            AND NOT EXISTS (
                SELECT
                    1
                FROM
                    orders o
                WHERE
                    o.o_custkey = c.c_custkey
            )
    ) AS custsale
GROUP BY
    cntrycode
ORDER BY
    cntrycode;
