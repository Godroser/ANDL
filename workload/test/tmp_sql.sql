SELECT
    l_orderkey,
    o_orderdate,
    o_shippriority
FROM
    customer,
    orders,
    lineitem,
    partsupp
WHERE
    c_mktsegment = 'HOUSEHOLD'
    AND c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_orderkey % 10 = 0
    AND o_orderdate < DATE '1995-03-14'
    AND l_shipdate > DATE '1995-03-14'
    AND ps_partkey = l_partkey
    AND ps_suppkey = l_suppkey
    AND l2_distance(ps_text_embedding, '{VECTOR}') < 0.925
ORDER BY
    l2_distance(ps_text_embedding, '{VECTOR}') APPROXIMATE
LIMIT {LIMIT}