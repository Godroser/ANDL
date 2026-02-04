# %% [markdown]
# # Basic

# %%
import pandas as pd
from tqdm import tqdm
import numpy as np
np.random.seed(0)

from rich.traceback import install
install()

# %%
import mysql.connector

conn = mysql.connector.connect(
    host='127.0.0.1',
    port=10200,
    user='root',
    database='tpch'
)
cur = conn.cursor()
conn.autocommit = True

# %%
def read_fbin(filename, start_idx=0, chunk_size=None):
    """Read *.fbin file that contains float32 vectors."""
    with open(filename, "rb") as f:
        nvecs, dim = np.fromfile(f, count=2, dtype=np.int32)
        # Seek to the correct position: 8 bytes (header) + start_idx * dim * 4 bytes (float32)
        f.seek(8 + start_idx * dim * 4)
        # Read all available data
        arr = np.fromfile(f, dtype=np.float32)
        # Calculate actual number of vectors that can be formed
        actual_nvecs = len(arr) // dim
        if chunk_size is not None:
            actual_nvecs = min(actual_nvecs, chunk_size)
        n_fetch = min(actual_nvecs, nvecs - start_idx) if chunk_size is None else min(actual_nvecs, chunk_size)
        print(f"{filename}: nvecs={nvecs}, dim={dim}, start_idx={start_idx}, requested={chunk_size if chunk_size else nvecs-start_idx}, actual_read={actual_nvecs}")
        # Only keep the vectors we can fully form
        arr = arr[:n_fetch * dim]
    return arr.reshape(-1, dim)

# Use WIKI queries (768 dim) to match ps_text_embedding dimension
# deep = read_fbin('/data/dzh/seekdb/Exqutor/Vector-augmented_SQL_analytics/DEEP/query.public.10K.fbin')  # 96 dim, for ps_image_embedding
deep = read_fbin('/data/dzh/seekdb/Exqutor/Vector-augmented_SQL_analytics/WIKI/queries.fbin')  # 768 dim, for ps_text_embedding

# %%
import re
import time

def get_time(result):
    # For MySQL/SeekDB, the explain output format may differ from PostgreSQL
    # This function may need adjustment based on actual output format
    planning_time = 0.0
    execution_time = 0.0
    
    # Try to extract timing information from result
    # Adjust regex patterns based on actual MySQL/SeekDB explain output
    for row in result:
        if isinstance(row, (list, tuple)) and len(row) > 0:
            row_str = str(row[0])
            if 'time' in row_str.lower() or 'ms' in row_str.lower():
                # Extract numeric values
                time_match = re.search(r'(\d+\.?\d*)\s*ms', row_str, re.IGNORECASE)
                if time_match:
                    execution_time = float(time_match.group(1))
                    break
    
    total_time = planning_time + execution_time
    return total_time if total_time > 0 else 0.0

# %% [markdown]
# # TPC-H query sampling

# %%
def vector_to_string(vector):
    """Convert numpy array to SeekDB vector string format: '[1.0,2.0,3.0]'"""
    return '[' + ','.join(str(float(x)) for x in vector) + ']'

def run(query, num):
    # Remove EXPLAIN from query for actual execution
    query_exec = query.replace('EXPLAIN', '').strip()
    # First run EXPLAIN to show plan
    cur.execute(query)
    r = cur.fetchall()
    print("Query plan:")
    for row in r:
        print(row)
    print()
    
    # Measure actual execution time
    time_result = []
    for i in range(num):
        start_time = time.time()
        cur.execute(query_exec)
        r = cur.fetchall()
        end_time = time.time()
        exec_time_ms = (end_time - start_time) * 1000  # Convert to milliseconds
        
        for row in r[:5]:  # Print first 5 rows only
            print(row[0] if len(row) > 0 else row)
        if len(r) > 5:
            print(f"... ({len(r)} total rows)")
        
        time_result.append(exec_time_ms)
        print(f"Execution time: {exec_time_ms:.2f} ms")
        print()
    
    print("All execution times:", time_result)
    if len(time_result) > 2:
        trimmed = sorted(time_result)[1:-1]
        print(f"Mean (trimmed): {np.mean(trimmed):.2f} ms, Std: {np.std(trimmed):.2f} ms")
    else:
        print(f"Mean: {np.mean(time_result):.2f} ms, Std: {np.std(time_result):.2f} ms")
    return time_result

num = 10
total_result = []

# %%
# Note: Changed ps_embedding to ps_text_embedding based on insert_data_pgvector.py
# Also changed index name from partsupp_deep_hnsw to partsupp_wiki_hnsw
# Changed <-> operator to l2_distance() function for SeekDB
query_vec_str = vector_to_string(deep[0])
query = f"""
    EXPLAIN
    SELECT
        l2_distance(ps_text_embedding, '{query_vec_str}') AS distance
    FROM
        partsupp
    WHERE
        l2_distance(ps_text_embedding, '{query_vec_str}') < 0.925
    ORDER BY
        distance APPROXIMATE LIMIT 10
"""

cur.execute(query)
r = cur.fetchall()
print(len(r))
for row in r:
    print(row)

# %%
for r in query.split('\n'):
    print(r)

# %% [markdown]
# ## Q3

# %%
query_vec_str = vector_to_string(deep[0])
query = f"""
    EXPLAIN
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
        AND o_orderdate < DATE '1995-03-14'
        AND l_shipdate > DATE '1995-03-14'
        AND ps_partkey = l_partkey
        AND ps_suppkey = l_suppkey
        AND l2_distance(ps_text_embedding, '{query_vec_str}') < 0.925
    ORDER BY
        l2_distance(ps_text_embedding, '{query_vec_str}') APPROXIMATE LIMIT 10
"""

time_result = run(query, num)
total_result.append(time_result)

# %% [markdown]
# ## Q5

# %%

query_vec_str = vector_to_string(deep[0])
query = f"""
    EXPLAIN
    SELECT
        n_name
    FROM
        customer,
        orders,
        lineitem,
        supplier,
        nation,
        region,
        partsupp
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
        AND ps_partkey = l_partkey
        AND ps_suppkey = l_suppkey
        AND l2_distance(ps_text_embedding, '{query_vec_str}') < 0.925
    ORDER BY
        l2_distance(ps_text_embedding, '{query_vec_str}') APPROXIMATE LIMIT 10
"""

time_result = run(query, num)
total_result.append(time_result)

# %% [markdown]
# ## Q8

# %%

query = f"""
    EXPLAIN
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
                partsupp
            WHERE
                p_partkey = l_partkey
                AND s_suppkey = l_suppkey
                AND ps_partkey = l_partkey
                AND ps_suppkey = l_suppkey
                AND l_orderkey = o_orderkey
                AND o_custkey = c_custkey
                AND c_nationkey = n1.n_nationkey
                AND n1.n_regionkey = r_regionkey
                AND r_name = 'MIDDLE EAST'
                AND s_nationkey = n2.n_nationkey
                AND o_orderdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
                AND p_type = 'ECONOMY BRUSHED BRASS'
                AND l2_distance(ps_text_embedding, '{vector_to_string(deep[0])}') < 0.925
            ORDER BY
                l2_distance(ps_text_embedding, '{vector_to_string(deep[0])}') APPROXIMATE LIMIT 10
        ) as all_nations
    GROUP BY
        o_year
    ORDER BY
        o_year
    LIMIT 1
"""

time_result = run(query, num)
total_result.append(time_result)

# %% [markdown]
# ## Q9

# %%

query = f"""
EXPLAIN
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
            partsupp,
            orders,
            nation
        WHERE
            s_suppkey = l_suppkey
            AND ps_suppkey = l_suppkey
            AND ps_partkey = l_partkey
            AND p_partkey = l_partkey
            AND o_orderkey = l_orderkey
            AND s_nationkey = n_nationkey
            AND p_name LIKE '%almond%'
            -- 向量过滤条件保留
            AND l2_distance(ps_text_embedding, '{vector_to_string(deep[0])}') < 0.925
    ) AS profit
GROUP BY
    nation,
    o_year
ORDER BY
    nation,
    o_year DESC
LIMIT 1
"""
# query = f"""
#     EXPLAIN
#     SELECT
#         nation,
#         o_year,
#         SUM(amount) AS sum_profit
#     FROM
#         (
#             SELECT
#                 n_name AS nation,
#                 EXTRACT(YEAR FROM o_orderdate) AS o_year,
#                 l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity AS amount
#             FROM
#                 part,
#                 supplier,
#                 lineitem,
#                 partsupp,
#                 orders,
#                 nation
#             WHERE
#                 s_suppkey = l_suppkey
#                 AND ps_suppkey = l_suppkey
#                 AND ps_partkey = l_partkey
#                 AND p_partkey = l_partkey
#                 AND o_orderkey = l_orderkey
#                 AND s_nationkey = n_nationkey
#                 AND p_name LIKE '%almond%'
#                 AND l2_distance(ps_text_embedding, '{vector_to_string(deep[0])}') < 0.925
#             ORDER BY
#                 l2_distance(ps_text_embedding, '{vector_to_string(deep[0])}') APPROXIMATE
#         ) as profit
#     GROUP BY
#         nation,
#         o_year
#     ORDER BY
#         nation,
#         o_year DESC
#     LIMIT 1
# """

time_result = run(query, num)
total_result.append(time_result)

# %% [markdown]
# ## Q10

# %%

query = f"""
    EXPLAIN
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
        partsupp
    WHERE
        c_custkey = o_custkey
        AND l_orderkey = o_orderkey
        AND o_orderdate >= DATE '1993-11-01'
        AND o_orderdate < DATE '1993-11-01' + INTERVAL 3 MONTH
        AND l_returnflag = 'R'
        AND c_nationkey = n_nationkey
        AND ps_partkey = l_partkey
        AND ps_suppkey = l_suppkey
        AND l2_distance(ps_text_embedding, '{vector_to_string(deep[0])}') < 0.925
    ORDER BY
        l2_distance(ps_text_embedding, '{vector_to_string(deep[0])}') APPROXIMATE LIMIT 10
"""

time_result = run(query, num)
total_result.append(time_result)

# %% [markdown]
# ## Q11

# %%
query = f"""
    EXPLAIN
    SELECT
        ps_partkey
    FROM
        partsupp,
        supplier,
        nation
    WHERE
        ps_suppkey = s_suppkey
        AND s_nationkey = n_nationkey
        AND n_name = 'ARGENTINA'
        AND l2_distance(ps_text_embedding, '{vector_to_string(deep[0])}') < 0.925
    ORDER BY
        l2_distance(ps_text_embedding, '{vector_to_string(deep[0])}') APPROXIMATE LIMIT 10
"""

time_result = run(query, num)
total_result.append(time_result)

# %% [markdown]
# ## Q12

# %%

query = f"""
    EXPLAIN
    SELECT
        l_shipmode
    FROM
        orders,
        lineitem,
        partsupp
    WHERE
        o_orderkey = l_orderkey
        AND l_shipmode IN ('RAIL', 'SHIP')
        AND l_commitdate < l_receiptdate
        AND l_shipdate < l_commitdate
        AND l_receiptdate >= DATE '1994-01-01'
        AND l_receiptdate < DATE '1994-01-01' + INTERVAL 1 YEAR
        AND ps_partkey = l_partkey
        AND ps_suppkey = l_suppkey
        AND l2_distance(ps_text_embedding, '{vector_to_string(deep[0])}') < 0.925
    ORDER BY
        l2_distance(ps_text_embedding, '{vector_to_string(deep[0])}') APPROXIMATE LIMIT 10
"""

time_result = run(query, num)
total_result.append(time_result)

# %% [markdown]
# ## Q18

# %%

query = f"""
    EXPLAIN
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
        partsupp
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
        AND ps_partkey = l_partkey
        AND ps_suppkey = l_suppkey
        AND l2_distance(ps_text_embedding, '{vector_to_string(deep[0])}') < 0.925
    ORDER BY
        l2_distance(ps_text_embedding, '{vector_to_string(deep[0])}') APPROXIMATE LIMIT 10
""" 

# time_result = run(query, num)
# total_result.append(time_result)

# %% [markdown]
# ## Q20

# %%

query = f"""
    EXPLAIN
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
                ps_suppkey
            FROM
                partsupp,
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
                agg_partkey = ps_partkey
                AND agg_suppkey = ps_suppkey
                AND ps_partkey IN (
                    SELECT
                        p_partkey
                    FROM
                        part
                    WHERE
                        p_name LIKE 'almond%'
                )
                AND ps_availqty > agg_quantity
                AND l2_distance(ps_text_embedding, '{vector_to_string(deep[0])}') < 0.925
        )
        AND s_nationkey = n_nationkey
        AND n_name = 'ALGERIA'
    ORDER BY
        s_name
    LIMIT 1
"""

# query = f"""
#     EXPLAIN
#     SELECT
#         s_name,
#         s_address,
#         n_name
#     FROM
#         supplier,
#         nation
#     WHERE
#         s_suppkey IN (
#             SELECT
#                 ps_suppkey
#             FROM
#                 partsupp,
#                 (
#                     SELECT
#                         l_partkey AS agg_partkey,
#                         l_suppkey AS agg_suppkey,
#                         0.5 * SUM(l_quantity) AS agg_quantity
#                     FROM
#                         lineitem
#                     WHERE
#                         l_shipdate >= DATE '1993-01-01'
#                         AND l_shipdate < DATE '1993-01-01' + INTERVAL 1 YEAR
#                     GROUP BY
#                         l_partkey,
#                         l_suppkey
#                 ) agg_lineitem
#             WHERE
#                 agg_partkey = ps_partkey
#                 AND agg_suppkey = ps_suppkey
#                 AND ps_partkey IN (
#                     SELECT
#                         p_partkey
#                     FROM
#                         part
#                     WHERE
#                         p_name LIKE 'almond%'
#                 )
#                 AND ps_availqty > agg_quantity
#                 AND l2_distance(ps_text_embedding, '{vector_to_string(deep[0])}') < 0.925
#             ORDER BY
#                 l2_distance(ps_text_embedding, '{vector_to_string(deep[0])}') APPROXIMATE
#         )
#         AND s_nationkey = n_nationkey
#         AND n_name = 'ALGERIA'
#     ORDER BY
#         s_name
#     LIMIT 1
# """
    
time_result = run(query, num)
total_result.append(time_result)

# %%

# %%
# np.save(filename, total_result)
print(total_result)

# %%
for t in total_result:
    for tt in t:
        print(tt, end='\t')
    print()

# %%



