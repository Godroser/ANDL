import pandas as pd
import numpy as np
import mysql.connector
from decimal import Decimal
from concurrent.futures import ThreadPoolExecutor

# File paths
partsupp_csv_path = '/data/dzh/seekdb/workload/tpch0.1/partsupp.csv'
part_csv_path = '/data/dzh/seekdb/workload/tpch0.1/part.csv'
# deep_bin_path = '/data/dzh/seekdb/Exqutor/Vector-augmented_SQL_analytics/DEEP/base.1B.fbin'  # Not processed yet
wiki_bin_path = '/data/dzh/seekdb/Exqutor/Vector-augmented_SQL_analytics/WIKI/base.10M.fbin'

# DB connection
conn = mysql.connector.connect(
    host='127.0.0.1',
    port=10200,
    user='root',
    database='tpch'
)
conn.autocommit = False  # MySQL connector uses autocommit differently

def read_fbin(filename, start_idx=0, chunk_size=None):
    with open(filename, "rb") as f:
        nvecs, dim = np.fromfile(f, count=2, dtype=np.int32)
        n_fetch = (nvecs - start_idx) if chunk_size is None else chunk_size
        arr = np.fromfile(f, count=n_fetch * dim, dtype=np.float32, offset=start_idx * 4 * dim)
    return arr.reshape(-1, dim)

def read_csv(path, columns):
    df = pd.read_csv(path, delimiter='|', header=None, engine="pyarrow")
    df = df.iloc[:, :columns]
    return df

def vector_to_string(vector):
    """Convert numpy array to SeekDB vector string format: '[1.0,2.0,3.0]'"""
    return '[' + ','.join(str(float(x)) for x in vector) + ']'

# --- PARTSUPP TABLE ---
PARTSUPP_TABLE = "partsupp"
with conn.cursor() as cur:
    cur.execute(f"DROP TABLE IF EXISTS {PARTSUPP_TABLE};")
    cur.execute(f"""
        CREATE TABLE {PARTSUPP_TABLE} (
            ps_partkey      INTEGER NOT NULL,
            ps_suppkey      INTEGER NOT NULL,
            ps_availqty     INTEGER,
            ps_supplycost   DECIMAL,
            ps_comment      VARCHAR(199),
            ps_image_embedding vector(96),
            ps_text_embedding vector(768)
        );
    """)
    conn.commit()

partsupp_df = read_csv(partsupp_csv_path, 5)
num_rows = len(partsupp_df)
chunk_size = 10000
num_chunks = (num_rows + chunk_size - 1) // chunk_size

def process_partsupp_chunk(chunk_idx, chunk_size, text_embedding_file, partsupp_df):
    start_index = chunk_idx * chunk_size
    end_index = min(start_index + chunk_size, len(partsupp_df))
    df_chunk = partsupp_df.iloc[start_index:end_index].copy()

    # Read embeddings (only text embedding, image embedding not processed yet)
    # image_vectors = read_fbin(image_embedding_file, start_idx=start_index, chunk_size=(end_index - start_index))
    text_vectors = read_fbin(text_embedding_file, start_idx=start_index, chunk_size=(end_index - start_index))

    # Convert vectors to string format for SeekDB
    # df_chunk['ps_image_embedding'] = [vector_to_string(v) for v in image_vectors]
    df_chunk['ps_image_embedding'] = None  # Not processed yet
    df_chunk['ps_text_embedding'] = [vector_to_string(v) for v in text_vectors]

    # Prepare INSERT statements
    insert_sql = f"""
        INSERT INTO {PARTSUPP_TABLE}
        (ps_partkey, ps_suppkey, ps_availqty, ps_supplycost, ps_comment, ps_image_embedding, ps_text_embedding)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    
    rows = []
    for row in df_chunk.itertuples(index=False, name=None):
        # Handle NULL for ps_image_embedding
        image_emb = row[5] if row[5] is not None else None
        rows.append((
            int(row[0]), int(row[1]), int(row[2]), Decimal(str(row[3])), str(row[4]),
            image_emb, row[6]  # ps_image_embedding (None for NULL), ps_text_embedding (string format)
        ))

    # Create a new connection for each thread
    thread_conn = mysql.connector.connect(
        host='127.0.0.1',
        port=10200,
        user='root',
        database='tpch'
    )
    try:
        # Insert in smaller batches to avoid max_allowed_packet error
        # Vector data is large (768 dimensions), so use smaller batch size
        batch_size = 100  # Insert 100 rows at a time
        with thread_conn.cursor() as cur:
            for i in range(0, len(rows), batch_size):
                batch = rows[i:i + batch_size]
                cur.executemany(insert_sql, batch)
            thread_conn.commit()
    finally:
        thread_conn.close()

with ThreadPoolExecutor(max_workers=8) as executor:
    futures = [
        executor.submit(process_partsupp_chunk, i, chunk_size, wiki_bin_path, partsupp_df)
        for i in range(num_chunks)
    ]
    for fut in futures:
        fut.result()

# --- PART TABLE ---
PART_TABLE = "part"
with conn.cursor() as cur:
    cur.execute(f"DROP TABLE IF EXISTS {PART_TABLE};")
    cur.execute(f"""
        CREATE TABLE {PART_TABLE} (
            p_partkey      INTEGER NOT NULL,
            p_name         VARCHAR(55),
            p_mfgr         VARCHAR(25),
            p_brand        VARCHAR(10),
            p_type         VARCHAR(25),
            p_size         INTEGER,
            p_container    VARCHAR(10),
            p_retailprice  DECIMAL,
            p_comment      VARCHAR(23),
            text_embedding vector(768)
        );
    """)
    conn.commit()

part_df = read_csv(part_csv_path, 9)
num_part_rows = len(part_df)
wiki_vectors = read_fbin(wiki_bin_path, start_idx=0, chunk_size=num_part_rows)
part_df['text_embedding'] = [vector_to_string(v) for v in wiki_vectors]

# Prepare INSERT statements
insert_sql = f"""
    INSERT INTO {PART_TABLE}
    (p_partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice, p_comment, text_embedding)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

rows = [
    (
        int(row[0]), str(row[1]), str(row[2]), str(row[3]), str(row[4]),
        int(row[5]), str(row[6]), Decimal(str(row[7])), str(row[8]), row[9]  # text_embedding already in string format
    )
    for row in part_df.itertuples(index=False, name=None)
]

# Insert in smaller batches to avoid max_allowed_packet error
# Vector data is large (768 dimensions), so use smaller batch size
batch_size = 100  # Insert 100 rows at a time
with conn.cursor() as cur:
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        cur.executemany(insert_sql, batch)
    conn.commit()

# Create indexes
with conn.cursor() as cur:
    # cur.execute("CREATE VECTOR INDEX partsupp_deep_hnsw ON partsupp (ps_image_embedding) WITH (distance=l2, type=hnsw, lib=vsag);")  # Not processed yet
    cur.execute("CREATE VECTOR INDEX partsupp_wiki_hnsw ON partsupp (ps_text_embedding) WITH (distance=l2, type=hnsw, lib=vsag);")
    cur.execute("CREATE VECTOR INDEX part_wiki_hnsw ON part (text_embedding) WITH (distance=l2, type=hnsw, lib=vsag);")
    conn.commit()

conn.close()
