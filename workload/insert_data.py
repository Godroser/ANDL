import pandas as pd
import numpy as np
import mysql.connector
from decimal import Decimal
from concurrent.futures import ThreadPoolExecutor
import threading
from queue import Queue
import os
from pathlib import Path
import re
import bisect
from typing import List

# File paths
partsupp_csv_path = '/data/dzh/seekdb/workload/tpch10/partsupp.csv'
part_csv_path = '/data/dzh/seekdb/workload/tpch10/part.csv'
# deep_bin_path = '/data/dzh/seekdb/Exqutor/Vector-augmented_SQL_analytics/DEEP/base.1B.fbin'  # Not processed yet
# 这个已废弃
wiki_bin_path = '/data/dzh/seekdb/Exqutor/Vector-augmented_SQL_analytics/WIKI/base.10M.fbin'

# Embedding dataset path:
# - A single .fbin file (backward compatible with wiki_bin_path)
# - Or a directory containing multiple .fbin shards (after extracting a larger dataset)
WIKI_EMBEDDING_PATH = '/data/dzh/seekdb/Exqutor/Vector-augmented_SQL_analytics/WIKI/base.10M.fbin'
# "/data/dzh/seekdb/Exqutor/Vector-augmented_SQL_analytics/WIKI/wiki_all_88M/base.88M.fbin"

# DB connection config
DB_CONFIG = {
    'host': '127.0.0.1',
    'port': 10200,
    'user': 'root',
    'database': 'tpch10'
}

# Temp directory for chunk files that will be loaded by the DB server
TMP_DIR = "/data/dzh/seekdb/workload/tmp/vector_load_sf10"

# Performance configuration
MAX_WORKERS = 32  # Increased from 8
BATCH_SIZE = 500  # Increased from 100 (max_allowed_packet=1GB allows much larger batches)
CHUNK_SIZE = 20000  # Increased from 10000 for better parallelization

# Prefer bulk load (much faster than executemany for large data)
USE_LOAD_DATA_INFILE = True

# Vector string formatting: tradeoff between size & precision.
# '%.6g' is usually enough for embedding search and reduces I/O significantly.
VEC_FLOAT_FMT = "%.6g"


# Connection pool for thread reuse
connection_pool = Queue()
pool_lock = threading.Lock()

def get_connection():
    """Get a connection from pool or create new one"""
    try:
        return connection_pool.get_nowait()
    except:
        conn = mysql.connector.connect(**DB_CONFIG)
        conn.autocommit = False
        return conn

def return_connection(conn):
    """Return connection to pool"""
    try:
        connection_pool.put_nowait(conn)
    except:
        conn.close()

def read_fbin(filename, start_idx=0, chunk_size=None):
    with open(filename, "rb") as f:
        nvecs, dim = np.fromfile(f, count=2, dtype=np.int32)
        n_fetch = (nvecs - start_idx) if chunk_size is None else chunk_size
        
        # Calculate the byte position: 8 bytes for header (2 int32) + start_idx * dim * 4 bytes per float32
        header_size = 8  # 2 * sizeof(int32)
        byte_offset = header_size + int(start_idx) * int(dim) * 4
        
        # Seek to the correct position (offset is relative to file start)
        # After reading header, file pointer is at position 8, so we need to seek from start
        f.seek(byte_offset, 0)  # 0 means from start of file
        
        # Read the data without using offset parameter (since we already seeked)
        arr = np.fromfile(f, count=n_fetch * dim, dtype=np.float32)
    return arr.reshape(-1, dim)

def read_csv(path, columns):
    df = pd.read_csv(path, delimiter='|', header=None, engine="pyarrow")
    df = df.iloc[:, :columns]
    return df

def vectors_to_strings_batch(vectors):
    """Convert numpy array of vectors to SeekDB vector string format efficiently
    Optimized version using numpy vectorization - much faster than per-vector conversion
    """
    n_vectors, dim = vectors.shape
    
    # Pre-allocate list for better performance
    result = [None] * n_vectors
    
    # Convert all vectors to string format efficiently
    # Using list comprehension with numpy's astype is faster than manual loops
    # Format: '[v1,v2,v3,...]' for each vector
    for i in range(n_vectors):
        # Convert numpy array to string array, then join
        # This is faster than using vector_to_string for each vector
        vec_str = '[' + ','.join(vectors[i].astype(str)) + ']'
        result[i] = vec_str
    
    return result

def ensure_tmp_dir():
    Path(TMP_DIR).mkdir(parents=True, exist_ok=True)

def iter_csv_chunks(path, n_cols, chunk_size):
    """
    Stream TPCH '|' delimited files without pandas (much lower memory).
    TPCH rows typically end with a trailing '|', so split will contain an extra empty field.
    """
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        buf = []
        for line in f:
            line = line.rstrip("\n")
            if not line:
                continue
            parts = line.split("|")
            # Keep first n_cols; TPCH has a trailing delimiter so parts[-1] is often empty.
            buf.append(parts[:n_cols])
            if len(buf) >= chunk_size:
                yield buf
                buf = []
        if buf:
            yield buf

def vectors_to_strings_batch_fast(vectors: np.ndarray, fmt: str = VEC_FLOAT_FMT):
    """
    Faster + smaller vector serialization.
    Uses numpy vectorized float formatting first, then per-row join.
    """
    # Vectorized formatting (C loop) -> array of strings
    s = np.char.mod(fmt, vectors)
    out = []
    for i in range(s.shape[0]):
        out.append("[" + ",".join(s[i].tolist()) + "]")
    return out

def _natural_sort_key(s: str):
    # Stable shard ordering like xxx_01.fbin, xxx_02.fbin ...
    parts = re.split(r"(\d+)", s)
    key = []
    for p in parts:
        key.append(int(p) if p.isdigit() else p)
    return key

def resolve_fbin_paths(path: str) -> List[str]:
    """
    Resolve a single .fbin file or a directory containing multiple .fbin shards.
    Note: we DO NOT modify the directory; only list and read.
    """
    if os.path.isfile(path):
        return [path]
    if os.path.isdir(path):
        files = []
        for name in os.listdir(path):
            full = os.path.join(path, name)
            if os.path.isfile(full) and name.lower().endswith(".fbin"):
                files.append(full)
        files.sort(key=lambda p: _natural_sort_key(os.path.basename(p)))
        if not files:
            raise FileNotFoundError(
                f"No .fbin files found under directory: {path}. "
                f"If this directory currently contains downloaded archives (e.g. *.tar.00 ...), "
                f"please extract them to produce .fbin shard files, then point WIKI_EMBEDDING_PATH to that directory."
            )
        return files
    raise FileNotFoundError(f"WIKI_EMBEDDING_PATH not found: {path}")

class FbinVectorSource:
    """
    Read vectors by global row offset across 1 or N .fbin files.
    Each .fbin format: int32 nvecs, int32 dim, then float32[nvecs * dim].
    """
    def __init__(self, paths: List[str]):
        self.paths = paths
        self.counts: List[int] = []
        self.starts: List[int] = []  # global start offset of each file
        self.dim: int = -1
        total = 0
        for p in self.paths:
            with open(p, "rb") as f:
                nvecs, dim = np.fromfile(f, count=2, dtype=np.int32)
            nvecs = int(nvecs)
            dim = int(dim)
            if self.dim == -1:
                self.dim = dim
            elif self.dim != dim:
                raise ValueError(f"Dimension mismatch across shards: expected {self.dim}, got {dim} in {p}")
            self.starts.append(total)
            self.counts.append(nvecs)
            total += nvecs
        self.total = total

    @classmethod
    def from_path(cls, path: str) -> "FbinVectorSource":
        return cls(resolve_fbin_paths(path))

    def read(self, start_idx: int, count: int) -> np.ndarray:
        if count <= 0:
            return np.empty((0, self.dim), dtype=np.float32)
        if start_idx < 0:
            raise ValueError(f"start_idx must be >= 0, got {start_idx}")
        end = start_idx + count
        if end > self.total:
            raise ValueError(
                f"Vector dataset exhausted: need [{start_idx}, {end}) but only {self.total} vectors available "
                f"(missing {end - self.total})."
            )

        if len(self.paths) == 1:
            return read_fbin(self.paths[0], start_idx=start_idx, chunk_size=count)

        # Find shard i where starts[i] <= start_idx < starts[i+1]
        i = bisect.bisect_right(self.starts, start_idx) - 1
        if i < 0:
            i = 0

        remaining = count
        cur = start_idx
        chunks = []
        while remaining > 0:
            local_start = cur - self.starts[i]
            can_read = min(remaining, self.counts[i] - local_start)
            if can_read <= 0:
                i += 1
                continue
            chunks.append(read_fbin(self.paths[i], start_idx=local_start, chunk_size=can_read))
            cur += can_read
            remaining -= can_read
            if remaining > 0:
                i += 1
        return np.concatenate(chunks, axis=0)

def load_data_infile(conn, table_name: str, infile_path: str, user_var_count: int, set_sql: str):
    """
    Execute: LOAD DATA INFILE '...' INTO TABLE ... (@c1,...,@cN) SET ...
    This avoids per-row INSERT overhead and is typically the fastest path in OceanBase/MySQL.
    """
    user_vars = ",".join([f"@c{i}" for i in range(1, user_var_count + 1)])
    sql = f"""
        LOAD DATA INFILE '{infile_path}'
        INTO TABLE {table_name}
        FIELDS TERMINATED BY '|'
        LINES TERMINATED BY '\\n'
        ({user_vars})
        SET {set_sql};
    """
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()

# --- PARTSUPP TABLE ---
PARTSUPP_TABLE = "partsupp"
conn = mysql.connector.connect(**DB_CONFIG)
conn.autocommit = False

wiki_source = FbinVectorSource.from_path(WIKI_EMBEDDING_PATH)
print(f"WIKI embeddings: {wiki_source.total} vectors, dim={wiki_source.dim}, shards={len(wiki_source.paths)}")

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

print(f"Reading partsupp CSV file...")
ensure_tmp_dir()

if USE_LOAD_DATA_INFILE:
    print(f"Partsupp table: using LOAD DATA INFILE (chunk_size={CHUNK_SIZE}, workers={MAX_WORKERS})")
    chunk_idx = 0
    start_index = 0
    for rows_cols in iter_csv_chunks(partsupp_csv_path, n_cols=5, chunk_size=CHUNK_SIZE):
        n = len(rows_cols)
        # Read corresponding embeddings
        text_vectors = wiki_source.read(start_index, n)
        text_embeddings_str = vectors_to_strings_batch_fast(text_vectors)

        # Write chunk file: 7 columns (5 scalar + placeholder + vector)
        chunk_path = os.path.join(TMP_DIR, f"partsupp_with_vec_{chunk_idx}.csv")
        with open(chunk_path, "w", encoding="utf-8") as out:
            for i, cols in enumerate(rows_cols):
                # cols: [ps_partkey, ps_suppkey, ps_availqty, ps_supplycost, ps_comment]
                # add col6 placeholder, col7 vector
                out.write("|".join([
                    cols[0], cols[1], cols[2], cols[3], cols[4],
                    "",  # ps_image_embedding placeholder
                    text_embeddings_str[i],
                    ""   # keep TPCH-style trailing delimiter
                ]) + "\n")

        set_sql = (
            "ps_partkey=@c1, ps_suppkey=@c2, ps_availqty=@c3, ps_supplycost=@c4, ps_comment=@c5, "
            "ps_image_embedding=NULL, ps_text_embedding=@c7"
        )
        load_data_infile(conn, PARTSUPP_TABLE, chunk_path, user_var_count=7, set_sql=set_sql)

        chunk_idx += 1
        start_index += n
        if chunk_idx % 10 == 0:
            print(f"Completed {chunk_idx} partsupp chunks (rows loaded so far: {start_index})")

    conn.close()
    print("Partsupp table import completed (LOAD DATA INFILE)!")
else:
    # Fallback: previous executemany path (kept for compatibility)
    partsupp_df = read_csv(partsupp_csv_path, 5)
    num_rows = len(partsupp_df)
    num_chunks = (num_rows + CHUNK_SIZE - 1) // CHUNK_SIZE
    print(f"Partsupp table: {num_rows} rows, {num_chunks} chunks, chunk_size={CHUNK_SIZE}, batch_size={BATCH_SIZE}, workers={MAX_WORKERS}")

    # Pre-populate connection pool
    for _ in range(min(MAX_WORKERS, 8)):
        conn_pool_item = mysql.connector.connect(**DB_CONFIG)
        conn_pool_item.autocommit = False
        connection_pool.put(conn_pool_item)

    def process_partsupp_chunk(chunk_idx, chunk_size, text_embedding_source: FbinVectorSource, partsupp_df, table_name):
        start_index = chunk_idx * chunk_size
        end_index = min(start_index + chunk_size, len(partsupp_df))
        df_chunk = partsupp_df.iloc[start_index:end_index].copy()

        text_vectors = text_embedding_source.read(start_index, end_index - start_index)
        text_embeddings_str = vectors_to_strings_batch_fast(text_vectors)

        insert_sql = f"""
            INSERT INTO {table_name}
            (ps_partkey, ps_suppkey, ps_availqty, ps_supplycost, ps_comment, ps_image_embedding, ps_text_embedding)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        
        rows = []
        for idx, row in enumerate(df_chunk.itertuples(index=False, name=None)):
            rows.append((
                int(row[0]), int(row[1]), int(row[2]), Decimal(str(row[3])), str(row[4]),
                None,
                text_embeddings_str[idx]
            ))

        thread_conn = get_connection()
        try:
            with thread_conn.cursor() as cur:
                for i in range(0, len(rows), BATCH_SIZE):
                    batch = rows[i:i + BATCH_SIZE]
                    cur.executemany(insert_sql, batch)
                thread_conn.commit()
        finally:
            return_connection(thread_conn)

    print(f"Starting parallel import of partsupp table (executemany fallback)...")
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [
            executor.submit(process_partsupp_chunk, i, CHUNK_SIZE, wiki_source, partsupp_df, PARTSUPP_TABLE)
            for i in range(num_chunks)
        ]
        for idx, fut in enumerate(futures):
            fut.result()
            if (idx + 1) % 10 == 0:
                print(f"Completed {idx + 1}/{num_chunks} chunks")

    while not connection_pool.empty():
        conn_pool_item = connection_pool.get()
        conn_pool_item.close()

    conn.close()
    print("Partsupp table import completed (executemany fallback)!")

# --- PART TABLE ---
PART_TABLE = "part"
conn = mysql.connector.connect(**DB_CONFIG)
conn.autocommit = False

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

print(f"\nReading part CSV file...")
ensure_tmp_dir()

if USE_LOAD_DATA_INFILE:
    print(f"Part table: using LOAD DATA INFILE (chunk_size={CHUNK_SIZE}, workers={MAX_WORKERS})")
    chunk_idx = 0
    start_index = 0
    for rows_cols in iter_csv_chunks(part_csv_path, n_cols=9, chunk_size=CHUNK_SIZE):
        n = len(rows_cols)
        text_vectors = wiki_source.read(start_index, n)
        text_embeddings_str = vectors_to_strings_batch_fast(text_vectors)

        chunk_path = os.path.join(TMP_DIR, f"part_with_vec_{chunk_idx}.csv")
        with open(chunk_path, "w", encoding="utf-8") as out:
            for i, cols in enumerate(rows_cols):
                # cols: [p_partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice, p_comment]
                out.write("|".join(cols + [text_embeddings_str[i], ""]) + "\n")

        set_sql = (
            "p_partkey=@c1, p_name=@c2, p_mfgr=@c3, p_brand=@c4, p_type=@c5, "
            "p_size=@c6, p_container=@c7, p_retailprice=@c8, p_comment=@c9, text_embedding=@c10"
        )
        load_data_infile(conn, PART_TABLE, chunk_path, user_var_count=10, set_sql=set_sql)

        chunk_idx += 1
        start_index += n
        if chunk_idx % 10 == 0:
            print(f"Completed {chunk_idx} part chunks (rows loaded so far: {start_index})")

    conn.close()
    print("Part table import completed (LOAD DATA INFILE)!")
else:
    part_df = read_csv(part_csv_path, 9)
    num_part_rows = len(part_df)
    num_part_chunks = (num_part_rows + CHUNK_SIZE - 1) // CHUNK_SIZE
    print(f"Part table: {num_part_rows} rows, {num_part_chunks} chunks, chunk_size={CHUNK_SIZE}, batch_size={BATCH_SIZE}, workers={MAX_WORKERS}")

    for _ in range(min(MAX_WORKERS, 8)):
        conn_pool_item = mysql.connector.connect(**DB_CONFIG)
        conn_pool_item.autocommit = False
        connection_pool.put(conn_pool_item)

    def process_part_chunk(chunk_idx, chunk_size, text_embedding_source: FbinVectorSource, part_df, table_name):
        start_index = chunk_idx * chunk_size
        end_index = min(start_index + chunk_size, len(part_df))
        df_chunk = part_df.iloc[start_index:end_index].copy()

        text_vectors = text_embedding_source.read(start_index, end_index - start_index)
        text_embeddings_str = vectors_to_strings_batch_fast(text_vectors)

        insert_sql = f"""
            INSERT INTO {table_name}
            (p_partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice, p_comment, text_embedding)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        rows = []
        for idx, row in enumerate(df_chunk.itertuples(index=False, name=None)):
            rows.append((
                int(row[0]), str(row[1]), str(row[2]), str(row[3]), str(row[4]),
                int(row[5]), str(row[6]), Decimal(str(row[7])), str(row[8]), text_embeddings_str[idx]
            ))

        thread_conn = get_connection()
        try:
            with thread_conn.cursor() as cur:
                for i in range(0, len(rows), BATCH_SIZE):
                    batch = rows[i:i + BATCH_SIZE]
                    cur.executemany(insert_sql, batch)
                thread_conn.commit()
        finally:
            return_connection(thread_conn)

    print(f"Starting parallel import of part table (executemany fallback)...")
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [
            executor.submit(process_part_chunk, i, CHUNK_SIZE, wiki_source, part_df, PART_TABLE)
            for i in range(num_part_chunks)
        ]
        for idx, fut in enumerate(futures):
            fut.result()
            if (idx + 1) % 10 == 0:
                print(f"Completed {idx + 1}/{num_part_chunks} chunks")

    while not connection_pool.empty():
        conn_pool_item = connection_pool.get()
        conn_pool_item.close()

    conn.close()
    print("Part table import completed (executemany fallback)!")

# Create indexes
print("\nCreating vector indexes...")
conn = mysql.connector.connect(**DB_CONFIG)
conn.autocommit = False
with conn.cursor() as cur:
    # cur.execute("CREATE VECTOR INDEX partsupp_deep_hnsw ON partsupp (ps_image_embedding) WITH (distance=l2, type=hnsw, lib=vsag);")  # Not processed yet
    print("Creating partsupp_wiki_hnsw index...")
    cur.execute("CREATE VECTOR INDEX partsupp_wiki_hnsw ON partsupp (ps_text_embedding) WITH (distance=l2, type=hnsw, lib=vsag);")
    print("Creating part_wiki_hnsw index...")
    cur.execute("CREATE VECTOR INDEX part_wiki_hnsw ON part (text_embedding) WITH (distance=l2, type=hnsw, lib=vsag);")
    conn.commit()
conn.close()
print("All indexes created successfully!")