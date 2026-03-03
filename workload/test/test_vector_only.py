#!/usr/bin/env python3
"""
纯向量查询延时测试 - 分别计算指定向量在 part_vector 和 partsupp_vector 上的查询延时
仿照 test_tpch10_4.py 的纯向量查询逻辑
"""
import time
import numpy as np
import mysql.connector
from rich.console import Console
from rich.table import Table

# --- 配置 ---
CONFIG = {
    'db_host': '127.0.0.1',
    'db_port': 10200,
    'db_user': 'root',
    'db_name': 'tpch10_5',
    'vector_file': '/data/dzh/seekdb/Exqutor/Vector-augmented_SQL_analytics/WIKI/queries.fbin',
    'vector_count': 2,    # 测试 3 个向量
    'result_limit': 10,
}

# 纯向量查询模板
VECTOR_ONLY_TEMPLATES = [
    # ("partsupp_vector", "SELECT ps_partkey FROM partsupp_vector ORDER BY l2_distance(ps_text_embedding,'{VECTOR}') APPROXIMATE LIMIT {LIMIT};"),
    ("partsupp_vector", ""),    
    ("part_vector", "SELECT p_partkey FROM part ORDER BY l2_distance(text_embedding,'{VECTOR}') APPROXIMATE LIMIT {LIMIT};"),
]

console = Console()


def read_vectors(filename, limit):
    with open(filename, "rb") as f:
        header = np.fromfile(f, count=2, dtype=np.int32)
        total_nvecs, dim = header[0], header[1]
        read_count = min(limit, total_nvecs)
        data = np.fromfile(f, count=read_count * dim, dtype=np.float32)
        return data.reshape(-1, dim)


def safe_execute_query(cursor, sql):
    try:
        try:
            while cursor.nextset():
                cursor.fetchall()
        except:
            pass
        cursor.execute(sql)
        try:
            cursor.fetchall()
        except Exception:
            pass
        while True:
            try:
                if cursor.nextset():
                    cursor.fetchall()
                else:
                    break
            except Exception:
                break
        return True, None
    except Exception as e:
        try:
            while cursor.nextset():
                cursor.fetchall()
        except:
            pass
        return False, str(e)


def run_benchmark():
    vectors = read_vectors(CONFIG['vector_file'], CONFIG['vector_count'])
    console.print(f"[bold green]已加载 {len(vectors)} 个向量[/bold green]")
    console.print(f"[bold green]数据库: {CONFIG['db_name']}[/bold green]\n")

    conn = mysql.connector.connect(
        host=CONFIG['db_host'], port=CONFIG['db_port'],
        user=CONFIG['db_user'], database=CONFIG['db_name'],
        autocommit=True,
        allow_local_infile=True,
        sql_mode='',
        charset='utf8mb4',
        use_unicode=True
    )
    cursor = conn.cursor()

    try:
        cursor.execute("SET SESSION optimizer_dynamic_sampling = 0")
    except Exception as e:
        console.print(f"[yellow]警告: 无法禁用动态采样: {e}[/yellow]")

    # 结果: result[vector_idx][table_name] = latency_ms
    results = []
    for i, vec in enumerate(vectors):
        vec_str = "[" + ",".join(map(str, vec)) + "]"
        row = {"vector_idx": i + 1}
        for table_name, vec_template in VECTOR_ONLY_TEMPLATES:
            vec_sql = vec_template.replace('{VECTOR}', vec_str).replace('{LIMIT}', str(CONFIG['result_limit']))
            start = time.perf_counter()
            success, error = safe_execute_query(cursor, vec_sql)
            latency_ms = (time.perf_counter() - start) * 1000 if success else None
            row[table_name] = latency_ms
            if not success:
                console.print(f"[yellow]向量 {i+1} 在 {table_name} 上查询失败: {error}[/yellow]")
            try:
                while cursor.nextset():
                    cursor.fetchall()
            except:
                pass
        results.append(row)

    # 输出表格
    table = Table(title=f"纯向量查询延时 (共 {len(vectors)} 个向量)")
    table.add_column("向量", style="cyan", justify="center")
    table.add_column("part_vector (ms)", style="magenta", justify="right")
    table.add_column("partsupp_vector (ms)", style="green", justify="right")

    for row in results:
        pv = f"{row['part_vector']:.2f}" if row['part_vector'] is not None else "N/A"
        psv = f"{row['partsupp_vector']:.2f}" if row['partsupp_vector'] is not None else "N/A"
        table.add_row(str(row["vector_idx"]), pv, psv)

    console.print(table)

    # 输出平均值
    pv_vals = [r["part_vector"] for r in results if r["part_vector"] is not None]
    psv_vals = [r["partsupp_vector"] for r in results if r["partsupp_vector"] is not None]
    if pv_vals:
        console.print(f"\n[bold]part_vector 平均延时:[/bold] {np.mean(pv_vals):.2f} ms")
    if psv_vals:
        console.print(f"[bold]partsupp_vector 平均延时:[/bold] {np.mean(psv_vals):.2f} ms")

    cursor.close()
    conn.close()


if __name__ == "__main__":
    run_benchmark()
