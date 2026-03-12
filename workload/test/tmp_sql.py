#!/usr/bin/env python3
import os
import time
import json
import numpy as np
import mysql.connector
from rich.console import Console
from rich.table import Table

# --- 配置 ---
CONFIG = {
    'db_host': '127.0.0.1',
    'db_port': 10200,
    'db_user': 'root',
    # 这里的 db_name 只是占位，实际会在循环中覆盖为不同的库名
    'db_name': 'tpch10_1',
    'vector_file': '/data/dzh/seekdb/Exqutor/Vector-augmented_SQL_analytics/WIKI/queries.fbin',
    # 使用当前目录下的 tmp_sql.sql
    'sql_file': os.path.join(os.path.dirname(__file__), 'tmp_sql.sql'),
    'vector_limit': 1,    # 测试多少个向量
    'result_limit': 10,
    'output_file': None,  # 如需导出 JSON 结果可在此填写路径
}

# 需要依次测试的数据库
DB_NAMES = ['tpch10_1', 'tpch10_5', 'tpch10_6', 'tpch10_7']

# 纯向量查询模板，用于测量底层向量检索延时
VECTOR_ONLY_TEMPLATE = "SELECT p_partkey FROM part ORDER BY l2_distance(text_embedding,'{VECTOR}') APPROXIMATE LIMIT {LIMIT};"

console = Console()


def safe_execute_query(cursor, sql, description=""):
    """
    安全执行SQL查询，确保完全消费所有结果集，避免 'Commands out of sync' 错误
    支持多语句查询
    """
    try:
        # 先清空任何未消费的结果（如果存在）
        try:
            while cursor.nextset():
                cursor.fetchall()
        except Exception:
            pass

        # 执行查询
        cursor.execute(sql)

        # 获取第一个结果集（如果有）
        try:
            cursor.fetchall()
        except Exception:
            # 某些查询可能没有结果集（如 DDL 语句），这是正常的
            pass

        # 处理可能存在的多个结果集（多语句查询）
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
        # 如果出错，尝试重置连接状态
        try:
            while cursor.nextset():
                cursor.fetchall()
        except Exception:
            pass
        return False, str(e)


class DataLoader:
    @staticmethod
    def read_vectors(filename, limit):
        with open(filename, "rb") as f:
            header = np.fromfile(f, count=2, dtype=np.int32)
            total_nvecs, dim = header[0], header[1]
            read_count = min(limit, total_nvecs)
            data = np.fromfile(f, count=read_count * dim, dtype=np.float32)
            return data.reshape(-1, dim)


def load_sql_template(filename):
    with open(filename, 'r') as f:
        return f.read().strip()


def run_benchmark_multi_db():
    # 读取向量和 SQL 模板
    vectors = DataLoader.read_vectors(CONFIG['vector_file'], CONFIG['vector_limit'])
    sql_template = load_sql_template(CONFIG['sql_file'])

    has_vector_placeholder = '{VECTOR}' in sql_template
    has_limit_placeholder = '{LIMIT}' in sql_template

    console.print(f"[bold green]已加载 {len(vectors)} 个向量，SQL 文件: {CONFIG['sql_file']}[/bold green]\n")

    all_results = {}

    for db_name in DB_NAMES:
        console.print(f"\n[bold blue]=== 开始测试数据库: {db_name} ===[/bold blue]")

        conn = mysql.connector.connect(
            host=CONFIG['db_host'],
            port=CONFIG['db_port'],
            user=CONFIG['db_user'],
            database=db_name,
            autocommit=True,
            allow_local_infile=True,
            sql_mode='',
            charset='utf8mb4',
            use_unicode=True,
        )
        cursor = conn.cursor()

        # 禁用动态采样，避免 dynamic sampling timeout
        try:
            cursor.execute("SET SESSION optimizer_dynamic_sampling = 0")
        except Exception as e:
            console.print(f"[yellow]警告: 无法在 {db_name} 禁用动态采样: {e}[/yellow]")

        vec_latencies = []
        sql_latencies = []

        for i, vec in enumerate(vectors):
            vec_str = "[" + ",".join(map(str, vec)) + "]"

            # # --- 测试 1: 纯向量查询 ---
            # vec_sql = VECTOR_ONLY_TEMPLATE.replace('{VECTOR}', vec_str).replace('{LIMIT}', str(CONFIG['result_limit']))
            # start_v = time.perf_counter()
            # success, error = safe_execute_query(cursor, vec_sql, "纯向量查询")
            # if success:
            #     vec_latencies.append((time.perf_counter() - start_v) * 1000)
            # else:
            #     console.print(f"  [yellow]{db_name} 纯向量查询警告: {error}[/yellow]")

            # --- 测试 2: 业务 SQL （来自 tmp_sql.sql）---
            full_sql = sql_template
            if has_vector_placeholder:
                full_sql = full_sql.replace('{VECTOR}', vec_str)
            if has_limit_placeholder:
                full_sql = full_sql.replace('{LIMIT}', str(CONFIG['result_limit']))

            start_s = time.perf_counter()
            success, error = safe_execute_query(cursor, full_sql, "业务 SQL 查询")
            if success:
                sql_latencies.append((time.perf_counter() - start_s) * 1000)
            else:
                console.print(f"  [red]{db_name} 业务 SQL 查询出错: {error}[/red]")

        cursor.close()
        conn.close()

        all_results[db_name] = {
            "vec_latencies": vec_latencies,
            "sql_latencies": sql_latencies,
        }

        # 每个数据库结束后给出简单统计
        if sql_latencies:
            avg_sql = float(np.mean(sql_latencies))
            console.print(f"[green]{db_name} 业务 SQL 平均延时: {avg_sql:.2f} ms[/green]")
        else:
            console.print(f"[yellow]{db_name} 无成功的业务 SQL 测试样本[/yellow]")

    # 汇总表格输出
    table = Table(title="tmp_sql 多数据库延时统计")
    table.add_column("数据库", style="cyan")
    table.add_column("纯向量样本数", justify="right")
    table.add_column("纯向量平均延时(ms)", justify="right")
    table.add_column("SQL 样本数", justify="right")
    table.add_column("SQL 平均延时(ms)", justify="right")
    table.add_column("SQL 最小延时(ms)", justify="right")
    table.add_column("SQL 最大延时(ms)", justify="right")

    for db_name in DB_NAMES:
        res = all_results.get(db_name, {})
        vec_lat = res.get("vec_latencies", [])
        sql_lat = res.get("sql_latencies", [])

        vec_cnt = len(vec_lat)
        sql_cnt = len(sql_lat)

        vec_avg = float(np.mean(vec_lat)) if vec_lat else 0.0
        sql_avg = float(np.mean(sql_lat)) if sql_lat else 0.0
        sql_min = float(np.min(sql_lat)) if sql_lat else 0.0
        sql_max = float(np.max(sql_lat)) if sql_lat else 0.0

        table.add_row(
            db_name,
            str(vec_cnt),
            f"{vec_avg:.2f}" if vec_cnt else "N/A",
            str(sql_cnt),
            f"{sql_avg:.2f}" if sql_cnt else "N/A",
            f"{sql_min:.2f}" if sql_cnt else "N/A",
            f"{sql_max:.2f}" if sql_cnt else "N/A",
        )

    console.print()
    console.print(table)

    # 如需导出 JSON 结果
    if CONFIG['output_file'] is not None:
        output = {
            "config": {
                "vector_limit": CONFIG['vector_limit'],
                "result_limit": CONFIG['result_limit'],
                "db_names": DB_NAMES,
            },
            "databases": {},
        }
        for db_name, res in all_results.items():
            sql_lat = res.get("sql_latencies", [])
            output["databases"][db_name] = {
                "vec_latencies_ms": res.get("vec_latencies", []),
                "sql_latencies_ms": sql_lat,
                "sql_avg_latency_ms": float(np.mean(sql_lat)) if sql_lat else 0.0,
            }

        with open(CONFIG['output_file'], 'w') as f:
            json.dump(output, f, indent=4)
        console.print(f"[bold green]结果已保存到: {CONFIG['output_file']}[/bold green]")


if __name__ == "__main__":
    run_benchmark_multi_db()

