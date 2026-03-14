#!/usr/bin/env python3
"""
TPC-H 测试脚本 - 仅测试混合查询 (Hybrid Queries)
仿照 test_tpch10.py，但只测试包含向量+标量的混合查询，不测试：
  - 标量查询 (无 {VECTOR} 的纯 SQL)
  - 纯向量查询 (VECTOR_ONLY_TEMPLATE)
"""
import os
import re
import time
import json
import numpy as np
import mysql.connector
from rich.console import Console
from rich.table import Table
from rich.panel import Panel

# --- 配置 ---
CONFIG = {
    'db_host': '127.0.0.1',
    'db_port': 10200,
    'db_user': 'root',
    'db_name': 'tpch10_5',
    'vector_file': '/data/dzh/seekdb/Exqutor/Vector-augmented_SQL_analytics/WIKI/queries.fbin',
    'sql_file': '/data/dzh/seekdb/workload/test/tpch_queries.sql',
    'vector_limit': 1,    # 测试多少个向量
    'result_limit': 10,
    'output_file': None  # 例如: 'hybrid_latency_report.json'
}

console = Console()


def safe_execute_query(cursor, sql, description=""):
    """
    安全执行SQL查询，确保完全消费所有结果集，避免 'Commands out of sync' 错误
    支持多语句查询（如 Q15 包含 CREATE VIEW, SELECT, DROP VIEW）
    """
    try:
        try:
            while cursor.nextset():
                cursor.fetchall()
        except Exception:
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

    @staticmethod
    def load_queries(filename):
        with open(filename, 'r') as f:
            content = f.read()
        queries = {}
        matches = re.findall(r'--Q(\d+)(.*?)(?=--Q\d+|$)', content, re.DOTALL)
        for q_id, q_sql in matches:
            queries[f"Q{q_id}"] = q_sql.strip()
        return queries


def run_benchmark():
    vectors = DataLoader.read_vectors(CONFIG['vector_file'], CONFIG['vector_limit'])
    queries = DataLoader.load_queries(CONFIG['sql_file'])

    # 只保留混合查询（包含 {VECTOR} 的查询）
    hybrid_queries = {k: v for k, v in queries.items() if '{VECTOR}' in v}
    console.print(
        f"[bold green]已加载 {len(vectors)} 个向量[/bold green]\n"
        f"[bold green]混合查询: {len(hybrid_queries)} 条 "
        f"(跳过 {len(queries) - len(hybrid_queries)} 条标量查询)[/bold green]\n"
    )

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

    report = {}

    def _query_sort_key(item):
        name = item[0] if isinstance(item, tuple) else item
        return int(name[1:]) if name.startswith('Q') and name[1:].isdigit() else 0

    sorted_queries = sorted(hybrid_queries.items(), key=_query_sort_key)
    for q_name, q_template in sorted_queries:
        console.print(f"[*] 正在测试 {q_name} (混合查询)...")
        report[q_name] = {"sql_times": []}

        has_limit_placeholder = '{LIMIT}' in q_template

        for i, vec in enumerate(vectors):
            vec_str = "[" + ",".join(map(str, vec)) + "]"
            full_sql = q_template.replace('{VECTOR}', vec_str)
            if has_limit_placeholder:
                full_sql = full_sql.replace('{LIMIT}', str(CONFIG['result_limit']))

            start_s = time.perf_counter()
            success, error = safe_execute_query(cursor, full_sql, "混合查询")
            if success:
                report[q_name]["sql_times"].append((time.perf_counter() - start_s) * 1000)
            else:
                console.print(f"  [red]混合查询出错: {error}[/red]")
            try:
                while cursor.nextset():
                    cursor.fetchall()
            except Exception:
                pass

    # 输出结果表格
    table = Table(
        title=f"TPC-H 混合查询性能测试结果 (共测试 {len(hybrid_queries)} 条混合查询)"
    )
    table.add_column("Query ID", style="cyan")
    table.add_column("测试次数", style="blue", justify="right")
    table.add_column("平均延时 (ms)", style="magenta", justify="right")
    table.add_column("最小延时 (ms)", style="green", justify="right")
    table.add_column("最大延时 (ms)", style="red", justify="right")

    total_avg = []
    for q_name in sorted(report.keys(), key=_query_sort_key):
        data = report[q_name]
        if data["sql_times"]:
            avg_sql = np.mean(data["sql_times"])
            min_sql = np.min(data["sql_times"])
            max_sql = np.max(data["sql_times"])
            total_avg.append(avg_sql)
            table.add_row(
                q_name,
                str(len(data["sql_times"])),
                f"{avg_sql:.2f}",
                f"{min_sql:.2f}",
                f"{max_sql:.2f}"
            )
        else:
            table.add_row(q_name, "0", "N/A", "N/A", "N/A")

    console.print(table)

    if total_avg:
        overall_avg = np.mean(total_avg)
        console.print(f"\n[bold cyan]总体统计:[/bold cyan]")
        console.print(f"  [green]混合查询平均延时: {overall_avg:.2f} ms[/green]")
        console.print(f"  [green]成功测试的查询数: {len(total_avg)}/{len(hybrid_queries)}[/green]")

    # 保存结果
    final_output = {
        "config": {
            "vector_limit": CONFIG['vector_limit'],
            "result_limit": CONFIG['result_limit'],
            "total_hybrid_queries": len(hybrid_queries),
            "total_vectors_tested": len(vectors)
        },
        "overall_avg_latency_ms": np.mean(total_avg) if total_avg else 0,
        "queries": {}
    }

    for q_name, data in sorted(report.items(), key=lambda x: _query_sort_key(x[0])):
        if data["sql_times"]:
            final_output["queries"][q_name] = {
                "test_count": len(data["sql_times"]),
                "avg_latency_ms": np.mean(data["sql_times"]),
                "min_latency_ms": float(np.min(data["sql_times"])),
                "max_latency_ms": float(np.max(data["sql_times"])),
                "all_latencies_ms": data["sql_times"]
            }
        else:
            final_output["queries"][q_name] = {
                "test_count": 0,
                "error": "No successful tests"
            }

    if CONFIG['output_file'] is not None:
        with open(CONFIG['output_file'], 'w') as f:
            json.dump(final_output, f, indent=4)
        console.print(f"\n[bold green]结果已保存到: {CONFIG['output_file']}[/bold green]")

    cursor.close()
    conn.close()


if __name__ == "__main__":
    run_benchmark()
