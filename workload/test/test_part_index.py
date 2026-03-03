#!/usr/bin/env python3
"""
TPC-H 测试脚本 - tpch01 数据库
针对 orders / orders1 / orders2 / orders3 / orders4 / orders5 分别测试指定 SQL 的执行延时。
支持多种缓存规避策略，减少反复测试导致的 cache 影响。
"""
import argparse
import random
import subprocess
import sys
import time
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
    'db_name': 'tpch01',
    'vector_file': '/data/dzh/seekdb/Exqutor/Vector-augmented_SQL_analytics/WIKI/queries.fbin',
    'vector_limit': 2,
    'result_limit': 10,
}

# 要测试的 orders 表名列表
# ORDERS_TABLES = ['orders', 'orders1', 'orders2', 'orders3', 'orders4', 'orders5']
ORDERS_TABLES = ['orders5', 'orders4', 'orders3', 'orders2', 'orders1', 'orders']

# SQL 模板：{ORDERS_TABLE} 会被替换为 orders/orders1/...，{VECTOR} 和 {LIMIT} 为占位符
SQL_TEMPLATE = """
SELECT
    n_name
FROM
    customer,
    {ORDERS_TABLE},
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
    AND l2_distance(ps_text_embedding, '{VECTOR}') < 0.925
ORDER BY
    l2_distance(ps_text_embedding, '{VECTOR}') APPROXIMATE
LIMIT {LIMIT}
"""

console = Console()


def drop_os_cache():
    """尝试清空 OS 页缓存 (需要 root 权限)"""
    try:
        subprocess.run(
            ['sync'],
            check=True,
            capture_output=True,
            timeout=10,
        )
        with open('/proc/sys/vm/drop_caches', 'w') as f:
            f.write('3')
        return True
    except (PermissionError, FileNotFoundError, subprocess.TimeoutExpired) as e:
        console.print(f"[yellow]无法清空 OS 缓存 (需 root): {e}[/yellow]")
        return False


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


def run_benchmark(args):
    vectors = read_vectors(CONFIG['vector_file'], CONFIG['vector_limit'])
    tables = list(ORDERS_TABLES)
    if args.random_order:
        random.shuffle(tables)
        console.print("[yellow]已启用随机顺序测试[/yellow]")

    conn = mysql.connector.connect(
        host=CONFIG['db_host'],
        port=CONFIG['db_port'],
        user=CONFIG['db_user'],
        database=CONFIG['db_name'],
        autocommit=True,
        allow_local_infile=True,
        sql_mode='',
        charset='utf8mb4',
        use_unicode=True,
    )
    cursor = conn.cursor()

    try:
        cursor.execute("SET SESSION optimizer_dynamic_sampling = 0")
    except Exception as e:
        console.print(f"[yellow]警告: 无法禁用动态采样: {e}[/yellow]")

    report = {}  # table_name -> list of latencies (ms)

    for orders_table in tables:
        report[orders_table] = []
        console.print(f"[*] 正在测试表 {orders_table}...")

        for vec_idx, vec in enumerate(vectors):
            vec_str = "[" + ",".join(map(str, vec)) + "]"
            sql = (
                SQL_TEMPLATE.replace('{ORDERS_TABLE}', orders_table)
                .replace('{VECTOR}', vec_str)
                .replace('{LIMIT}', str(CONFIG['result_limit']))
            )

            # 可选：每次执行前清空 OS 缓存 (需 root)
            if args.drop_cache:
                drop_os_cache()

            # 可选：warmup 轮次
            for _ in range(args.warmup):
                safe_execute_query(cursor, sql)
                try:
                    while cursor.nextset():
                        cursor.fetchall()
                except Exception:
                    pass

            # 正式计时：执行 args.runs 次
            run_times = []
            for run_idx in range(args.runs):
                if args.drop_cache and run_idx > 0:
                    drop_os_cache()
                start = time.perf_counter()
                success, error = safe_execute_query(cursor, sql)
                elapsed_ms = (time.perf_counter() - start) * 1000
                if success:
                    run_times.append(elapsed_ms)
                else:
                    console.print(f"  [red]查询失败: {error}[/red]")
                try:
                    while cursor.nextset():
                        cursor.fetchall()
                except Exception:
                    pass

            if args.skip_first and len(run_times) > 1:
                run_times = run_times[1:]
            report[orders_table].extend(run_times)

    # 输出结果表格
    table = Table(
        title=f"tpch01 各 orders 表 SQL 执行延时 (共 {len(tables)} 个表, {len(vectors)} 向量 × {args.runs} 次)"
    )
    table.add_column("表名", style="cyan")
    table.add_column("有效样本数", style="blue", justify="right")
    table.add_column("平均延时 (ms)", style="magenta", justify="right")
    table.add_column("最小 (ms)", style="green", justify="right")
    table.add_column("最大 (ms)", style="red", justify="right")
    table.add_column("中位数 (ms)", style="yellow", justify="right")

    for t in ORDERS_TABLES:
        times = report.get(t, [])
        if times:
            table.add_row(
                t,
                str(len(times)),
                f"{np.mean(times):.2f}",
                f"{np.min(times):.2f}",
                f"{np.max(times):.2f}",
                f"{np.median(times):.2f}",
            )
        else:
            table.add_row(t, "0", "N/A", "N/A", "N/A", "N/A")

    console.print(table)

    if report:
        all_times = [x for v in report.values() for x in v]
        if all_times:
            console.print(
                f"\n[bold cyan]总体:[/bold cyan] 平均 {np.mean(all_times):.2f} ms, "
                f"中位数 {np.median(all_times):.2f} ms"
            )

    cursor.close()
    conn.close()


def main():
    parser = argparse.ArgumentParser(
        description="tpch01 数据库 orders 表变体 SQL 延时测试，支持缓存规避"
    )
    parser.add_argument(
        "--drop-cache",
        action="store_true",
        help="每次执行前清空 OS 页缓存 (需 root: echo 3 > /proc/sys/vm/drop_caches)",
    )
    parser.add_argument(
        "--warmup",
        type=int,
        default=0,
        metavar="N",
        help="每个查询正式计时前先执行 N 次 warmup (不计入延时)",
    )
    parser.add_argument(
        "--runs",
        type=int,
        default=1,
        metavar="N",
        help="每个 (表, 向量) 组合执行 N 次，取所有样本统计",
    )
    parser.add_argument(
        "--skip-first",
        action="store_true",
        help="丢弃每个组合的第一次运行 (通常为冷缓存)，只统计后续运行",
    )
    parser.add_argument(
        "--random-order",
        action="store_true",
        help="随机打乱表测试顺序，减少顺序访问带来的 cache 影响",
    )
    args = parser.parse_args()

    console.print(
        Panel.fit(
            "[bold blue]tpch01 orders 表变体 SQL 延时测试[/bold blue]\n"
            f"数据库: {CONFIG['db_name']} | 表: {', '.join(ORDERS_TABLES)}\n"
            f"缓存策略: warmup={args.warmup}, runs={args.runs}, skip_first={args.skip_first}, "
            f"drop_cache={args.drop_cache}, random_order={args.random_order}",
            border_style="blue",
        )
    )
    console.print()

    run_benchmark(args)


if __name__ == "__main__":
    main()
