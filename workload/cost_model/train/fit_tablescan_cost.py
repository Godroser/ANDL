#!/usr/bin/env python3
"""
TableScan 代价公式拟合脚本

代价公式:
  IO:  ioFactor * rows * rowSize + c_io
  CPU: scanFactor * rows * log2(rowSize) + c_cpu
  Total: ioFactor * (rows * rowSize) + scanFactor * (rows * log2(rowSize)) + c

通过执行多种全表扫描 SQL，收集 (rows, rowSize, latency)，
用线性回归拟合 ioFactor, scanFactor, c。

依赖: pip install mysql-connector-python numpy rich
用法: python fit_tablescan_cost.py
      或修改 CONFIG 中的 db_name/db_host/db_port 后执行
"""

import time
import json
import math
import random
import argparse
import numpy as np
import mysql.connector
from typing import List, Tuple, Optional
from rich.console import Console
from rich.table import Table
from rich.panel import Panel

# --- 配置 ---
CONFIG = {
    'db_host': '127.0.0.1',
    'db_port': 10200,
    'db_user': 'root',
    'db_name': 'tpch10',
    'warmup_runs': 0,
    'measure_runs': 1,
    'shuffle_seed': 42,
    'output_file': 'tablescan_cost_coefficients_0309.json',
}

# TPC-H 表平均行大小估计 (bytes)，基于 load_table_tpch01.sql schema
# 包含 vector 列的表行更大
TABLE_ROW_SIZES = {
    'region': 181,
    'nation': 185,
    'supplier': 197,
    'customer': 223,
    'part': 3253,       # 含 vector(768) ~ 3072 bytes
    'partsupp': 3291,   # 含 vector(768)
    'orders': 152,
    'lineitem': 155,
}

console = Console()


def safe_execute_query(cursor, sql: str) -> Tuple[bool, Optional[str], Optional[list]]:
    """执行 SQL，返回 (成功, 错误信息, 结果行列表)"""
    try:
        try:
            while cursor.nextset():
                cursor.fetchall()
        except Exception:
            pass

        cursor.execute(sql)
        rows = cursor.fetchall()

        while True:
            try:
                if cursor.nextset():
                    cursor.fetchall()
                else:
                    break
            except Exception:
                break

        return True, None, rows
    except Exception as e:
        try:
            while cursor.nextset():
                cursor.fetchall()
        except Exception:
            pass
        return False, str(e), None


def parse_explain_rows(cursor, sql: str) -> Optional[float]:
    """
    执行 EXPLAIN 并解析出 TableScan 的 rows 估计值。
    若有多表/多算子，取主表 scan 的 rows（通常为第一个或最大的）。
    """
    explain_sql = f"EXPLAIN {sql}"
    success, err, rows = safe_execute_query(cursor, explain_sql)
    if not success or not rows:
        return None

    # 尝试 EXPLAIN FORMAT=JSON
    explain_json_sql = f"EXPLAIN FORMAT=JSON {sql}"
    success_json, err_json, rows_json = safe_execute_query(cursor, explain_json_sql)
    if success_json and rows_json and len(rows_json) > 0:
        try:
            # OceanBase/MySQL EXPLAIN FORMAT=JSON 返回单行，列为 JSON 字符串
            json_str = rows_json[0][0] if isinstance(rows_json[0][0], str) else str(rows_json[0][0])
            data = json.loads(json_str)
            return _extract_rows_from_explain_json(data)
        except (json.JSONDecodeError, KeyError, TypeError):
            pass

    # 回退：解析传统 EXPLAIN 的 rows 列
    if rows and len(rows) > 0:
        # 标准 EXPLAIN 列顺序: id, select_type, table, partitions, type, possible_keys, key, key_len, ref, rows, filtered, Extra
        try:
            desc = cursor.description
            cols = [d[0] for d in desc] if desc else []
            row_vals = list(rows[0]) if isinstance(rows[0], (list, tuple)) else [rows[0]]
            idx = 9
            if cols:
                for i, col in enumerate(cols):
                    if col and 'rows' in str(col).lower():
                        idx = i
                        break
            if idx < len(row_vals):
                val = row_vals[idx]
                if val is not None:
                    return float(val)
        except (IndexError, ValueError, StopIteration, TypeError):
            pass

        # 取所有行中 rows 列的最大值（多表时）
        max_rows = 0
        for r in rows:
            row_list = list(r) if hasattr(r, '__iter__') and not isinstance(r, str) else [r]
            for v in row_list:
                if isinstance(v, (int, float)) and v > max_rows:
                    max_rows = v
                elif isinstance(v, str) and v.replace('.', '').isdigit():
                    max_rows = max(max_rows, float(v))
        if max_rows > 0:
            return float(max_rows)

    return None


def _extract_rows_from_explain_json(data: dict) -> Optional[float]:
    """从 EXPLAIN FORMAT=JSON 中递归提取 rows 估计"""
    rows_list = []

    def collect(o):
        if isinstance(o, dict):
            for k, v in o.items():
                if k in ('rows', 'rows_examined_per_scan', 'rows_produced_by_join'):
                    try:
                        rows_list.append(float(v))
                    except (TypeError, ValueError):
                        pass
                collect(v)
        elif isinstance(o, list):
            for x in o:
                collect(x)

    collect(data)
    return max(rows_list) if rows_list else None


def get_table_row_count(cursor, table: str) -> Optional[int]:
    """从 COUNT(*) 获取表总行数（仅用于无过滤的全表扫描）"""
    try:
        success, _, rows = safe_execute_query(cursor, f"SELECT COUNT(*) FROM {table}")
        if success and rows:
            return int(rows[0][0])
    except Exception:
        pass
    return None


def get_query_result_count(cursor, sql: str) -> Optional[int]:
    """执行 SQL 并返回实际返回的结果行数，作为准确的 rows 基数"""
    try:
        success, _, rows = safe_execute_query(cursor, sql)
        if success and rows is not None:
            return len(rows)
    except Exception:
        pass
    return None


# --- 定义 Scan 查询 ---
# 每个元素: (query_id, sql, table_name, use_full_row_size)
# 包含全列扫描（SELECT *）和部分列扫描（SELECT 指定列），
# rows 一律使用表基数，rowSize 一律使用全列行大小。
def _build_scan_queries() -> List[Tuple[str, str, str, bool]]:
    table_order = ['lineitem', 'orders', 'supplier', 'customer', 'part', 'region', 'nation']
    projection_columns = {
        'lineitem': "l_orderkey, l_partkey, l_suppkey",
        'orders': "o_orderkey, o_custkey, o_orderdate",
        'supplier': "s_suppkey, s_nationkey, s_name",
        'customer': "c_custkey, c_nationkey, c_name",
        'part': "p_partkey, p_name, p_mfgr",
        'region': "r_regionkey, r_name",
        'nation': "n_nationkey, n_regionkey, n_name",
    }
    sql_templates = [
        ("full", "SELECT * FROM {table}"),
        ("projected", "SELECT {projection} FROM {table}"),
        ("projected_alias", "SELECT t.{projection_alias} FROM {table} t"),
    ]

    per_table_queries = {}
    for table in table_order:
        table_queries: List[Tuple[str, str, str, bool]] = []
        for suffix, template in sql_templates:
            qid = f"{table}_{suffix}"
            projection = projection_columns[table]
            projection_alias = projection.replace(", ", ", t.")
            sql = template.format(
                table=table,
                projection=projection,
                projection_alias=projection_alias,
            )
            table_queries.append((qid, sql, table, True))
        per_table_queries[table] = table_queries

    # 打散执行顺序：尽量避免同一表 SQL 连续执行，减少缓存放大效应。
    rng = random.Random(CONFIG.get('shuffle_seed', 42))
    queries: List[Tuple[str, str, str, bool]] = []
    last_table = None
    while True:
        available_tables = [t for t, qs in per_table_queries.items() if qs]
        if not available_tables:
            break
        candidates = [t for t in available_tables if t != last_table] or available_tables
        chosen_table = rng.choice(candidates)
        queries.append(per_table_queries[chosen_table].pop(0))
        last_table = chosen_table

    return queries


SCAN_QUERIES = _build_scan_queries()


def run_calibration():
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

    samples: List[dict] = []

    all_queries = []
    table_cardinality_cache = {}
    for q in SCAN_QUERIES:
        qid, sql, table_name, _ = q
        if table_name not in table_cardinality_cache:
            table_rows = get_table_row_count(cursor, table_name)
            if table_rows is None or table_rows <= 0:
                console.print(f"[yellow]跳过 {qid}: 无法获取表 {table_name} 的基数[/yellow]")
                continue
            table_cardinality_cache[table_name] = table_rows
        all_queries.append(q)

    for qid, sql, table_name, _ in all_queries:
        if table_name not in TABLE_ROW_SIZES:
            console.print(f"[yellow]跳过 {qid}: 未配置表 {table_name} 的 row_size[/yellow]")
            continue
        if table_name not in table_cardinality_cache:
            console.print(f"[yellow]跳过 {qid}: 未缓存表 {table_name} 的基数[/yellow]")
            continue

        # rows 固定取表基数，row_size 固定取表全列平均行大小
        row_size = TABLE_ROW_SIZES[table_name]
        rows_est = float(table_cardinality_cache[table_name])

        # Warmup
        for _ in range(CONFIG['warmup_runs']):
            safe_execute_query(cursor, sql)

        # 测量延时
        latencies = []
        for _ in range(CONFIG['measure_runs']):
            start = time.perf_counter()
            success, err, _ = safe_execute_query(cursor, sql)
            elapsed_ms = (time.perf_counter() - start) * 1000
            if success:
                latencies.append(elapsed_ms)
            else:
                console.print(f"[red]{qid} 执行失败: {err}[/red]")
                break

        if not latencies:
            continue

        latency = np.median(latencies)
        row_size_safe = max(1, row_size)
        log2_row_size = math.log2(row_size_safe)

        samples.append({
            'query_id': qid,
            'rows': rows_est,
            'row_size': row_size,
            'rows_x_row_size': rows_est * row_size,
            'rows_x_log2_row_size': rows_est * log2_row_size,
            'latency_ms': latency,
        })
        console.print(f"  [green]{qid}[/green]: rows={rows_est:.0f}, rowSize={row_size}, latency={latency:.2f} ms")

    cursor.close()
    conn.close()

    if len(samples) < 3:
        console.print("[red]有效样本不足，无法拟合[/red]")
        return

    # 回归: latency = ioFactor * (rows * rowSize) + scanFactor * (rows * log2(rowSize)) + c
    X1 = np.array([s['rows_x_row_size'] for s in samples])
    X2 = np.array([s['rows_x_log2_row_size'] for s in samples])
    y = np.array([s['latency_ms'] for s in samples])

    # 构造设计矩阵 [X1, X2, 1]
    X = np.column_stack([X1, X2, np.ones(len(samples))])

    # 最小二乘
    coeffs, residuals, rank, s = np.linalg.lstsq(X, y, rcond=None)
    coeffs = np.atleast_1d(coeffs)
    coeffs = np.pad(coeffs, (0, max(0, 3 - len(coeffs))), constant_values=0)[:3]
    io_factor, scan_factor, c = float(coeffs[0]), float(coeffs[1]), float(coeffs[2])

    # 预测与残差
    y_pred = X @ coeffs
    mse = np.mean((y - y_pred) ** 2)
    rmse = np.sqrt(mse)
    ss_res = np.sum((y - y_pred) ** 2)
    ss_tot = np.sum((y - np.mean(y)) ** 2)
    r2 = 1 - ss_res / ss_tot if ss_tot > 1e-10 else 0

    # 输出结果
    console.print(Panel.fit(
        f"[bold]TableScan 代价公式拟合结果[/bold]\n\n"
        f"公式: cost = ioFactor * (rows * rowSize) + scanFactor * (rows * log2(rowSize)) + c\n\n"
        f"  ioFactor   = {io_factor:.6e}\n"
        f"  scanFactor = {scan_factor:.6e}\n"
        f"  c          = {c:.6f}\n\n"
        f"  RMSE = {rmse:.4f} ms\n"
        f"  R²   = {r2:.4f}\n"
        f"  有效样本数 = {len(samples)}",
        title="拟合系数",
        border_style="green",
    ))

    # 预测 vs 实际表格
    table = Table(title="预测 vs 实际延时")
    table.add_column("Query", style="cyan")
    table.add_column("Rows", justify="right")
    table.add_column("RowSize", justify="right")
    table.add_column("实际 (ms)", justify="right")
    table.add_column("预测 (ms)", justify="right")
    table.add_column("误差 (%)", justify="right")

    for i, s in enumerate(samples):
        err_pct = 100 * (y_pred[i] - y[i]) / y[i] if y[i] != 0 else 0
        table.add_row(
            s['query_id'],
            f"{s['rows']:.0f}",
            str(s['row_size']),
            f"{s['latency_ms']:.2f}",
            f"{y_pred[i]:.2f}",
            f"{err_pct:.1f}%",
        )
    console.print(table)

    # 保存
    result = {
        'formula': {
            'io': 'ioFactor * rows * rowSize + c_io',
            'cpu': 'scanFactor * rows * log2(rowSize) + c_cpu',
            'total': 'ioFactor * (rows * rowSize) + scanFactor * (rows * log2(rowSize)) + c',
        },
        'coefficients': {
            'ioFactor': float(io_factor),
            'scanFactor': float(scan_factor),
            'c': float(c),
        },
        'metrics': {
            'rmse_ms': float(rmse),
            'r2': float(r2),
            'n_samples': len(samples),
        },
        'samples': samples,
    }

    out_path = CONFIG['output_file']
    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump(result, f, indent=2, ensure_ascii=False)

    console.print(f"\n[bold green]系数已保存到: {out_path}[/bold green]")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="TableScan 代价公式拟合")
    parser.add_argument("--db-name", default=None, help="数据库名，默认 tpch10")
    parser.add_argument("--db-host", default=None, help="数据库主机")
    parser.add_argument("--db-port", type=int, default=None, help="数据库端口")
    parser.add_argument("--output", "-o", default=None, help="输出 JSON 文件路径")
    parser.add_argument("--runs", type=int, default=3, help="每条查询测量次数")
    args = parser.parse_args()

    if args.db_name is not None:
        CONFIG["db_name"] = args.db_name
    if args.db_host is not None:
        CONFIG["db_host"] = args.db_host
    if args.db_port is not None:
        CONFIG["db_port"] = args.db_port
    if args.output is not None:
        CONFIG["output_file"] = args.output
    if args.runs is not None:
        CONFIG["measure_runs"] = args.runs

    run_calibration()
