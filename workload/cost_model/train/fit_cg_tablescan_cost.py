#!/usr/bin/env python3
"""
列存表 orders_cg 扫描算子代价公式拟合脚本

代价公式:
  IO:  ioFactor * rows * rowSize + c_io
  CPU: rows * log2(rowSize) * scanFactor + c_cpu
  Total: ioFactor * (rows * rowSize) + scanFactor * (rows * log2(rowSize)) + c

其中 rowSize 为扫描列的总大小（bytes），rows 为表基数。

方法:
  1. 对 orders_cg 执行不同列的 SELECT 扫描（单列、多列、全列）
  2. 测量延时，rows 固定为表基数
  3. 多元线性回归拟合 ioFactor, scanFactor, c

依赖: pip install mysql-connector-python numpy rich
用法: python fit_cg_tablescan_cost.py
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
    'table': 'orders_cg',
    'warmup_runs': 0,
    'measure_runs': 1,
    'shuffle_seed': 47,
    'output_file': 'cg_tablescan_cost_coefficients.json',
}

# orders 表各列大小估计 (bytes)，基于 TPC-H schema
# INTEGER=4, CHAR(1)=1, DECIMAL(15,2)=8, DATE=4, CHAR(15)=15, VARCHAR(79)=79
ORDERS_COLUMN_SIZES = {
    'o_orderkey': 4, 'o_custkey': 4, 'o_orderstatus': 1, 'o_totalprice': 8,
    'o_orderdate': 4, 'o_orderpriority': 15, 'o_clerk': 15, 'o_shippriority': 4,
    'o_comment': 79,
    # 大写别名（部分库使用大写列名）
    'O_ORDERKEY': 4, 'O_CUSTKEY': 4, 'O_ORDERSTATUS': 1, 'O_TOTALPRICE': 8,
    'O_ORDERDATE': 4, 'O_ORDERPRIORITY': 15, 'O_CLERK': 15, 'O_SHIPPRIORITY': 4,
    'O_COMMENT': 79,
}

# 扫描查询定义: (query_id, columns_list) -> rowSize = sum(ORDERS_COLUMN_SIZES[c])
# 列名可能为小写 (o_orderkey) 或大写 (O_ORDERKEY)，需与表实际一致
SCAN_QUERIES = [
    ("o_orderkey", ["o_orderkey"]),
    ("o_custkey", ["o_custkey"]),
    ("o_orderdate", ["o_orderdate"]),
    ("o_orderkey_custkey", ["o_orderkey", "o_custkey"]),
    ("o_orderkey_date", ["o_orderkey", "o_orderdate"]),
    ("o_orderkey_custkey_date", ["o_orderkey", "o_custkey", "o_orderdate"]),
    ("o_orderkey_custkey_date_totalprice", ["o_orderkey", "o_custkey", "o_orderdate", "o_totalprice"]),
    ("o_orderkey_custkey_date_totalprice_priority", ["o_orderkey", "o_custkey", "o_orderdate", "o_totalprice", "o_orderpriority"]),
    ("o_orderkey_custkey_date_totalprice_priority_clerk", ["o_orderkey", "o_custkey", "o_orderdate", "o_totalprice", "o_orderpriority", "o_clerk"]),
    ("o_orderkey_custkey_date_totalprice_priority_clerk_shippriority", ["o_orderkey", "o_custkey", "o_orderdate", "o_totalprice", "o_orderpriority", "o_clerk", "o_shippriority"]),
    ("o_orderkey_custkey_status_date", ["o_orderkey", "o_custkey", "o_orderstatus", "o_orderdate"]),
    ("full", ["o_orderkey", "o_custkey", "o_orderstatus", "o_totalprice", "o_orderdate", "o_orderpriority", "o_clerk", "o_shippriority", "o_comment"]),
]

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


def get_table_row_count(cursor, table: str) -> Optional[int]:
    """从 COUNT(*) 获取表总行数"""
    try:
        success, _, rows = safe_execute_query(cursor, f"SELECT COUNT(*) FROM {table}")
        if success and rows:
            return int(rows[0][0])
    except Exception:
        pass
    return None


def get_actual_column_names(cursor, table: str) -> List[str]:
    """获取表实际列名（可能为小写或大写）"""
    try:
        success, _, rows = safe_execute_query(cursor, f"SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='{table}' ORDER BY ORDINAL_POSITION")
        if success and rows:
            return [str(r[0]) for r in rows]
    except Exception:
        pass
    return list(ORDERS_COLUMN_SIZES.keys())


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

    table = CONFIG['table']

    # 检查表是否存在
    success, _, rows = safe_execute_query(cursor, f"SHOW TABLES LIKE '{table}'")
    if not success or not rows:
        console.print(f"[red]表 {table} 不存在[/red]")
        cursor.close()
        conn.close()
        return

    # 获取表基数
    n_rows = get_table_row_count(cursor, table)
    if n_rows is None or n_rows <= 0:
        console.print(f"[red]无法获取表 {table} 的基数[/red]")
        cursor.close()
        conn.close()
        return

    # 获取实际列名
    actual_cols = get_actual_column_names(cursor, table)
    actual_cols_lower = {c.lower(): c for c in actual_cols}

    # 打乱执行顺序
    query_list = list(SCAN_QUERIES)
    rng = random.Random(CONFIG.get('shuffle_seed', 47))
    rng.shuffle(query_list)

    samples: List[dict] = []

    for qid, cols in query_list:
        # 计算 rowSize（扫描列的总大小）
        row_size = sum(ORDERS_COLUMN_SIZES.get(c, ORDERS_COLUMN_SIZES.get(c.lower(), 4)) for c in cols)
        row_size = max(1, row_size)

        # 构建 SELECT 语句，使用实际存在的列
        select_cols = []
        for c in cols:
            cl = c.lower()
            if cl in actual_cols_lower:
                select_cols.append(actual_cols_lower[cl])
            else:
                console.print(f"[yellow]跳过 {qid}: 列 {c} 不存在[/yellow]")
                select_cols = None
                break
        if select_cols is None:
            continue

        cols_str = ", ".join(select_cols)
        sql = f"SELECT {cols_str} FROM {table}"

        # Warmup
        for _ in range(CONFIG['warmup_runs']):
            safe_execute_query(cursor, sql)

        # 测量
        latencies = []
        for _ in range(CONFIG['measure_runs']):
            start = time.perf_counter()
            success, err, _ = safe_execute_query(cursor, sql)
            elapsed_ms = (time.perf_counter() - start) * 1000
            if success:
                latencies.append(elapsed_ms)
            else:
                console.print(f"[red]{qid} 失败: {err}[/red]")
                break

        if not latencies:
            continue

        latency = np.median(latencies)
        rows_float = float(n_rows)
        log2_rs = math.log2(row_size)
        x1 = rows_float * row_size
        x2 = rows_float * log2_rs

        samples.append({
            'query_id': qid,
            'columns': cols,
            'rows': rows_float,
            'row_size': row_size,
            'rows_x_row_size': x1,
            'rows_x_log2_row_size': x2,
            'latency_ms': latency,
        })

        console.print(
            f"  [green]{qid}[/green]: rows={n_rows}, rowSize={row_size}, "
            f"rows×rowSize={x1:.0f}, rows×log2(rowSize)={x2:.0f}, latency={latency:.2f} ms"
        )

    cursor.close()
    conn.close()

    if len(samples) < 3:
        console.print("[red]有效样本不足，无法拟合[/red]")
        return

    # 回归: latency = ioFactor * (rows * rowSize) + scanFactor * (rows * log2(rowSize)) + c
    X1 = np.array([s['rows_x_row_size'] for s in samples])
    X2 = np.array([s['rows_x_log2_row_size'] for s in samples])
    y = np.array([s['latency_ms'] for s in samples])
    X = np.column_stack([X1, X2, np.ones(len(samples))])

    coeffs = np.linalg.lstsq(X, y, rcond=None)[0]
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
        f"[bold]列存表 {table} 扫描代价公式拟合结果[/bold]\n\n"
        f"公式:\n"
        f"  IO:  ioFactor * rows * rowSize + c_io\n"
        f"  CPU: rows * log2(rowSize) * scanFactor + c_cpu\n"
        f"  Total: ioFactor * (rows * rowSize) + scanFactor * (rows * log2(rowSize)) + c\n\n"
        f"  ioFactor   = {io_factor:.6e}\n"
        f"  scanFactor = {scan_factor:.6e}\n"
        f"  c          = {c:.6f}\n\n"
        f"  RMSE = {rmse:.4f} ms\n"
        f"  R²   = {r2:.4f}\n"
        f"  有效样本数 = {len(samples)}",
        title="拟合系数",
        border_style="green",
    ))

    # 计算过程表格
    table_out = Table(title="预测 vs 实际延时")
    table_out.add_column("Query", style="cyan")
    table_out.add_column("rows", justify="right")
    table_out.add_column("rowSize", justify="right")
    table_out.add_column("实际 (ms)", justify="right")
    table_out.add_column("预测 (ms)", justify="right")
    table_out.add_column("误差 (%)", justify="right")

    for i, s in enumerate(samples):
        err_pct = 100 * (y_pred[i] - s['latency_ms']) / s['latency_ms'] if s['latency_ms'] != 0 else 0
        table_out.add_row(
            s['query_id'],
            f"{s['rows']:.0f}",
            str(s['row_size']),
            f"{s['latency_ms']:.2f}",
            f"{y_pred[i]:.2f}",
            f"{err_pct:.1f}%",
        )
    console.print(table_out)

    # 保存
    result = {
        'formula': {
            'io': 'ioFactor * rows * rowSize + c_io',
            'cpu': 'rows * log2(rowSize) * scanFactor + c_cpu',
            'total': 'ioFactor * (rows * rowSize) + scanFactor * (rows * log2(rowSize)) + c',
        },
        'coefficients': {
            'ioFactor': float(io_factor),
            'scanFactor': float(scan_factor),
            'c': float(c),
        },
        'table': table,
        'metrics': {'rmse_ms': float(rmse), 'r2': float(r2), 'n_samples': len(samples)},
        'column_sizes': ORDERS_COLUMN_SIZES,
        'samples': samples,
    }

    out_path = CONFIG['output_file']
    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump(result, f, indent=2, ensure_ascii=False)

    console.print(f"\n[bold green]系数已保存到: {out_path}[/bold green]")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="列存表 orders_cg 扫描代价拟合")
    parser.add_argument("--db-name", default=None, help="数据库名")
    parser.add_argument("--db-host", default=None, help="数据库主机")
    parser.add_argument("--db-port", type=int, default=None, help="数据库端口")
    parser.add_argument("--table", default=None, help="列存表名，默认 orders_cg")
    parser.add_argument("--output", "-o", default=None, help="输出 JSON 文件路径")
    parser.add_argument("--runs", type=int, default=1, help="每条查询测量次数")
    args = parser.parse_args()

    if args.db_name is not None:
        CONFIG["db_name"] = args.db_name
    if args.db_host is not None:
        CONFIG["db_host"] = args.db_host
    if args.db_port is not None:
        CONFIG["db_port"] = args.db_port
    if args.table is not None:
        CONFIG["table"] = args.table
    if args.output is not None:
        CONFIG["output_file"] = args.output
    if args.runs is not None:
        CONFIG["measure_runs"] = args.runs

    run_calibration()
