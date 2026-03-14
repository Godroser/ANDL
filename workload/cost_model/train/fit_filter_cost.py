#!/usr/bin/env python3
"""
Filter 算子代价公式拟合脚本

代价公式:
  cost = rows * cpufactor * numFuncs

其中 rows 为表基数（扫描行数），numFuncs 为过滤条件数量。

方法:
  1. 执行 SELECT * FROM table WHERE cond1 AND cond2 AND ... 测量总延时
  2. 用 Scan 公式计算 Scan 延时: L_scan = 4.38e-5*(rows*rowSize) + 1.51e-4*(rows*log2(rowSize)) + 315.2
  3. Filter 延时 = 总延时 - Scan 延时
  4. 线性回归拟合 cpufactor: filter_latency = cpufactor * (rows * numFuncs)

依赖: pip install mysql-connector-python numpy rich
用法: python fit_filter_cost.py
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

# --- Scan 公式系数: L_scan = 4.38e-5*(rows*rowSize) + 1.51e-4*(rows*log2(rowSize)) + 315.2 ---
SCAN_IO_FACTOR = 4.38e-5
SCAN_CPU_FACTOR = 1.51e-4
SCAN_CONSTANT = 315.2


def scan_latency_ms(rows: float, row_size: float) -> float:
    """Scan 算子延时公式 (ms)"""
    rs = max(1, row_size)
    return SCAN_IO_FACTOR * (rows * row_size) + SCAN_CPU_FACTOR * (rows * math.log2(rs)) + SCAN_CONSTANT


# --- 配置 ---
CONFIG = {
    'db_host': '127.0.0.1',
    'db_port': 10200,
    'db_user': 'root',
    'db_name': 'tpch10',
    'warmup_runs': 0,
    'measure_runs': 1,
    'shuffle_seed': 43,
    'output_file': 'filter_cost_coefficients.json',
}

TABLE_ROW_SIZES = {
    'region': 181,
    'nation': 185,
    'supplier': 197,
    'customer': 223,
    'part': 3253,
    'orders': 152,
    'lineitem': 155,
}

# 每表可用于 filter 的列（整数或可比较类型），条件形式 col >= 0 保证全表扫描
FILTER_COLUMNS = {
    'lineitem': ['l_orderkey', 'l_partkey', 'l_suppkey', 'l_linenumber', 'l_quantity'],
    'orders': ['o_orderkey', 'o_custkey', 'o_shippriority'],
    'supplier': ['s_suppkey', 's_nationkey'],
    'customer': ['c_custkey', 'c_nationkey'],
    'part': ['p_partkey', 'p_size'],
    'region': ['r_regionkey'],
    'nation': ['n_nationkey', 'n_regionkey'],
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


def get_table_row_count(cursor, table: str) -> Optional[int]:
    """从 COUNT(*) 获取表总行数"""
    try:
        success, _, rows = safe_execute_query(cursor, f"SELECT COUNT(*) FROM {table}")
        if success and rows:
            return int(rows[0][0])
    except Exception:
        pass
    return None


def _build_filter_queries() -> List[Tuple[str, str, str, int]]:
    """
    生成 SELECT * FROM table WHERE cond1 AND cond2 AND ... 的查询。
    返回: (query_id, sql, table_name, num_funcs)
    """
    queries: List[Tuple[str, str, str, int]] = []
    table_order = ['lineitem', 'orders', 'supplier', 'customer', 'part', 'region', 'nation']

    for table in table_order:
        cols = FILTER_COLUMNS.get(table, [])
        if not cols:
            continue
        for n in range(1, min(len(cols) + 1, 6)):  # numFuncs 1..min(5, len(cols))
            conds = [f"{c} >= 0" for c in cols[:n]]
            where_clause = " AND ".join(conds)
            sql = f"SELECT * FROM {table} WHERE {where_clause}"
            qid = f"{table}_filter_{n}funcs"
            queries.append((qid, sql, table, n))

    # 打散执行顺序
    rng = random.Random(CONFIG.get('shuffle_seed', 43))
    rng.shuffle(queries)
    return queries


FILTER_QUERIES = _build_filter_queries()


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
    table_cardinality_cache: dict = {}

    for qid, sql, table_name, num_funcs in FILTER_QUERIES:
        if table_name not in TABLE_ROW_SIZES:
            console.print(f"[yellow]跳过 {qid}: 未配置表 {table_name} 的 row_size[/yellow]")
            continue

        if table_name not in table_cardinality_cache:
            table_rows = get_table_row_count(cursor, table_name)
            if table_rows is None or table_rows <= 0:
                console.print(f"[yellow]跳过 {qid}: 无法获取表 {table_name} 的基数[/yellow]")
                continue
            table_cardinality_cache[table_name] = table_rows

        rows = float(table_cardinality_cache[table_name])
        row_size = TABLE_ROW_SIZES[table_name]

        # Warmup
        for _ in range(CONFIG['warmup_runs']):
            safe_execute_query(cursor, sql)

        # 测量总延时
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

        latency_total = np.median(latencies)

        # Scan 延时 (ms)
        latency_scan = scan_latency_ms(rows, row_size)

        # Filter 延时 = 总延时 - Scan 延时
        latency_filter = latency_total - latency_scan

        # 若 filter 延时为负，说明测量噪声或 Scan 公式偏差，置为 0 或跳过
        if latency_filter < 0:
            console.print(f"  [dim]{qid}[/dim]: rows={rows:.0f}, numFuncs={num_funcs}, "
                         f"total={latency_total:.2f} ms, scan={latency_scan:.2f} ms, "
                         f"filter={latency_filter:.2f} ms (负值，跳过)")
            continue

        rows_x_num_funcs = rows * num_funcs

        samples.append({
            'query_id': qid,
            'rows': rows,
            'num_funcs': num_funcs,
            'rows_x_num_funcs': rows_x_num_funcs,
            'latency_total_ms': latency_total,
            'latency_scan_ms': latency_scan,
            'latency_filter_ms': latency_filter,
        })

        # 输出计算过程
        console.print(
            f"  [green]{qid}[/green]: rows={rows:.0f}, numFuncs={num_funcs}, "
            f"total={latency_total:.2f} ms, scan={latency_scan:.2f} ms, "
            f"filter={latency_filter:.2f} ms"
        )
        console.print(
            f"    [dim]  scan = {SCAN_IO_FACTOR:.2e}*({rows:.0f}*{row_size}) + "
            f"{SCAN_CPU_FACTOR:.2e}*({rows:.0f}*log2({row_size})) + {SCAN_CONSTANT} = {latency_scan:.2f} ms[/dim]"
        )
        console.print(
            f"    [dim]  filter = total - scan = {latency_total:.2f} - {latency_scan:.2f} = {latency_filter:.2f} ms[/dim]"
        )

    cursor.close()
    conn.close()

    if len(samples) < 2:
        console.print("[red]有效样本不足，无法拟合[/red]")
        return

    # 回归: latency_filter = cpufactor * (rows * numFuncs)
    # 即 y = cpufactor * X，无截距
    X = np.array([s['rows_x_num_funcs'] for s in samples])
    y = np.array([s['latency_filter_ms'] for s in samples])

    # 最小二乘: min ||y - cpufactor * X||^2 => cpufactor = (X'y) / (X'X)
    X = X.reshape(-1, 1)
    cpufactor = float(np.linalg.lstsq(X, y, rcond=None)[0][0])

    # 预测与残差
    y_pred = cpufactor * X.flatten()
    mse = np.mean((y - y_pred) ** 2)
    rmse = np.sqrt(mse)
    ss_res = np.sum((y - y_pred) ** 2)
    ss_tot = np.sum((y - np.mean(y)) ** 2)
    r2 = 1 - ss_res / ss_tot if ss_tot > 1e-10 else 0

    # 输出拟合结果与计算过程
    console.print(Panel.fit(
        f"[bold]Filter 代价公式拟合结果[/bold]\n\n"
        f"公式: cost = rows * cpufactor * numFuncs\n\n"
        f"  cpufactor = {cpufactor:.6e}\n\n"
        f"  RMSE = {rmse:.4f} ms\n"
        f"  R²   = {r2:.4f}\n"
        f"  有效样本数 = {len(samples)}\n\n"
        f"[bold]Scan 公式（用于计算 scan 延时）:[/bold]\n"
        f"  L_scan = {SCAN_IO_FACTOR:.2e} * (rows * rowSize) + {SCAN_CPU_FACTOR:.2e} * (rows * log2(rowSize)) + {SCAN_CONSTANT}",
        title="拟合系数",
        border_style="green",
    ))

    # 计算过程表格
    table = Table(title="计算过程与预测 vs 实际")
    table.add_column("Query", style="cyan")
    table.add_column("rows", justify="right")
    table.add_column("numFuncs", justify="right")
    table.add_column("total (ms)", justify="right")
    table.add_column("scan (ms)", justify="right")
    table.add_column("filter (ms)", justify="right")
    table.add_column("预测 filter (ms)", justify="right")
    table.add_column("误差 (%)", justify="right")

    for i, s in enumerate(samples):
        pred = y_pred[i]
        err_pct = 100 * (pred - s['latency_filter_ms']) / s['latency_filter_ms'] if s['latency_filter_ms'] != 0 else 0
        table.add_row(
            s['query_id'],
            f"{s['rows']:.0f}",
            str(s['num_funcs']),
            f"{s['latency_total_ms']:.2f}",
            f"{s['latency_scan_ms']:.2f}",
            f"{s['latency_filter_ms']:.2f}",
            f"{pred:.2f}",
            f"{err_pct:.1f}%",
        )
    console.print(table)

    # 回归计算过程
    console.print(Panel.fit(
        f"[bold]回归计算过程[/bold]\n\n"
        f"模型: filter_latency = cpufactor * (rows * numFuncs)\n"
        f"即 y = cpufactor * X，其中 X = rows * numFuncs\n\n"
        f"最小二乘: cpufactor = (X'X)^(-1) X'y = sum(X_i * y_i) / sum(X_i^2)\n\n"
        f"  sum(X_i * y_i) = {float(np.sum(X.flatten() * y)):.4e}\n"
        f"  sum(X_i^2)    = {float(np.sum(X.flatten() ** 2)):.4e}\n"
        f"  cpufactor     = {float(np.sum(X.flatten() * y)):.4e} / {float(np.sum(X.flatten() ** 2)):.4e} = {cpufactor:.6e}",
        title="回归推导",
        border_style="blue",
    ))

    # 保存
    result = {
        'formula': 'cost = rows * cpufactor * numFuncs',
        'coefficients': {'cpufactor': float(cpufactor)},
        'scan_formula': {
            'expr': f'{SCAN_IO_FACTOR:.2e} * (rows * rowSize) + {SCAN_CPU_FACTOR:.2e} * (rows * log2(rowSize)) + {SCAN_CONSTANT}',
            'io_factor': SCAN_IO_FACTOR,
            'cpu_factor': SCAN_CPU_FACTOR,
            'constant': SCAN_CONSTANT,
        },
        'metrics': {'rmse_ms': float(rmse), 'r2': float(r2), 'n_samples': len(samples)},
        'samples': samples,
    }

    out_path = CONFIG['output_file']
    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump(result, f, indent=2, ensure_ascii=False)

    console.print(f"\n[bold green]系数已保存到: {out_path}[/bold green]")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Filter 算子代价公式拟合")
    parser.add_argument("--db-name", default=None, help="数据库名")
    parser.add_argument("--db-host", default=None, help="数据库主机")
    parser.add_argument("--db-port", type=int, default=None, help="数据库端口")
    parser.add_argument("--output", "-o", default=None, help="输出 JSON 文件路径")
    parser.add_argument("--runs", type=int, default=1, help="每条查询测量次数")
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
