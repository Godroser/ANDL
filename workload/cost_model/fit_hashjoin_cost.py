#!/usr/bin/env python3
"""
HashJoin 算子代价公式拟合脚本

代价公式:
  IO:   buildRows * buildRowSize * (ioFactor + writeFactor)
  CPU:  buildRows * buildFilters * cpuFactor + buildRows * nKeys * cpuFactor
        + buildRows * buildRowSize * memFactor + buildRows * cpuFactor
        + probeRows * probeFilters * cpuFactor + probeRows * nKeys * cpuFactor
        + probeRows * probeRowSize * memFactor + probeRows * cpuFactor

对于小表 join 只需计算 CPU 部分（无 IO）。

已知: ioFactor = 4.38e-5, cpuFactor = 1.73e-3
待拟合: writeFactor, memFactor

方法:
  1. 执行 JOIN 查询，测量总延时
  2. 减去两表 Scan 延时，得到 HashJoin 延时
  3. 小表 join: 仅 CPU，拟合 memFactor
  4. 大表 join: IO+CPU，拟合 writeFactor 和 memFactor

依赖: pip install mysql-connector-python numpy rich
用法: python fit_hashjoin_cost.py
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

# --- 已知系数 ---
IO_FACTOR = 4.38e-5
CPU_FACTOR = 1.73e-3

# Scan 公式系数（用于扣减 Scan 延时）
SCAN_IO_FACTOR = 0 #4.38e-5
SCAN_CPU_FACTOR = 1.51e-4
SCAN_CONSTANT = 315.2

# 小表阈值 (bytes): buildRows*buildRowSize 低于此值视为小表 join，仅 CPU
SMALL_BUILD_THRESHOLD_BYTES = 1e8  # 100MB


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
    'shuffle_seed': 44,
    'output_file': 'hashjoin_cost_coefficients_new.json',
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

# JOIN 查询定义: (query_id, sql, build_table, probe_table, n_keys, build_filters, probe_filters)
# build/probe 由表基数决定，较小者为 build
JOIN_DEFINITIONS = [
    ("region_nation", "SELECT * FROM region r JOIN nation n ON r.r_regionkey = n.n_regionkey",
     "region", "nation", 1, 0, 0),
    ("nation_supplier", "SELECT * FROM nation n JOIN supplier s ON n.n_nationkey = s.s_nationkey",
     "nation", "supplier", 1, 0, 0),
    ("nation_customer", "SELECT * FROM nation n JOIN customer c ON n.n_nationkey = c.c_nationkey",
     "nation", "customer", 1, 0, 0),
    ("supplier_nation", "SELECT * FROM supplier s JOIN nation n ON s.s_nationkey = n.n_nationkey",
     "nation", "supplier", 1, 0, 0),
    ("customer_orders", "SELECT * FROM customer c JOIN orders o ON c.c_custkey = o.o_custkey",
     "customer", "orders", 1, 0, 0),
    ("orders_lineitem", "SELECT * FROM orders o JOIN lineitem l ON o.o_orderkey = l.l_orderkey",
     "orders", "lineitem", 1, 0, 0),
    ("part_lineitem", "SELECT * FROM part p JOIN lineitem l ON p.p_partkey = l.l_partkey",
     "part", "lineitem", 1, 0, 0),
    ("supplier_lineitem", "SELECT * FROM supplier s JOIN lineitem l ON s.s_suppkey = l.l_suppkey",
     "supplier", "lineitem", 1, 0, 0),
    # 带 filter 的 join
    ("nation_supplier_f1", "SELECT * FROM nation n JOIN supplier s ON n.n_nationkey = s.s_nationkey WHERE n.n_nationkey >= 0",
     "nation", "supplier", 1, 1, 0),
    ("nation_supplier_f2", "SELECT * FROM nation n JOIN supplier s ON n.n_nationkey = s.s_nationkey WHERE n.n_nationkey >= 0 AND s.s_suppkey >= 0",
     "nation", "supplier", 1, 1, 1),
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


def _known_cpu_part(build_rows: float, build_filters: int, n_keys: int,
                    probe_rows: float, probe_filters: int) -> float:
    """
    CPU 中已知部分（不含 memFactor 的项）:
    cpuFactor * (buildRows*buildFilters + buildRows*nKeys + buildRows + probeRows*probeFilters + probeRows*nKeys + probeRows)
    """
    return CPU_FACTOR * (
        build_rows * build_filters + build_rows * n_keys + build_rows
        + probe_rows * probe_filters + probe_rows * n_keys + probe_rows
    )


def _mem_factor_term(build_rows: float, build_row_size: float, probe_rows: float, probe_row_size: float) -> float:
    """memFactor 对应的项: buildRows*buildRowSize + probeRows*probeRowSize"""
    return build_rows * build_row_size + probe_rows * probe_row_size


def _io_term(build_rows: float, build_row_size: float, io_factor: float, write_factor: float) -> float:
    """IO 项: buildRows * buildRowSize * (ioFactor + writeFactor)"""
    return build_rows * build_row_size * (io_factor + write_factor)


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

    # 获取表基数
    table_cardinality: dict = {}
    for t in ['region', 'nation', 'supplier', 'customer', 'part', 'orders', 'lineitem']:
        cnt = get_table_row_count(cursor, t)
        if cnt is not None:
            table_cardinality[t] = cnt
        else:
            console.print(f"[yellow]无法获取表 {t} 的基数，跳过[/yellow]")

    # 打乱执行顺序
    join_list = list(JOIN_DEFINITIONS)
    rng = random.Random(CONFIG.get('shuffle_seed', 44))
    rng.shuffle(join_list)

    samples: List[dict] = []

    for qid, sql, build_t, probe_t, n_keys, build_filters, probe_filters in join_list:
        if build_t not in table_cardinality or probe_t not in table_cardinality:
            continue
        if build_t not in TABLE_ROW_SIZES or probe_t not in TABLE_ROW_SIZES:
            console.print(f"[yellow]跳过 {qid}: 未配置 row_size[/yellow]")
            continue

        build_rows = float(table_cardinality[build_t])
        probe_rows = float(table_cardinality[probe_t])
        build_row_size = TABLE_ROW_SIZES[build_t]
        probe_row_size = TABLE_ROW_SIZES[probe_t]

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

        # Scan 延时
        scan_build = scan_latency_ms(build_rows, build_row_size)
        scan_probe = scan_latency_ms(probe_rows, probe_row_size)
        scan_total = scan_build + scan_probe

        # HashJoin 延时 = 总延时 - Scan 延时
        latency_hashjoin = latency_total - scan_total

        if latency_hashjoin < 0:
            console.print(f"  [dim]{qid}[/dim]: total={latency_total:.2f} ms, scan={scan_total:.2f} ms, "
                         f"hashjoin={latency_hashjoin:.2f} ms (负值，跳过)")
            continue

        build_bytes = build_rows * build_row_size
        is_small_join = build_bytes < SMALL_BUILD_THRESHOLD_BYTES

        samples.append({
            'query_id': qid,
            'build_table': build_t,
            'probe_table': probe_t,
            'build_rows': build_rows,
            'probe_rows': probe_rows,
            'build_row_size': build_row_size,
            'probe_row_size': probe_row_size,
            'n_keys': n_keys,
            'build_filters': build_filters,
            'probe_filters': probe_filters,
            'latency_total_ms': latency_total,
            'scan_build_ms': scan_build,
            'scan_probe_ms': scan_probe,
            'latency_hashjoin_ms': latency_hashjoin,
            'is_small_join': is_small_join,
            'build_rows_x_row_size': build_rows * build_row_size,
            'probe_rows_x_row_size': probe_rows * probe_row_size,
            'mem_factor_term': _mem_factor_term(build_rows, build_row_size, probe_rows, probe_row_size),
            'known_cpu': _known_cpu_part(build_rows, build_filters, n_keys, probe_rows, probe_filters),
        })

        # 输出计算过程
        console.print(
            f"  [green]{qid}[/green]: build={build_t}({build_rows:.0f}), probe={probe_t}({probe_rows:.0f}), "
            f"nKeys={n_keys}, buildFilters={build_filters}, probeFilters={probe_filters}"
        )
        console.print(
            f"    total={latency_total:.2f} ms, scan_build={scan_build:.2f} ms, scan_probe={scan_probe:.2f} ms, "
            f"hashjoin={latency_hashjoin:.2f} ms"
        )
        console.print(
            f"    [dim]  scan_build = {SCAN_IO_FACTOR:.2e}*({build_rows:.0f}*{build_row_size}) + "
            f"{SCAN_CPU_FACTOR:.2e}*({build_rows:.0f}*log2({build_row_size})) + {SCAN_CONSTANT} = {scan_build:.2f} ms[/dim]"
        )
        console.print(
            f"    [dim]  scan_probe = {SCAN_IO_FACTOR:.2e}*({probe_rows:.0f}*{probe_row_size}) + "
            f"{SCAN_CPU_FACTOR:.2e}*({probe_rows:.0f}*log2({probe_row_size})) + {SCAN_CONSTANT} = {scan_probe:.2f} ms[/dim]"
        )
        console.print(
            f"    [dim]  hashjoin = total - scan_build - scan_probe = {latency_total:.2f} - {scan_build:.2f} - {scan_probe:.2f} = {latency_hashjoin:.2f} ms[/dim]"
        )
        console.print(f"    [dim]  is_small_join (CPU only) = {is_small_join}[/dim]")

    cursor.close()
    conn.close()

    if len(samples) < 2:
        console.print("[red]有效样本不足，无法拟合[/red]")
        return

    # 分离小表 join 和大表 join
    small_samples = [s for s in samples if s['is_small_join']]
    large_samples = [s for s in samples if not s['is_small_join']]

    # 拟合 memFactor
    # 模型: latency_hashjoin = known_cpu + memFactor * mem_factor_term  (小表，无 IO)
    # 或: latency_hashjoin = io_term + known_cpu + memFactor * mem_factor_term  (大表)
    # 对于小表: y = latency_hashjoin - known_cpu = memFactor * mem_factor_term
    # 对于大表: y = latency_hashjoin - known_cpu = buildRows*buildRowSize*(ioFactor+writeFactor) + memFactor*mem_factor_term
    #          = buildRows*buildRowSize*(ioFactor+writeFactor+memFactor) - buildRows*buildRowSize*memFactor + memFactor*probeRows*probeRowSize
    # 实际上: IO = buildRows*buildRowSize*(ioFactor+writeFactor)
    #        CPU_mem = memFactor*(buildRows*buildRowSize + probeRows*probeRowSize)
    # 所以: y = buildRows*buildRowSize*(ioFactor+writeFactor+memFactor) + probeRows*probeRowSize*memFactor
    # 令 X1 = buildRows*buildRowSize, X2 = probeRows*probeRowSize
    # y = (ioFactor+writeFactor+memFactor)*X1 + memFactor*X2 = coeff1*X1 + coeff2*X2
    # 则 memFactor = coeff2, writeFactor = coeff1 - ioFactor - coeff2

    # 统一回归: y = latency_hashjoin - known_cpu
    # 小表: y = memFactor * (X1 + X2), 即单变量 X = X1+X2
    # 大表: y = (ioFactor+writeFactor+memFactor)*X1 + memFactor*X2

    # 策略: 先用小表拟合 memFactor，再用大表拟合 writeFactor
    mem_factor = 0.0
    write_factor = 0.0

    if small_samples:
        # 小表: y = memFactor * (buildRows*buildRowSize + probeRows*probeRowSize)
        X_small = np.array([s['mem_factor_term'] for s in small_samples])
        y_small = np.array([s['latency_hashjoin_ms'] - s['known_cpu'] for s in small_samples])
        X_small = X_small.reshape(-1, 1)
        mem_factor = float(np.linalg.lstsq(X_small, y_small, rcond=None)[0][0])
        if mem_factor < 0:
            mem_factor = 0.0
        console.print(f"\n[bold]小表 join 拟合 memFactor:[/bold]")
        console.print(f"  模型: hashjoin - known_cpu = memFactor * (buildRows*buildRowSize + probeRows*probeRowSize)")
        sum_xy = float(np.sum(X_small.flatten() * y_small))
        sum_xx = float(np.sum(X_small.flatten() ** 2))
        console.print(f"  sum(X*y) = {sum_xy:.4e}, sum(X^2) = {sum_xx:.4e}")
        console.print(f"  memFactor = sum(X*y)/sum(X^2) = {sum_xy:.4e}/{sum_xx:.4e} = {mem_factor:.6e}")

    if large_samples:
        # 大表: y = (ioFactor+writeFactor+memFactor)*X1 + memFactor*X2
        # 若已有 memFactor，则 y - memFactor*X2 = (ioFactor+writeFactor+memFactor)*X1
        # 即 y - memFactor*(X1+X2) = (ioFactor+writeFactor)*X1
        # 所以 writeFactor = (y - memFactor*(X1+X2)) / X1 - ioFactor
        # 或用多元回归直接拟合
        X1 = np.array([s['build_rows_x_row_size'] for s in large_samples])
        X2 = np.array([s['probe_rows_x_row_size'] for s in large_samples])
        y = np.array([s['latency_hashjoin_ms'] - s['known_cpu'] for s in large_samples])

        # 多元回归: y = a*X1 + b*X2
        # 则 memFactor = b, ioFactor+writeFactor+memFactor = a => writeFactor = a - ioFactor - b
        X_mat = np.column_stack([X1, X2])
        coeffs = np.linalg.lstsq(X_mat, y, rcond=None)[0]
        coeff1, coeff2 = float(coeffs[0]), float(coeffs[1])

        mem_factor = max(0, coeff2)
        write_factor = coeff1 - IO_FACTOR - mem_factor
        if write_factor < 0:
            write_factor = 0.0

        console.print(f"\n[bold]大表 join 拟合 writeFactor, memFactor:[/bold]")
        console.print(f"  模型: y = coeff1*X1 + coeff2*X2, 其中 X1=buildRows*buildRowSize, X2=probeRows*probeRowSize, y=hashjoin-known_cpu")
        console.print(f"  多元回归 (X'X)^(-1) X'y 得 coeff1={coeff1:.6e}, coeff2={coeff2:.6e}")
        console.print(f"  memFactor = coeff2 = {mem_factor:.6e}")
        console.print(f"  writeFactor = coeff1 - ioFactor - memFactor = {coeff1:.6e} - {IO_FACTOR:.2e} - {mem_factor:.6e} = {write_factor:.6e}")

    # 若只有小表，writeFactor 保持 0
    if not large_samples:
        console.print(f"\n[dim]无大表 join 样本，writeFactor 保持 0[/dim]")

    # 预测与残差
    y_pred = []
    for s in samples:
        if s['is_small_join']:
            pred = s['known_cpu'] + mem_factor * s['mem_factor_term']
        else:
            pred = _io_term(s['build_rows'], s['build_row_size'], IO_FACTOR, write_factor) \
                   + s['known_cpu'] + mem_factor * s['mem_factor_term']
        y_pred.append(pred)

    y_pred = np.array(y_pred)
    y_actual = np.array([s['latency_hashjoin_ms'] for s in samples])
    mse = np.mean((y_actual - y_pred) ** 2)
    rmse = np.sqrt(mse)
    ss_res = np.sum((y_actual - y_pred) ** 2)
    ss_tot = np.sum((y_actual - np.mean(y_actual)) ** 2)
    r2 = 1 - ss_res / ss_tot if ss_tot > 1e-10 else 0

    # 输出结果
    console.print(Panel.fit(
        f"[bold]HashJoin 代价公式拟合结果[/bold]\n\n"
        f"已知: ioFactor = {IO_FACTOR:.2e}, cpuFactor = {CPU_FACTOR:.2e}\n\n"
        f"拟合: writeFactor = {write_factor:.6e}\n"
        f"      memFactor   = {mem_factor:.6e}\n\n"
        f"  RMSE = {rmse:.4f} ms\n"
        f"  R²   = {r2:.4f}\n"
        f"  有效样本数 = {len(samples)} (小表 {len(small_samples)}, 大表 {len(large_samples)})\n\n"
        f"[bold]公式:[/bold]\n"
        f"  IO:  buildRows * buildRowSize * (ioFactor + writeFactor)\n"
        f"  CPU: buildRows*buildFilters*cpuFactor + buildRows*nKeys*cpuFactor + buildRows*buildRowSize*memFactor + buildRows*cpuFactor\n"
        f"     + probeRows*probeFilters*cpuFactor + probeRows*nKeys*cpuFactor + probeRows*probeRowSize*memFactor + probeRows*cpuFactor",
        title="拟合系数",
        border_style="green",
    ))

    # 计算过程表格
    table = Table(title="预测 vs 实际 HashJoin 延时")
    table.add_column("Query", style="cyan")
    table.add_column("build", justify="right")
    table.add_column("probe", justify="right")
    table.add_column("实际 (ms)", justify="right")
    table.add_column("预测 (ms)", justify="right")
    table.add_column("误差 (%)", justify="right")
    table.add_column("小表", justify="center")

    for i, s in enumerate(samples):
        err_pct = 100 * (y_pred[i] - s['latency_hashjoin_ms']) / s['latency_hashjoin_ms'] if s['latency_hashjoin_ms'] != 0 else 0
        table.add_row(
            s['query_id'],
            f"{s['build_table']}({s['build_rows']:.0f})",
            f"{s['probe_table']}({s['probe_rows']:.0f})",
            f"{s['latency_hashjoin_ms']:.2f}",
            f"{y_pred[i]:.2f}",
            f"{err_pct:.1f}%",
            "Y" if s['is_small_join'] else "N",
        )
    console.print(table)

    # 保存
    result = {
        'formula': {
            'io': 'buildRows * buildRowSize * (ioFactor + writeFactor)',
            'cpu': 'buildRows*buildFilters*cpuFactor + buildRows*nKeys*cpuFactor + buildRows*buildRowSize*memFactor + buildRows*cpuFactor '
                  '+ probeRows*probeFilters*cpuFactor + probeRows*nKeys*cpuFactor + probeRows*probeRowSize*memFactor + probeRows*cpuFactor',
        },
        'coefficients': {
            'ioFactor': IO_FACTOR,
            'cpuFactor': CPU_FACTOR,
            'writeFactor': float(write_factor),
            'memFactor': float(mem_factor),
        },
        'small_join_note': '对于小表 join 仅计算 CPU 部分',
        'scan_formula': {
            'expr': f'{SCAN_IO_FACTOR:.2e}*(rows*rowSize) + {SCAN_CPU_FACTOR:.2e}*(rows*log2(rowSize)) + {SCAN_CONSTANT}',
        },
        'metrics': {'rmse_ms': float(rmse), 'r2': float(r2), 'n_samples': len(samples),
                   'n_small': len(small_samples), 'n_large': len(large_samples)},
        'samples': samples,
    }

    out_path = CONFIG['output_file']
    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump(result, f, indent=2, ensure_ascii=False)

    console.print(f"\n[bold green]系数已保存到: {out_path}[/bold green]")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="HashJoin 算子代价公式拟合")
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
