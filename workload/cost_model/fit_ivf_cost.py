#!/usr/bin/env python3
"""
IVF 索引算子代价公式拟合脚本

代价公式（如图所示）:
  IO:         nprobe × Avg_Bucket_Size × Cost_{seq_read}
  Cost_coarse: nlist × dim × CPU_{dist_factor}
  Cost_fine:   (nprobe × (N_total / nlist)) × dim × CPU_{dist_factor}

总代价:
  Cost = Cost_{seq_read} × (nprobe × Avg_Bucket_Size)
      + CPU_{dist_factor} × [nlist × dim + nprobe × (N_total / nlist) × dim]

给定参数: avg_bucket_size=16384, nlist=1024, dim=784

待拟合: Cost_{seq_read}, CPU_{dist_factor}

方法:
  1. 执行 IVF 向量检索 SQL，通过 SET ob_ivf_nprobes 改变 nprobe
  2. 测量总延时
  3. 多元线性回归拟合 Cost_{seq_read} 和 CPU_{dist_factor}

依赖: pip install mysql-connector-python numpy rich
用法: python fit_ivf_cost.py
      或修改 CONFIG 中的 db_name/db_host/db_port 后执行
"""

import re
import time
import json
import random
import argparse
import numpy as np
import mysql.connector
from typing import List, Tuple, Optional, Dict
from rich.console import Console
from rich.table import Table
from rich.panel import Panel

# --- 给定参数（如图所示），可通过命令行覆盖 ---
IVF_PARAMS = {
    'avg_bucket_size': 16384,
    'nlist': 1024,
    'dim': 784,
}

# --- 配置 ---
CONFIG = {
    'db_host': '127.0.0.1',
    'db_port': 10200,
    'db_user': 'root',
    'db_name': 'tpch10_3',
    'vector_file': '/data/dzh/seekdb/Exqutor/Vector-augmented_SQL_analytics/WIKI/queries.fbin',
    'vector_limit': 10,
    'result_limit': 10,
    'warmup_runs': 0,
    'measure_runs': 1,
    'shuffle_seed': 46,
    'output_file': 'ivf_cost_coefficients.json',
}

# 向量表定义: (table_name, vector_col, select_col)
# 需有 IVF 索引；若 tpch10 无 IVF，可改为其他库的 IVF 表
IVF_TABLES = [
    ("part", "text_embedding", "p_partkey"),
    ("part_vector", "text_embedding", "p_partkey"),
    ("partsupp_vector", "ps_text_embedding", "ps_partkey"),
]

# nprobe 取值列表（用于生成多样本）
NPROBE_VALUES = [4, 8, 16, 32, 64, 128]

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


def read_vectors(filename: str, limit: int) -> np.ndarray:
    """从 .fbin 文件读取向量"""
    with open(filename, "rb") as f:
        header = np.fromfile(f, count=2, dtype=np.int32)
        total_nvecs, dim = int(header[0]), int(header[1])
        read_count = min(limit, total_nvecs)
        data = np.fromfile(f, count=read_count * dim, dtype=np.float32)
        return data.reshape(-1, dim)


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

    # 加载查询向量
    try:
        vectors = read_vectors(CONFIG['vector_file'], CONFIG['vector_limit'])
    except Exception as e:
        console.print(f"[red]无法加载向量文件 {CONFIG['vector_file']}: {e}[/red]")
        cursor.close()
        conn.close()
        return

    samples: List[dict] = []
    table_row_count: Dict[str, int] = {}
    table_exists_cache: Dict[str, bool] = {}

    # 构建待执行任务列表并打乱顺序
    tasks: List[Tuple] = []
    for table_name, vector_col, select_col in IVF_TABLES:
        for nprobe in NPROBE_VALUES:
            for vec_idx in range(len(vectors)):
                tasks.append((table_name, vector_col, select_col, nprobe, vec_idx))
    rng = random.Random(CONFIG.get('shuffle_seed', 46))
    rng.shuffle(tasks)

    for table_name, vector_col, select_col, nprobe, vec_idx in tasks:
        # 检查表是否存在
        if table_name not in table_exists_cache:
            success, _, rows = safe_execute_query(cursor, f"SHOW TABLES LIKE '{table_name}'")
            table_exists_cache[table_name] = success and rows and len(rows) > 0
        if not table_exists_cache[table_name]:
            continue

        # 获取 N_total
        if table_name not in table_row_count:
            table_row_count[table_name] = get_table_row_count(cursor, table_name) or 0
        n_total = table_row_count[table_name]

        # 设置 nprobe
        try:
            safe_execute_query(cursor, f"SET SESSION ob_ivf_nprobes = {nprobe}")
        except Exception:
            pass

        vec = vectors[vec_idx]
        vec_str = "[" + ",".join(map(str, vec.astype(float))) + "]"
        sql = f"SELECT {select_col} FROM {table_name} ORDER BY l2_distance({vector_col}, '{vec_str}') APPROXIMATE LIMIT {CONFIG['result_limit']}"

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
                console.print(f"[red]{table_name} nprobe={nprobe} vec={vec_idx} 失败: {err}[/red]")
                break

        if not latencies:
            continue

        latency = np.median(latencies)

        # 特征: X1 = nprobe × Avg_Bucket_Size (IO)
        #       X2 = nlist × dim + nprobe × (N_total / nlist) × dim (CPU)
        ab = IVF_PARAMS['avg_bucket_size']
        nl = IVF_PARAMS['nlist']
        dm = IVF_PARAMS['dim']
        x1 = nprobe * ab
        x2 = nl * dm + nprobe * (n_total / nl) * dm

        samples.append({
            'query_id': f"{table_name}_nprobe{nprobe}_v{vec_idx}",
            'table': table_name,
            'nprobe': nprobe,
            'n_total': n_total,
            'latency_ms': latency,
            'x1_io': x1,
            'x2_cpu': x2,
        })

        console.print(
            f"  [green]{table_name}[/green] nprobe={nprobe}, N_total={n_total}, "
            f"latency={latency:.2f} ms, X1={x1:.0f}, X2={x2:.0f}"
        )

    cursor.close()
    conn.close()

    if len(samples) < 3:
        console.print("[red]有效样本不足，无法拟合[/red]")
        return

    # 多元回归: latency = Cost_{seq_read} × X1 + CPU_{dist_factor} × X2
    X = np.column_stack([
        np.array([s['x1_io'] for s in samples]),
        np.array([s['x2_cpu'] for s in samples]),
    ])
    y = np.array([s['latency_ms'] for s in samples])

    coeffs = np.linalg.lstsq(X, y, rcond=None)[0]
    cost_seq_read = float(coeffs[0])
    cpu_dist_factor = float(coeffs[1])
    if cost_seq_read < 0:
        cost_seq_read = 0.0
    if cpu_dist_factor < 0:
        cpu_dist_factor = 0.0

    # 预测与残差
    y_pred = X @ coeffs
    mse = np.mean((y - y_pred) ** 2)
    rmse = np.sqrt(mse)
    ss_res = np.sum((y - y_pred) ** 2)
    ss_tot = np.sum((y - np.mean(y)) ** 2)
    r2 = 1 - ss_res / ss_tot if ss_tot > 1e-10 else 0

    # 输出结果
    console.print(Panel.fit(
        f"[bold]IVF 代价公式拟合结果[/bold]\n\n"
        f"公式:\n"
        f"  IO:         nprobe × Avg_Bucket_Size × Cost_{{seq_read}}\n"
        f"  Cost_coarse: nlist × dim × CPU_{{dist_factor}}\n"
        f"  Cost_fine:   (nprobe × (N_total / nlist)) × dim × CPU_{{dist_factor}}\n\n"
        f"给定: avg_bucket_size={IVF_PARAMS['avg_bucket_size']}, nlist={IVF_PARAMS['nlist']}, dim={IVF_PARAMS['dim']}\n\n"
        f"拟合:\n"
        f"  Cost_{{seq_read}}   = {cost_seq_read:.6e}\n"
        f"  CPU_{{dist_factor}} = {cpu_dist_factor:.6e}\n\n"
        f"  RMSE = {rmse:.4f} ms\n"
        f"  R²   = {r2:.4f}\n"
        f"  有效样本数 = {len(samples)}",
        title="拟合系数",
        border_style="green",
    ))

    # 计算过程表格
    table = Table(title="预测 vs 实际 IVF 延时")
    table.add_column("Query", style="cyan")
    table.add_column("nprobe", justify="right")
    table.add_column("N_total", justify="right")
    table.add_column("X1 (IO)", justify="right")
    table.add_column("X2 (CPU)", justify="right")
    table.add_column("实际 (ms)", justify="right")
    table.add_column("预测 (ms)", justify="right")
    table.add_column("误差 (%)", justify="right")

    for i, s in enumerate(samples):
        pred = y_pred[i]
        err_pct = 100 * (pred - s['latency_ms']) / s['latency_ms'] if s['latency_ms'] != 0 else 0
        table.add_row(
            s['query_id'],
            str(s['nprobe']),
            f"{s['n_total']:.0f}",
            f"{s['x1_io']:.0f}",
            f"{s['x2_cpu']:.0f}",
            f"{s['latency_ms']:.2f}",
            f"{pred:.2f}",
            f"{err_pct:.1f}%",
        )
    console.print(table)

    # 回归计算过程
    console.print(Panel.fit(
        f"[bold]回归计算过程[/bold]\n\n"
        f"模型: latency = Cost_{{seq_read}} × X1 + CPU_{{dist_factor}} × X2\n"
        f"  X1 = nprobe × {IVF_PARAMS['avg_bucket_size']}\n"
        f"  X2 = {IVF_PARAMS['nlist']} × {IVF_PARAMS['dim']} + nprobe × (N_total / {IVF_PARAMS['nlist']}) × {IVF_PARAMS['dim']}\n\n"
        f"多元回归 (X'X)^(-1) X'y 得:\n"
        f"  Cost_{{seq_read}}   = {cost_seq_read:.6e}\n"
        f"  CPU_{{dist_factor}} = {cpu_dist_factor:.6e}",
        title="回归推导",
        border_style="blue",
    ))

    # 保存
    result = {
        'formula': {
            'io': f"nprobe × {IVF_PARAMS['avg_bucket_size']} × Cost_seq_read",
            'cost_coarse': f"{IVF_PARAMS['nlist']} × {IVF_PARAMS['dim']} × CPU_dist_factor",
            'cost_fine': f"(nprobe × (N_total / {IVF_PARAMS['nlist']})) × {IVF_PARAMS['dim']} × CPU_dist_factor",
        },
        'given_params': dict(IVF_PARAMS),
        'coefficients': {
            'Cost_seq_read': float(cost_seq_read),
            'CPU_dist_factor': float(cpu_dist_factor),
        },
        'metrics': {'rmse_ms': float(rmse), 'r2': float(r2), 'n_samples': len(samples)},
        'samples': samples,
    }

    out_path = CONFIG['output_file']
    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump(result, f, indent=2, ensure_ascii=False)

    console.print(f"\n[bold green]系数已保存到: {out_path}[/bold green]")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="IVF 索引算子代价公式拟合")
    parser.add_argument("--db-name", default=None, help="数据库名")
    parser.add_argument("--db-host", default=None, help="数据库主机")
    parser.add_argument("--db-port", type=int, default=None, help="数据库端口")
    parser.add_argument("--vector-file", default=None, help="向量文件路径 (.fbin)")
    parser.add_argument("--vector-limit", type=int, default=None, help="使用的向量数量")
    parser.add_argument("--output", "-o", default=None, help="输出 JSON 文件路径")
    parser.add_argument("--runs", type=int, default=1, help="每条查询测量次数")
    parser.add_argument("--avg-bucket-size", type=int, default=None, help="Avg_Bucket_Size，默认 16384")
    parser.add_argument("--nlist", type=int, default=None, help="nlist，默认 1024")
    parser.add_argument("--dim", type=int, default=None, help="向量维度 dim，默认 784")
    args = parser.parse_args()

    if args.avg_bucket_size is not None:
        IVF_PARAMS['avg_bucket_size'] = args.avg_bucket_size
    if args.nlist is not None:
        IVF_PARAMS['nlist'] = args.nlist
    if args.dim is not None:
        IVF_PARAMS['dim'] = args.dim
    if args.db_name is not None:
        CONFIG["db_name"] = args.db_name
    if args.db_host is not None:
        CONFIG["db_host"] = args.db_host
    if args.db_port is not None:
        CONFIG["db_port"] = args.db_port
    if args.vector_file is not None:
        CONFIG["vector_file"] = args.vector_file
    if args.vector_limit is not None:
        CONFIG["vector_limit"] = args.vector_limit
    if args.output is not None:
        CONFIG["output_file"] = args.output
    if args.runs is not None:
        CONFIG["measure_runs"] = args.runs

    run_calibration()
