#!/usr/bin/env python3
"""
HNSW 索引算子代价公式拟合脚本

代价公式（如图所示）:
  Cost_layers = Σ(efSearch × dim × CPU_dist_factor)  for i=1 to L
  Cost_bottom = ef_search × d̄ × dim × CPU_dist_factor

其中:
  L: HNSW 层数（不含底层）
  efSearch: 各层搜索的 exploration factor
  ef_search: 底层搜索的 ef
  dim: 向量维度
  d̄: 底层平均度数（通常为 HNSW 的 m 参数）
  CPU_dist_factor: 待拟合系数

当 efSearch 各层相同时: Cost_layers = L × ef_search × dim × CPU_dist_factor
总代价: Cost = ef_search × dim × (L + d̄) × CPU_dist_factor

方法:
  1. 从数据库查询 HNSW 索引参数（dim, m, L 等），或使用默认/估计值
  2. 执行纯向量检索 SQL，测量总延时
  3. 纯向量检索延时 ≈ HNSW 代价（无表扫描扣减）
  4. 线性回归拟合 CPU_dist_factor

依赖: pip install mysql-connector-python numpy rich
用法: python fit_hnsw_cost.py
      或修改 CONFIG 中的 db_name/db_host/db_port 后执行
"""

import re
import time
import json
import math
import random
import argparse
import numpy as np
import mysql.connector
from typing import List, Tuple, Optional, Dict
from rich.console import Console
from rich.table import Table
from rich.panel import Panel

# --- 配置 ---
CONFIG = {
    'db_host': '127.0.0.1',
    'db_port': 10200,
    'db_user': 'root',
    'db_name': 'tpch10',
    'vector_file': '/data/dzh/seekdb/Exqutor/Vector-augmented_SQL_analytics/WIKI/queries.fbin',
    'vector_limit': 10,
    'result_limit': 10,
    'warmup_runs': 0,
    'measure_runs': 1,
    'shuffle_seed': 45,
    'output_file': 'hnsw_cost_coefficients.json',
}

# 默认 HNSW 参数（当无法从 DB 查询时使用）
DEFAULT_M = 16
DEFAULT_EF_SEARCH = 64

# 向量表定义: (table_name, vector_col, select_col, dim)
# tpch10 常见: part/part_vector text_embedding 768, partsupp_vector ps_text_embedding 768
VECTOR_TABLES = [
    ("part", "text_embedding", "p_partkey", 768),
    ("partsupp", "ps_text_embedding", "ps_partkey", 768)
]

# ef_search 取值列表（用于生成多样本）
EF_SEARCH_VALUES = [32, 64, 128, 256]

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


def get_vector_dim_from_db(cursor, table: str, col: str) -> Optional[int]:
    """尝试从 information_schema 或 SHOW CREATE TABLE 获取向量维度"""
    try:
        # 尝试 information_schema
        sql = f"""
            SELECT COLUMN_TYPE FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = '{table}' AND COLUMN_NAME = '{col}'
        """
        success, err, rows = safe_execute_query(cursor, sql)
        if success and rows and rows[0][0]:
            s = str(rows[0][0]).lower()
            m = re.search(r'vector\s*\(\s*(\d+)\s*\)', s)
            if m:
                return int(m.group(1))
    except Exception:
        pass
    return None


def get_hnsw_index_params(cursor, table: str) -> Dict:
    """
    尝试从数据库查询 HNSW 索引参数 (m, ef_search 等)。
    返回 dict，未查到的项为 None。
    """
    result = {"m": None, "ef_search": None}
    try:
        # 尝试获取 ob_hnsw_ef_search 当前值
        success, err, rows = safe_execute_query(cursor, "SHOW VARIABLES LIKE 'ob_hnsw_ef_search'")
        if success and rows:
            result["ef_search"] = int(rows[0][1]) if rows[0][1].isdigit() else None

        # m 等参数可能在 index params 中，OceanBase 可能无直接 SQL 接口，使用默认
    except Exception:
        pass
    return result


def estimate_hnsw_layers(n_vectors: int) -> int:
    """估计 HNSW 层数 L，通常 L ≈ log(n)"""
    if n_vectors <= 0:
        return 1
    return max(1, int(math.ceil(math.log2(n_vectors))))


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

    # 构建待执行任务列表并打乱顺序，减少缓存影响
    tasks: List[Tuple] = []
    for table_name, vector_col, select_col, default_dim in VECTOR_TABLES:
        for ef_search in EF_SEARCH_VALUES:
            for vec_idx in range(len(vectors)):
                tasks.append((table_name, vector_col, select_col, default_dim, ef_search, vec_idx))
    rng = random.Random(CONFIG.get('shuffle_seed', 45))
    rng.shuffle(tasks)

    for table_name, vector_col, select_col, default_dim, ef_search, vec_idx in tasks:
        # 检查表是否存在
        if table_name not in table_exists_cache:
            success, _, rows = safe_execute_query(cursor, f"SHOW TABLES LIKE '{table_name}'")
            table_exists_cache[table_name] = success and rows and len(rows) > 0
        if not table_exists_cache[table_name]:
            console.print(f"[dim]跳过 {table_name}: 表不存在[/dim]")
            continue

        # 获取 dim
        dim = get_vector_dim_from_db(cursor, table_name, vector_col) or default_dim

        # 获取行数并估计 L
        if table_name not in table_row_count:
            table_row_count[table_name] = get_table_row_count(cursor, table_name) or 0
        n_vectors = table_row_count[table_name]
        L = estimate_hnsw_layers(n_vectors)

        # 获取 m (d̄)
        params = get_hnsw_index_params(cursor, table_name)
        d_bar = params.get("m") or DEFAULT_M

        # 设置 ef_search
        try:
            safe_execute_query(cursor, f"SET SESSION ob_hnsw_ef_search = {ef_search}")
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
                console.print(f"[red]{table_name} ef={ef_search} vec={vec_idx} 失败: {err}[/red]")
                break

        if not latencies:
            continue

        latency = np.median(latencies)

        # 代价公式: Cost = ef_search × dim × (L + d̄) × CPU_dist_factor
        # 即 latency = X * CPU_dist_factor, 其中 X = ef_search * dim * (L + d_bar)
        cost_factor_term = ef_search * dim * (L + d_bar)

        samples.append({
            'query_id': f"{table_name}_ef{ef_search}_v{vec_idx}",
            'table': table_name,
            'ef_search': ef_search,
            'dim': dim,
            'L': L,
            'd_bar': d_bar,
            'n_vectors': n_vectors,
            'latency_ms': latency,
            'cost_factor_term': cost_factor_term,
        })

        console.print(
            f"  [green]{table_name}[/green] ef_search={ef_search}, dim={dim}, L={L}, d̄={d_bar}: "
            f"latency={latency:.2f} ms, X=ef*dim*(L+d̄)={cost_factor_term:.0f}"
        )

    cursor.close()
    conn.close()

    if len(samples) < 2:
        console.print("[red]有效样本不足，无法拟合[/red]")
        return

    # 回归: latency = CPU_dist_factor * cost_factor_term
    # 即 y = CPU_dist_factor * X, 无截距
    X = np.array([s['cost_factor_term'] for s in samples])
    y = np.array([s['latency_ms'] for s in samples])
    X = X.reshape(-1, 1)
    cpu_dist_factor = float(np.linalg.lstsq(X, y, rcond=None)[0][0])
    if cpu_dist_factor < 0:
        cpu_dist_factor = 0.0

    # 预测与残差
    y_pred = cpu_dist_factor * X.flatten()
    mse = np.mean((y - y_pred) ** 2)
    rmse = np.sqrt(mse)
    ss_res = np.sum((y - y_pred) ** 2)
    ss_tot = np.sum((y - np.mean(y)) ** 2)
    r2 = 1 - ss_res / ss_tot if ss_tot > 1e-10 else 0

    # 输出结果
    console.print(Panel.fit(
        f"[bold]HNSW 代价公式拟合结果[/bold]\n\n"
        f"公式:\n"
        f"  Cost_layers = Σ(efSearch × dim × CPU_dist_factor)  for i=1 to L\n"
        f"  Cost_bottom = ef_search × d̄ × dim × CPU_dist_factor\n\n"
        f"当 efSearch 各层相同 (= ef_search) 时:\n"
        f"  Cost = ef_search × dim × (L + d̄) × CPU_dist_factor\n\n"
        f"  CPU_dist_factor = {cpu_dist_factor:.6e}\n\n"
        f"  RMSE = {rmse:.4f} ms\n"
        f"  R²   = {r2:.4f}\n"
        f"  有效样本数 = {len(samples)}",
        title="拟合系数",
        border_style="green",
    ))

    # 计算过程表格
    table = Table(title="预测 vs 实际 HNSW 延时")
    table.add_column("Query", style="cyan")
    table.add_column("ef_search", justify="right")
    table.add_column("dim", justify="right")
    table.add_column("L", justify="right")
    table.add_column("d̄", justify="right")
    table.add_column("X=ef*dim*(L+d̄)", justify="right")
    table.add_column("实际 (ms)", justify="right")
    table.add_column("预测 (ms)", justify="right")
    table.add_column("误差 (%)", justify="right")

    for i, s in enumerate(samples):
        pred = y_pred[i]
        err_pct = 100 * (pred - s['latency_ms']) / s['latency_ms'] if s['latency_ms'] != 0 else 0
        table.add_row(
            s['query_id'],
            str(s['ef_search']),
            str(s['dim']),
            str(s['L']),
            str(s['d_bar']),
            f"{s['cost_factor_term']:.0f}",
            f"{s['latency_ms']:.2f}",
            f"{pred:.2f}",
            f"{err_pct:.1f}%",
        )
    console.print(table)

    # 回归计算过程
    sum_xy = float(np.sum(X.flatten() * y))
    sum_xx = float(np.sum(X.flatten() ** 2))
    console.print(Panel.fit(
        f"[bold]回归计算过程[/bold]\n\n"
        f"模型: latency = CPU_dist_factor × (ef_search × dim × (L + d̄))\n"
        f"即 y = CPU_dist_factor × X\n\n"
        f"  sum(X × y) = {sum_xy:.4e}\n"
        f"  sum(X²)   = {sum_xx:.4e}\n"
        f"  CPU_dist_factor = sum(X×y)/sum(X²) = {sum_xy:.4e}/{sum_xx:.4e} = {cpu_dist_factor:.6e}",
        title="回归推导",
        border_style="blue",
    ))

    # 保存
    result = {
        'formula': {
            'cost_layers': 'Σ(efSearch × dim × CPU_dist_factor) for i=1 to L',
            'cost_bottom': 'ef_search × d̄ × dim × CPU_dist_factor',
            'simplified': 'Cost = ef_search × dim × (L + d̄) × CPU_dist_factor when efSearch same for all layers',
        },
        'coefficients': {'CPU_dist_factor': float(cpu_dist_factor)},
        'params_note': {
            'L': 'estimated as ceil(log2(n_vectors))',
            'd_bar': 'average degree, typically m (default 16)',
        },
        'metrics': {'rmse_ms': float(rmse), 'r2': float(r2), 'n_samples': len(samples)},
        'samples': samples,
    }

    out_path = CONFIG['output_file']
    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump(result, f, indent=2, ensure_ascii=False)

    console.print(f"\n[bold green]系数已保存到: {out_path}[/bold green]")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="HNSW 索引算子代价公式拟合")
    parser.add_argument("--db-name", default=None, help="数据库名")
    parser.add_argument("--db-host", default=None, help="数据库主机")
    parser.add_argument("--db-port", type=int, default=None, help="数据库端口")
    parser.add_argument("--vector-file", default=None, help="向量文件路径 (.fbin)")
    parser.add_argument("--vector-limit", type=int, default=None, help="使用的向量数量")
    parser.add_argument("--output", "-o", default=None, help="输出 JSON 文件路径")
    parser.add_argument("--runs", type=int, default=1, help="每条查询测量次数")
    args = parser.parse_args()

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
