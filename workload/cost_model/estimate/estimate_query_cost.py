#!/usr/bin/env python3
"""
根据存储配置估算 TPC-H 查询的 table_scan 和 hash_join 执行代价。

支持:
  - 分区裁剪: filter 命中分区键时，按分区范围计算扫描 rows
  - 行存/列存: 行存用 Scan 公式，列存用 Scan列存 公式，列存需根据 SQL 涉及列计算 rowSize
  - IVF 索引: filter 含 vector_filter 或 l2_distance 时用 IVF 代价公式
  - 向量存储: together / separated / both；separated 时对有向量扫描的 SQL 加向量-标量 hash join 代价

代价公式见 operator_cost.md

用法:
  python estimate_query_cost.py --queries tpch_queries.json --config storage_config.example.json
  python estimate_query_cost.py --queries tpch_queries.json --config storage_config.json -o cost_report.json
"""

import re
import json
import math
import argparse
from typing import Dict, List, Any, Optional, Tuple
from pathlib import Path
from datetime import datetime, timedelta


# --- 代价公式系数 (operator_cost.md) ---
SCAN_IO_FACTOR = 4.38e-5
SCAN_CPU_FACTOR = 1.51e-4
SCAN_CONSTANT = 315.2

SCAN_CG_IO_FACTOR = -5.23e-6
SCAN_CG_CPU_FACTOR = 6.57e-4
SCAN_CG_CONSTANT = -6609.80

IVF_CPU_FACTOR = 1e-3  # Cost_coarse, Cost_fine 的系数

HASHJOIN_IO_FACTOR = 4.38e-5
HASHJOIN_CPU_FACTOR = 1.73e-3
HASHJOIN_MEM_FACTOR = 1.05e-4


def is_ivf_scan(filter_str: Optional[str], table: str) -> bool:
    """判断是否为 IVF 索引扫描: filter 含 vector_filter 或 l2_distance 且涉及向量列"""
    if not filter_str:
        return False
    f = (filter_str or "").lower()
    return "vector_filter" in f or ("l2_distance" in f and "embedding" in f)


def extract_columns_from_filter(filter_str: Optional[str]) -> List[str]:
    """从 filter 字符串提取列名，如 customer.C_MKTSEGMENT -> c_mktsegment"""
    if not filter_str:
        return []
    cols = []
    for m in re.finditer(r"(\w+)\.(\w+)", filter_str):
        table_prefix, col = m.group(1), m.group(2)
        cols.append(col.lower())
    return list(dict.fromkeys(cols))


def extract_columns_from_equal_conds(equal_conds: List[str], table: str) -> List[str]:
    """从 equal_conds 提取指定表的列名"""
    cols = []
    for cond in equal_conds or []:
        for m in re.finditer(rf"{re.escape(table)}\.(\w+)", cond, re.I):
            cols.append(m.group(1).lower())
        for m in re.finditer(r"(\w+)\s*=\s*" + re.escape(table) + r"\.(\w+)", cond, re.I):
            cols.append(m.group(2).lower())
    return list(dict.fromkeys(cols))


def compute_column_row_size(
    table: str,
    columns: List[str],
    table_config: Dict,
    default_full: bool = True,
) -> float:
    """根据列名计算列存 scan 的 rowSize (bytes)"""
    col_sizes = table_config.get("column_sizes", {})
    if not col_sizes:
        return float(table_config.get("row_size", 100))
    total = 0
    for c in columns:
        total += col_sizes.get(c, col_sizes.get(c.upper(), 4))
    if total <= 0 and default_full:
        return float(table_config.get("row_size", 100))
    return max(1, total)


def _add_months(base_date, delta_months: int):
    """按自然月加减，超出月末时截断到该月最后一天。"""
    month_index = (base_date.month - 1) + delta_months
    year = base_date.year + month_index // 12
    month = month_index % 12 + 1
    if month == 12:
        next_month_first = datetime(year + 1, 1, 1).date()
    else:
        next_month_first = datetime(year, month + 1, 1).date()
    month_last_day = (next_month_first - timedelta(days=1)).day
    day = min(base_date.day, month_last_day)
    return datetime(year, month, day).date()


def normalize_date_functions(filter_str: Optional[str]) -> Optional[str]:
    """
    将 filter 中常见 date_add/date_sub 表达式归一化为日期字面量，便于分区裁剪提取边界。
    支持形态:
      - date_sub('1994-12-01', cast(90, VARCHAR(1048576)), 4)
      - date_add('1993-07-01', 3, 6)
      - date_add('1994-01-01', '1', 8)
    unit code:
      - 4: day
      - 6: month
      - 8: year
    """
    if not filter_str:
        return filter_str

    unit_to_kind = {4: "day", 6: "month", 8: "year"}
    func_pat = re.compile(
        r"date_(add|sub)\(\s*'(\d{4}-\d{2}-\d{2})'\s*,\s*(.*?)\s*,\s*(\d+)\s*\)",
        re.I,
    )

    def _repl(m: re.Match) -> str:
        op = m.group(1).lower()
        date_text = m.group(2)
        interval_expr = m.group(3)
        unit_code = int(m.group(4))
        kind = unit_to_kind.get(unit_code)
        if kind is None:
            return m.group(0)
        interval_num_m = re.search(r"-?\d+", interval_expr or "")
        if not interval_num_m:
            return m.group(0)
        delta = int(interval_num_m.group(0))
        if op == "sub":
            delta = -delta

        try:
            base_date = datetime.strptime(date_text, "%Y-%m-%d").date()
            if kind == "day":
                out = base_date + timedelta(days=delta)
            elif kind == "month":
                out = _add_months(base_date, delta)
            else:  # year
                out = _add_months(base_date, delta * 12)
            return f"'{out.isoformat()}'"
        except Exception:
            return m.group(0)

    return func_pat.sub(_repl, filter_str)


def partition_pruning(
    filter_str: Optional[str],
    partition_key: str,
    partitions: List[Dict],
) -> Optional[int]:
    """
    分区裁剪: 若 filter 命中分区键的范围条件，返回需扫描分区的总基数。
    支持: col >= 'x', col < 'x', col > 'x', col <= 'x'
    返回 None 表示无法裁剪，用 est_rows。
    """
    if not filter_str or not partition_key or not partitions:
        return None

    normalized_filter = normalize_date_functions(filter_str)
    key_col = partition_key.split(".")[-1].lower()
    filter_lower = (normalized_filter or "").lower()
    if key_col not in filter_lower:
        return None

    lower_bound = None
    upper_bound = None
    # 匹配 'YYYY-MM-DD' 或 数字
    val_pat = r"['\"]?([\d\-][\d\-\.]*)['\"]?"
    for m in re.finditer(rf"([<>]=?)\s*{val_pat}", normalized_filter or ""):
        op, val = m.group(1), m.group(2)
        if "<" in op:
            upper_bound = val
        if ">" in op:
            lower_bound = val
    for m in re.finditer(rf"{val_pat}\s*([<>]=?)", normalized_filter or ""):
        val, op = m.group(1), m.group(2)
        if "<" in op:
            upper_bound = val
        if ">" in op:
            lower_bound = val

    if lower_bound is None and upper_bound is None:
        return None

    total = 0
    for p in partitions:
        r = p.get("range", "")
        mm = re.match(r"\[([^,]+),\s*([^)\]]+)\)", r)
        if not mm:
            continue
        p_lo, p_hi = mm.group(1).strip(), mm.group(2).strip()
        if lower_bound and p_hi <= lower_bound:
            continue
        if upper_bound and p_lo >= upper_bound:
            continue
        total += p.get("cardinality", 0)
    return total if total > 0 else None


def scan_cost_row(rows: float, row_size: float) -> float:
    """行存 Scan 代价 (ms)"""
    rs = max(1, row_size)
    return SCAN_IO_FACTOR * (rows * row_size) + SCAN_CPU_FACTOR * (rows * math.log2(rs)) + SCAN_CONSTANT


def scan_cost_column(rows: float, row_size: float) -> float:
    """列存 Scan 代价 (ms)"""
    rs = max(1, row_size)
    return SCAN_CG_IO_FACTOR * (rows * row_size) + SCAN_CG_CPU_FACTOR * (rows * math.log2(rs)) + SCAN_CG_CONSTANT


def ivf_cost(nlist: int, nprobe: int, dim: int, n_total: float) -> float:
    """IVF 索引代价 (ms): Cost_coarse + Cost_fine"""
    cost_coarse = nlist * dim * IVF_CPU_FACTOR
    cost_fine = (nprobe * (n_total / max(1, nlist))) * dim * IVF_CPU_FACTOR
    return cost_coarse + cost_fine


def hashjoin_cost(
    build_rows: float,
    build_row_size: float,
    probe_rows: float,
    probe_row_size: float,
    n_keys: int = 1,
    build_filters: int = 0,
    probe_filters: int = 0,
) -> float:
    """HashJoin 代价 (ms)"""
    return (
        build_rows * build_row_size * HASHJOIN_IO_FACTOR
        + build_rows * build_filters * HASHJOIN_CPU_FACTOR
        + build_rows * n_keys * HASHJOIN_CPU_FACTOR
        + build_rows * build_row_size * HASHJOIN_MEM_FACTOR
        + build_rows * HASHJOIN_CPU_FACTOR
        + probe_rows * probe_filters * HASHJOIN_CPU_FACTOR
        + probe_rows * n_keys * HASHJOIN_CPU_FACTOR
        + probe_rows * probe_row_size * HASHJOIN_MEM_FACTOR
        + probe_rows * HASHJOIN_CPU_FACTOR
    )


def estimate_table_scan(
    scan: Dict,
    table_config: Dict,
    query_data: Dict,
    storage_config: Dict,
) -> Dict[str, Any]:
    """
    估算单个 table_scan 的代价。
    返回: { cost_ms, rows, row_size, scan_type, partition_pruned, ... }
    """
    table = scan.get("table", "")
    filter_str = scan.get("filter")
    est_rows = scan.get("est_rows") or 0

    if table not in table_config:
        return {"cost_ms": 0, "rows": est_rows, "error": f"表 {table} 未在存储配置中"}

    cfg = table_config[table]
    storage_type = cfg.get("storage_type", "row")
    partition_key = cfg.get("partition_key")
    partitions = cfg.get("partitions", [])

    # 分区裁剪
    rows = est_rows
    partition_pruned = False
    pruned = partition_pruning(filter_str, partition_key, partitions)
    print(f"partition_pruning: {pruned}")
    if pruned is not None:
        rows = pruned
        partition_pruned = True

    # 列存: 根据 filter 和 hash_join 涉及的列计算 rowSize
    columns = extract_columns_from_filter(filter_str)
    for hj in query_data.get("hash_joins", []):
        if table in (hj.get("tables") or []):
            columns.extend(extract_columns_from_equal_conds(hj.get("equal_conds", []), table))
    if storage_type == "column" and columns:
        row_size = compute_column_row_size(table, columns, cfg, default_full=True)
    else:
        row_size = float(cfg.get("row_size", 100))

    # IVF 扫描
    if is_ivf_scan(filter_str, table):
        ivf = storage_config.get("ivf_params", {})
        nlist = ivf.get("nlist", 1024)
        nprobe = ivf.get("nprobe", 32)
        dim = ivf.get("dim", cfg.get("vector_dim", 768))
        n_total = cfg.get("partitions", [{}])[0].get("cardinality", 8000000) if partitions else est_rows
        if partitions:
            n_total = sum(p.get("cardinality", 0) for p in partitions)
        cost_ms = ivf_cost(nlist, nprobe, dim, float(n_total))
        return {
            "cost_ms": cost_ms,
            "rows": rows,
            "row_size": row_size,
            "scan_type": "ivf",
            "partition_pruned": partition_pruned,
            "table": table,
        }

    # 行存 / 列存
    if storage_type == "column":
        cost_ms = scan_cost_column(rows, row_size)
    else:
        cost_ms = scan_cost_row(rows, row_size)

    print(f"rows: {rows}, row_size: {row_size}, cost_ms: {cost_ms}")
    return {
        "cost_ms": max(0, cost_ms),
        "rows": rows,
        "row_size": row_size,
        "scan_type": "column" if storage_type == "column" else "row",
        "partition_pruned": partition_pruned,
        "table": table,
    }


def estimate_hash_join(
    hj: Dict,
    table_config: Dict,
    scan_rows: Dict[str, float],
    storage_config: Dict,
) -> Dict[str, Any]:
    """
    估算单个 hash_join 的代价。
    scan_rows: table -> rows 的映射
    """
    tables = hj.get("tables") or []
    equal_conds = hj.get("equal_conds") or []
    n_keys = len(equal_conds)
    build_filters = len(hj.get("other_conds") or [])
    probe_filters = 0

    if len(tables) != 2:
        return {"cost_ms": 0, "error": f"hash_join 需要恰好 2 张表, 得到 {len(tables)}"}

    t1, t2 = tables[0], tables[1]
    r1 = scan_rows.get(t1, 0) or 1
    r2 = scan_rows.get(t2, 0) or 1

    row_size1 = float(table_config.get(t1, {}).get("row_size", 100))
    row_size2 = float(table_config.get(t2, {}).get("row_size", 100))

    build_rows = min(r1, r2)
    probe_rows = max(r1, r2)
    if build_rows == r1:
        build_row_size, probe_row_size = row_size1, row_size2
    else:
        build_row_size, probe_row_size = row_size2, row_size1

    cost_ms = hashjoin_cost(
        build_rows, build_row_size, probe_rows, probe_row_size,
        n_keys=n_keys, build_filters=build_filters, probe_filters=probe_filters,
    )
    return {
        "cost_ms": max(0, cost_ms),
        "build_table": t1 if build_rows == r1 else t2,
        "probe_table": t2 if build_rows == r1 else t1,
        "build_rows": build_rows,
        "probe_rows": probe_rows,
        "n_keys": n_keys,
    }


def estimate_vector_merge_cost(
    scan_rows: Dict[str, float],
    table_config: Dict,
    vector_storage: str,
    vector_tables: List[str],
) -> float:
    """
    向量分离存储时，向量与标量 merge 的 hash join 代价。
    vector_tables: 本 query 中涉及向量扫描的表 (如 partsupp)
    vector_storage: "together" 无额外代价; "separated" 需加 merge 代价; "both" 使用 together 副本，无额外代价
    """
    if vector_storage not in ("separated",):
        return 0.0
    cost = 0.0
    for t in vector_tables:
        if t not in table_config:
            continue
        cfg = table_config[t]
        vec_col = cfg.get("vector_column")
        if not vec_col:
            continue
        rows = scan_rows.get(t, 0)
        if rows <= 0:
            continue
        # 向量行大小 (仅向量列) vs 标量行大小 (不含向量)
        vec_size = cfg.get("column_sizes", {}).get(vec_col, 3072)
        scalar_size = cfg.get("row_size", 3291) - vec_size
        scalar_size = max(1, scalar_size)
        # build=标量, probe=向量 或反过来，取较小为 build
        cost += hashjoin_cost(rows, scalar_size, rows, vec_size, n_keys=2, build_filters=0, probe_filters=0)
    return cost


def run_estimation(
    queries: Dict,
    storage_config: Dict,
) -> Dict[str, Any]:
    """对每条 query 估算 table_scan 和 hash_join 代价"""
    table_config = storage_config.get("tables", {})
    vector_storage = storage_config.get("vector_storage", "together")
    results = {}

    for qid, qdata in queries.items():
        print(f"qid: {qid}")
        if "error" in qdata:
            results[qid] = {"error": qdata["error"]}
            continue

        scan_rows = {}  # table -> rows
        scan_costs = []
        hj_costs = []
        vector_scan_tables = []

        for scan in qdata.get("table_scans", []):
            print(f"scan: {scan}")
            est = estimate_table_scan(scan, table_config, qdata, storage_config)
            scan_costs.append(est)
            t = scan.get("table", "")
            r = est.get("rows", scan.get("est_rows", 0))
            if t in scan_rows:
                scan_rows[t] = min(scan_rows[t], r)
            else:
                scan_rows[t] = r
            if est.get("scan_type") == "ivf":
                vector_scan_tables.append(t)

        for hj in qdata.get("hash_joins", []):
            est = estimate_hash_join(hj, table_config, scan_rows, storage_config)
            hj_costs.append(est)

        vector_merge_cost = estimate_vector_merge_cost(
            scan_rows, table_config, vector_storage, vector_scan_tables
        )

        total_scan = sum(s.get("cost_ms", 0) for s in scan_costs)
        total_hj = sum(h.get("cost_ms", 0) for h in hj_costs)
        total = total_scan + total_hj + vector_merge_cost

        results[qid] = {
            "table_scans": scan_costs,
            "hash_joins": hj_costs,
            "vector_merge_cost_ms": vector_merge_cost,
            "total_scan_cost_ms": total_scan,
            "total_hashjoin_cost_ms": total_hj,
            "total_cost_ms": total,
        }

    return results


def main():
    parser = argparse.ArgumentParser(description="估算 TPC-H 查询的 table_scan 和 hash_join 代价")
    parser.add_argument("--queries", "-q", default="tpch_queries.json", help="解析后的查询 JSON")
    parser.add_argument("--config", "-c", required=True, help="存储配置 JSON")
    parser.add_argument("--output", "-o", default=None, help="输出 JSON 路径")
    args = parser.parse_args()

    with open(args.queries, "r", encoding="utf-8") as f:
        queries = json.load(f)

    with open(args.config, "r", encoding="utf-8") as f:
        storage_config = json.load(f)

    results = run_estimation(queries, storage_config)

    for qid, r in results.items():
        if "error" in r:
            print(f"{qid}: {r['error']}")
            continue
        total = r.get("total_cost_ms", 0)
        scan_total = r.get("total_scan_cost_ms", 0)
        hj_total = r.get("total_hashjoin_cost_ms", 0)
        vm = r.get("vector_merge_cost_ms", 0)
        print(f"{qid}: total={total:.1f} ms (scan={scan_total:.1f}, hash_join={hj_total:.1f}, vector_merge={vm:.1f})")

    if args.output:
        with open(args.output, "w", encoding="utf-8") as f:
            json.dump(results, f, indent=2, ensure_ascii=False)
        print(f"\n结果已保存到 {args.output}")

    return 0


if __name__ == "__main__":
    exit(main())
