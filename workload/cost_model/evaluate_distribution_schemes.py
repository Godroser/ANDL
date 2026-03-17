#!/usr/bin/env python3
"""
基于候选数据分布方案，调用 estimate_query_cost 估算每条 SQL 的执行代价，
输出总开销最低的三个方案。
"""

import json
import sys
from pathlib import Path

# 添加 estimate 目录到 path
SCRIPT_DIR = Path(__file__).resolve().parent
ESTIMATE_DIR = SCRIPT_DIR / "estimate"
sys.path.insert(0, str(ESTIMATE_DIR))

from estimate_query_cost import run_estimation


# 日期分区模板（lineitem/orders 的日期列可复用）
DATE_PARTITIONS = [
    {"range": "[1992-01-01, 1993-01-01)", "cardinality": 8570000},
    {"range": "[1993-01-01, 1994-01-01)", "cardinality": 8570000},
    {"range": "[1994-01-01, 1995-01-01)", "cardinality": 8570000},
    {"range": "[1995-01-01, 1996-01-01)", "cardinality": 8570000},
    {"range": "[1996-01-01, 1997-01-01)", "cardinality": 8570000},
    {"range": "[1997-01-01, 1998-01-01)", "cardinality": 8570000},
    {"range": "[1998-01-01, 1999-01-01)", "cardinality": 8570000},
]

ORDERS_DATE_PARTITIONS = [
    {"range": "[1992-01-01, 1993-01-01)", "cardinality": 1500000},
    {"range": "[1993-01-01, 1994-01-01)", "cardinality": 1500000},
    {"range": "[1994-01-01, 1995-01-01)", "cardinality": 1500000},
    {"range": "[1995-01-01, 1996-01-01)", "cardinality": 1500000},
    {"range": "[1996-01-01, 1997-01-01)", "cardinality": 1500000},
    {"range": "[1997-01-01, 1998-01-01)", "cardinality": 1500000},
    {"range": "[1998-01-01, 1999-01-01)", "cardinality": 1500000},
]

# 支持分区裁剪的列及其分区配置
PARTITION_TEMPLATES = {
    "l_shipdate": DATE_PARTITIONS,
    "l_receiptdate": DATE_PARTITIONS,
    "l_commitdate": DATE_PARTITIONS,
    "o_orderdate": ORDERS_DATE_PARTITIONS,
}


def build_storage_config(base_config: dict, scheme_tables: dict) -> dict:
    """
    根据候选方案覆盖 base_config，生成用于代价估算的 storage_config。
    scheme_tables: { table: { partition_key, vector_storage? } }
    """
    import copy
    config = copy.deepcopy(base_config)

    tables_cfg = config.get("tables", {})

    for table, opts in scheme_tables.items():
        if table not in tables_cfg or not isinstance(opts, dict):
            continue
        cfg = tables_cfg[table]

        # 覆盖分区键
        pk_raw = opts.get("partition_key")
        pk = (pk_raw or "").strip().lower() if isinstance(pk_raw, str) else None
        if pk:
            cfg["partition_key"] = pk
            key_col = pk.split(".")[-1].lower()
            cfg["partitions"] = PARTITION_TEMPLATES.get(key_col, [])
        else:
            cfg["partition_key"] = None
            cfg["partitions"] = []

        # 向量存储（全局）
        if "vector_storage" in opts:
            config["vector_storage"] = opts["vector_storage"]

    return config


def main():
    candidates_path = SCRIPT_DIR / "distribution_candidates.json"
    base_config_path = ESTIMATE_DIR / "storage_config.example.json"
    queries_path = ESTIMATE_DIR / "tpch_queries.json"
    out_path = SCRIPT_DIR / "top3_schemes.json"

    if not candidates_path.exists():
        print(f"错误: 未找到 {candidates_path}")
        return 1
    if not base_config_path.exists():
        print(f"错误: 未找到 {base_config_path}")
        return 1
    if not queries_path.exists():
        queries_path = SCRIPT_DIR / "tpch_queries.json"
    if not queries_path.exists():
        print(f"错误: 未找到 tpch_queries.json")
        return 1

    with open(candidates_path, "r", encoding="utf-8") as f:
        candidates_data = json.load(f)

    with open(base_config_path, "r", encoding="utf-8") as f:
        base_config = json.load(f)

    with open(queries_path, "r", encoding="utf-8") as f:
        queries = json.load(f)

    schemes = candidates_data.get("candidate_schemes", [])
    if not schemes:
        print("错误: 无候选方案")
        return 1

    results = []
    total = len(schemes)

    for i, scheme_wrapper in enumerate(schemes):
        scheme = scheme_wrapper.get("tables", scheme_wrapper)
        storage_config = build_storage_config(base_config, scheme)

        est_results = run_estimation(queries, storage_config)
        total_cost = sum(
            r.get("total_cost_ms", 0) for r in est_results.values() if "error" not in r
        )
        results.append({
            "scheme": scheme,
            "total_cost_ms": total_cost,
            "per_query": {qid: r.get("total_cost_ms", 0) for qid, r in est_results.items()},
        })

        if (i + 1) % 100 == 0 or i == 0:
            print(f"  已评估 {i + 1}/{total} 个方案...", flush=True)

    results.sort(key=lambda x: x["total_cost_ms"])

    print("\n" + "=" * 70)
    print("总开销最低的三个方案")
    print("=" * 70)

    for rank, r in enumerate(results[:10], 1):
        print(f"\n【第 {rank} 名】总代价: {r['total_cost_ms']:.1f} ms")
        print("  配置:")
        for t in sorted(r["scheme"].keys()):
            opts = r["scheme"][t]
            if isinstance(opts, dict):
                pk = opts.get("partition_key") or "(空)"
                vs = opts.get("vector_storage", "")
                line = f"    {t}: partition_key={pk}"
                if vs:
                    line += f", vector_storage={vs}"
                print(line)

    output = {
        "top3": [
            {
                "rank": i,
                "total_cost_ms": r["total_cost_ms"],
                "scheme": r["scheme"],
            }
            for i, r in enumerate(results[:10], 1)
        ],
        "all_schemes_count": len(results),
    }
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    print(f"\n结果已保存到 {out_path}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
