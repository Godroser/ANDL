#!/usr/bin/env python3
"""
基于 analyze_tpch_queries_json 的分析结果，生成候选数据分布方案集合。

规则：
1. 分区键：每个表过滤次数 > 2 的列作为候选，同时包含「不设置分区键」
2. 向量存储：标量+向量混合查询涉及的表（含向量列），三种方式均为候选：
   - separated: 分离存储
   - both: 复制（双份存储）
   - together: 不分离（向量标量同存）
"""

import itertools
import json
from pathlib import Path

# 复用分析逻辑
from analyze_tpch_queries_json import (
    load_queries,
    classify_queries,
    collect_filter_columns,
    is_vector_filter,
)

# 向量存储方式：分离(separated)、复制(both)、不分离(together)
VECTOR_STORAGE_OPTIONS = ["separated", "both", "together"]
VECTOR_STORAGE_LABELS = {
    "separated": "分离",
    "both": "复制",
    "together": "不分离",
}

# TPC-H 表及向量列配置（与 storage_config 一致）
TABLES_WITH_VECTOR = {"partsupp": "ps_text_embedding"}


def get_tables_in_mixed_queries(queries: dict, mixed_qids: list[str]) -> set[str]:
    """获取标量+向量混合查询涉及的所有表"""
    tables = set()
    for qid in mixed_qids:
        data = queries.get(qid, {})
        for scan in data.get("table_scans", []):
            tables.add(scan["table"].lower())
        for hj in data.get("hash_joins", []):
            for t in hj.get("tables", []):
                tables.add(t.lower())
    return tables


def get_vector_tables_in_mixed(
    queries: dict, mixed_qids: list[str]
) -> set[str]:
    """获取混合查询中实际使用向量过滤的表"""
    tables = set()
    for qid in mixed_qids:
        data = queries.get(qid, {})
        for scan in data.get("table_scans", []):
            if is_vector_filter(scan.get("filter")):
                tables.add(scan["table"].lower())
    return tables


def normalize_column(col: str, table: str) -> str:
    """将列名规范化为小写（与 storage_config 一致）"""
    return col.lower()


def build_partition_key_candidates(
    table_col_counts: dict[str, dict[str, int]],
    min_filter_count: int = 3,
    exclude_vector_columns: bool = True,
) -> dict[str, list[str | None]]:
    """
    为每个表构建分区键候选列表。
    候选 = 过滤次数 > min_filter_count 的列 + None（不分区）
    向量列（如 ps_text_embedding）默认不作为分区键候选
    """
    candidates: dict[str, list[str | None]] = {}

    vector_cols = set(TABLES_WITH_VECTOR.values())

    for table, col_counts in table_col_counts.items():
        table_lower = table.lower()
        # 过滤次数 > 2 的列（即 >= 3），排除向量列
        hot_cols = [
            normalize_column(col, table)
            for col, cnt in col_counts.items()
            if cnt >= min_filter_count
            and (not exclude_vector_columns or normalize_column(col, table) not in vector_cols)
        ]
        hot_cols = sorted(set(hot_cols))
        candidates[table_lower] = [None] + hot_cols

    return candidates


def build_vector_storage_candidates(
    queries: dict, mixed_qids: list[str]
) -> dict[str, list[str]]:
    """
    为含向量列且出现在混合查询中的表构建向量存储方式候选。
    """
    vector_tables = get_vector_tables_in_mixed(queries, mixed_qids)
    return {t: list(VECTOR_STORAGE_OPTIONS) for t in vector_tables}


def generate_candidate_schemes(
    partition_candidates: dict[str, list[str | None]],
    vector_storage_candidates: dict[str, list[str]],
    all_tables: set[str],
) -> list[dict]:
    """
    生成候选方案集合（笛卡尔积）。
    每个方案为完整配置：{ table: { partition_key, vector_storage } }
    """
    # 确定需要枚举的表
    tables_with_partition = [
        t for t in all_tables if t in partition_candidates and partition_candidates[t]
    ]
    tables_with_vector = list(vector_storage_candidates.keys())

    # 分区键组合
    partition_choices = [
        partition_candidates.get(t, [None]) for t in tables_with_partition
    ]
    partition_combos = list(itertools.product(*partition_choices))

    # 向量存储组合（仅对含向量表）
    if tables_with_vector:
        vector_choices = [vector_storage_candidates[t] for t in tables_with_vector]
        vector_combos = list(itertools.product(*vector_choices))
    else:
        vector_combos = [()]

    schemes = []
    for p_combo in partition_combos:
        for v_combo in vector_combos:
            scheme = {}
            for i, t in enumerate(tables_with_partition):
                scheme[t] = {"partition_key": p_combo[i]}
                if t in tables_with_vector:
                    idx = tables_with_vector.index(t)
                    scheme[t]["vector_storage"] = v_combo[idx]
            # 补充仅有向量选项的表（如仅有 vector 无 partition 候选）
            for t in tables_with_vector:
                if t not in scheme:
                    idx = tables_with_vector.index(t)
                    scheme[t] = {
                        "partition_key": None,
                        "vector_storage": v_combo[idx],
                    }
                elif "vector_storage" not in scheme[t]:
                    idx = tables_with_vector.index(t)
                    scheme[t]["vector_storage"] = v_combo[idx]
            schemes.append(scheme)

    return schemes


def main():
    script_dir = Path(__file__).resolve().parent
    json_path = script_dir / "tpch_queries.json"

    queries = load_queries(json_path)
    scalar_only, mixed = classify_queries(queries)
    table_col_counts = collect_filter_columns(queries)

    # 分区键候选：过滤次数 > 2 的列 + 空
    partition_candidates = build_partition_key_candidates(
        table_col_counts, min_filter_count=3
    )

    # 向量存储候选：混合查询中涉及向量过滤的表
    vector_storage_candidates = build_vector_storage_candidates(queries, mixed)

    # 所有出现过的表
    all_tables = set(table_col_counts.keys()) | set(t.lower() for t in table_col_counts)
    all_tables = {t.lower() for t in table_col_counts}
    # 确保 TPC-H 标准表都在（即使没有过滤列）
    tpc_h_tables = {
        "region", "nation", "supplier", "customer", "part",
        "partsupp", "orders", "lineitem"
    }
    all_tables |= tpc_h_tables

    # 为没有过滤统计的表补充 partition 候选 [None]
    for t in all_tables:
        if t not in partition_candidates:
            partition_candidates[t] = [None]

    schemes = generate_candidate_schemes(
        partition_candidates, vector_storage_candidates, all_tables
    )

    # 输出
    output = {
        "summary": {
            "scalar_only_count": len(scalar_only),
            "mixed_count": len(mixed),
            "mixed_queries": mixed,
            "vector_storage_labels": VECTOR_STORAGE_LABELS,
            "partition_key_candidates": {
                k: [c if c else "" for c in v]
                for k, v in partition_candidates.items()
            },
            "vector_storage_candidates": vector_storage_candidates,
            "total_schemes": len(schemes),
        },
        "candidate_schemes": [
            {
                "tables": {
                    t: {
                        "partition_key": s[t].get("partition_key") or "",
                        **(
                            {"vector_storage": s[t]["vector_storage"]}
                            if "vector_storage" in s[t]
                            else {}
                        ),
                    }
                    for t in sorted(s.keys())
                }
            }
            for s in schemes
        ],
    }

    out_path = script_dir / "distribution_candidates.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print("=" * 60)
    print("候选数据分布方案生成结果")
    print("=" * 60)
    print(f"\n分区键候选（过滤次数>2的列 + 空）:")
    for t in sorted(partition_candidates.keys()):
        cands = partition_candidates[t]
        disp = [c or "(空)" for c in cands]
        print(f"  {t}: {disp}")

    print(f"\n向量存储候选（混合查询涉及的表）:")
    for t in sorted(vector_storage_candidates.keys()):
        labels = [f"{v}({VECTOR_STORAGE_LABELS[v]})" for v in vector_storage_candidates[t]]
        print(f"  {t}: {labels}")

    print(f"\n总候选方案数: {len(schemes)}")
    print(f"\n已写入: {out_path}")

    # 打印方案示例：前2个 + 一个含非空分区键的
    print("\n示例方案:")
    for i, s in enumerate(schemes[:2]):
        print(f"  方案 {i+1}: {json.dumps(s, ensure_ascii=False)}")
    # 找一个含非空分区键的方案
    for i, s in enumerate(schemes):
        if any(v.get("partition_key") for t, v in s.items() if isinstance(v, dict)):
            print(f"  方案（含分区键）: {json.dumps(s, ensure_ascii=False)}")
            break


if __name__ == "__main__":
    main()
