#!/usr/bin/env python3
"""
分析 tpch_queries.json：
1. 统计纯标量查询数量 vs 标量+向量混合查询数量
2. 从 table_scans 和 hash_joins 的 filter/other_conds 中提取过滤列
3. 统计每个表中出现在过滤条件中的列及其出现次数
"""

import json
import re
from collections import defaultdict
from pathlib import Path


# 向量过滤模式：用于判断是否为混合查询
VECTOR_FILTER_PATTERNS = ("vector_filter", "l2_distance")

# 表.列 正则：匹配 table.COLUMN 或 table.column 格式
TABLE_COLUMN_RE = re.compile(r"([a-zA-Z_][a-zA-Z0-9_]*)\s*\.\s*([a-zA-Z_][a-zA-Z0-9_]*)", re.IGNORECASE)


def load_queries(json_path: str) -> dict:
    """加载 tpch_queries.json"""
    with open(json_path, "r", encoding="utf-8") as f:
        return json.load(f)


def is_vector_filter(filter_str: str | None) -> bool:
    """判断 filter 是否包含向量过滤"""
    if not filter_str:
        return False
    return any(p in filter_str for p in VECTOR_FILTER_PATTERNS)


def classify_queries(queries: dict) -> tuple[list[str], list[str]]:
    """
    将查询分为：纯标量、标量+向量混合
    返回 (scalar_only_queries, mixed_queries)
    """
    scalar_only = []
    mixed = []

    for qid, data in queries.items():
        has_vector = False
        for scan in data.get("table_scans", []):
            if is_vector_filter(scan.get("filter")):
                has_vector = True
                break

        if has_vector:
            mixed.append(qid)
        else:
            scalar_only.append(qid)

    return scalar_only, mixed


def extract_columns_from_text(text: str | None) -> list[tuple[str, str]]:
    """
    从文本中提取 table.column 对，返回 [(table, column), ...]
    """
    if not text or text.lower() in ("null", "nil", "none"):
        return []

    found = []
    for m in TABLE_COLUMN_RE.finditer(text):
        table, col = m.group(1), m.group(2)
        found.append((table, col))
    return found


def collect_filter_columns(queries: dict) -> dict[str, dict[str, int]]:
    """
    从 table_scans.filter 和 hash_joins.other_conds 中收集过滤列
    返回: {table: {column: count}}
    """
    table_col_counts: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))

    for qid, data in queries.items():
        # table_scans.filter
        for scan in data.get("table_scans", []):
            filter_str = scan.get("filter")
            for table, col in extract_columns_from_text(filter_str):
                table_col_counts[table][col] += 1

        # hash_joins.other_conds（连接条件外的过滤条件）
        for hj in data.get("hash_joins", []):
            for cond in hj.get("other_conds", []):
                if isinstance(cond, str):
                    for table, col in extract_columns_from_text(cond):
                        table_col_counts[table][col] += 1

    return dict(table_col_counts)


def main():
    script_dir = Path(__file__).resolve().parent
    json_path = script_dir / "tpch_queries.json"

    queries = load_queries(json_path)
    scalar_only, mixed = classify_queries(queries)

    print("=" * 60)
    print("TPC-H 查询类型统计")
    print("=" * 60)
    print(f"纯标量查询数量: {len(scalar_only)}")
    print(f"  查询: {', '.join(scalar_only)}")
    print()
    print(f"标量+向量混合查询数量: {len(mixed)}")
    print(f"  查询: {', '.join(mixed)}")
    print()

    table_col_counts = collect_filter_columns(queries)

    print("=" * 60)
    print("各表过滤列统计 (table_scans.filter + hash_joins.other_conds)")
    print("=" * 60)

    for table in sorted(table_col_counts.keys()):
        col_counts = table_col_counts[table]
        total = sum(col_counts.values())
        print(f"\n表 {table} (共 {total} 次过滤引用):")
        for col, count in sorted(col_counts.items(), key=lambda x: (-x[1], x[0])):
            print(f"  - {col}: {count} 次")


if __name__ == "__main__":
    main()
