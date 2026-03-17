#!/usr/bin/env python3
"""
根据 distribution_candidates.json 的 partition_key_candidates，查询 MySQL tpch10，
生成每个分区键下各分区的基数。

- 数值/日期连续列：范围分区，均等分成 8 个分区
- 其他列：list 分区，每个 distinct 值一个分区

输出格式与 storage_config.example.json 的 partitions 一致。

依赖: pip install mysql-connector-python

用法:
  python generate_partition_cardinalities.py
  python generate_partition_cardinalities.py -o partition_cardinalities.json
"""

import json
import argparse
from datetime import datetime, date, timedelta
from typing import Dict, List, Any, Optional, Tuple
from pathlib import Path

try:
    import mysql.connector
except ImportError:
    mysql = None

DB_CONFIG = {
    "host": "127.0.0.1",
    "port": 10200,
    "user": "root",
    "password": "",
    "database": "tpch10",
}

N_RANGE_PARTITIONS = 8

# 范围分区适用的类型（连续数值/日期）
RANGE_TYPES = {
    "int", "integer", "bigint", "smallint", "tinyint", "mediumint",
    "decimal", "numeric", "float", "double", "real",
    "date", "datetime", "timestamp",
}


def get_column_info(cursor, table: str, col: str) -> Optional[Tuple[str, str]]:
    """查询列的实际名称和数据类型，返回 (actual_col_name, data_type)。兼容大小写。"""
    try:
        cursor.execute(
            "SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS "
            "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s AND LOWER(COLUMN_NAME) = LOWER(%s)",
            (DB_CONFIG["database"], table, col),
        )
        row = cursor.fetchone()
        if row:
            return (row[0], row[1].lower())
    except Exception:
        pass
    return None


def get_column_type(cursor, table: str, col: str) -> Optional[str]:
    """查询列的数据类型"""
    info = get_column_info(cursor, table, col)
    return info[1] if info else None


def resolve_column_name(cursor, table: str, col: str) -> str:
    """解析列在数据库中的实际名称（用于 SQL 中）"""
    info = get_column_info(cursor, table, col)
    return info[0] if info else col


def is_range_column(cursor, table: str, col: str) -> bool:
    """判断列是否为连续数值/日期，适合范围分区"""
    dtype = get_column_type(cursor, table, col)
    return dtype in RANGE_TYPES if dtype else False


def get_range_partitions(
    cursor, table: str, col: str, n: int = N_RANGE_PARTITIONS
) -> List[Dict[str, Any]]:
    """范围分区：均等分成 n 个分区，查询每个分区的基数"""
    col_actual = resolve_column_name(cursor, table, col)
    if not col_actual:
        return []
    q = f"SELECT MIN(`{col_actual}`) as lo, MAX(`{col_actual}`) as hi FROM `{table}`"
    try:
        cursor.execute(q)
        row = cursor.fetchone()
        if not row or row[0] is None or row[1] is None:
            return []
        lo, hi = row[0], row[1]
    except Exception as e:
        print(f"  [WARN] {table}.{col} min/max: {e}")
        return []

    dtype = get_column_type(cursor, table, col)
    is_date = dtype and dtype in ("date", "datetime", "timestamp")

    if is_date:
        lo_d = lo.date() if isinstance(lo, datetime) else lo
        hi_d = hi.date() if isinstance(hi, datetime) else hi
        delta_days = (hi_d - lo_d).days
        if delta_days <= 0:
            bounds = [lo_d, hi_d]
        else:
            bounds = [
                lo_d + timedelta(days=int(delta_days * i / n))
                for i in range(n + 1)
            ]
            bounds[-1] = hi_d
    else:
        # 数值分区
        lo_val = float(lo) if lo is not None else 0
        hi_val = float(hi) if hi is not None else 0
        if lo_val >= hi_val:
            step = 1
        else:
            step = (hi_val - lo_val) / n
        bounds = [lo_val + i * step for i in range(n + 1)]
        bounds[-1] = hi_val

    partitions = []
    for i in range(len(bounds) - 1):
        b_lo, b_hi = bounds[i], bounds[i + 1]
        if is_date:
            lo_str = b_lo.isoformat() if hasattr(b_lo, "isoformat") else str(b_lo)
            hi_str = b_hi.isoformat() if hasattr(b_hi, "isoformat") else str(b_hi)
        else:
            lo_str = str(int(b_lo)) if b_lo == int(b_lo) else str(b_lo)
            hi_str = str(int(b_hi)) if b_hi == int(b_hi) else str(b_hi)

        try:
            cursor.execute(
                f"SELECT COUNT(*) FROM `{table}` WHERE `{col_actual}` >= %s AND `{col_actual}` < %s",
                (b_lo, b_hi),
            )
            cnt = cursor.fetchone()[0]
        except Exception as e:
            print(f"  [WARN] {table}.{col} range [{lo_str},{hi_str}): {e}")
            cnt = 0

        partitions.append({
            "range": f"[{lo_str}, {hi_str})",
            "cardinality": cnt,
        })

    return partitions


def get_list_partitions(cursor, table: str, col: str) -> List[Dict[str, Any]]:
    """list 分区：每个 distinct 值一个分区"""
    col_actual = resolve_column_name(cursor, table, col)
    if not col_actual:
        return []
    try:
        cursor.execute(
            f"SELECT `{col_actual}`, COUNT(*) FROM `{table}` GROUP BY `{col_actual}` ORDER BY `{col_actual}`"
        )
        rows = cursor.fetchall()
    except Exception as e:
        print(f"  [WARN] {table}.{col} list: {e}")
        return []

    partitions = []
    for val, cnt in rows:
        if val is None:
            val_str = "NULL"
        elif isinstance(val, (date, datetime)):
            val_str = val.isoformat() if hasattr(val, "isoformat") else str(val)
        else:
            val_str = str(val)
        partitions.append({
            "list": [val_str],
            "cardinality": cnt,
        })
    return partitions


def get_no_partition_total(cursor, table: str) -> int:
    """无分区键时，整表基数"""
    try:
        cursor.execute(f"SELECT COUNT(*) FROM `{table}`")
        return cursor.fetchone()[0]
    except Exception as e:
        print(f"  [WARN] {table} count: {e}")
        return 0


def table_exists(cursor, table: str) -> bool:
    try:
        cursor.execute(
            "SELECT 1 FROM INFORMATION_SCHEMA.TABLES "
            "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s",
            (DB_CONFIG["database"], table),
        )
        return cursor.fetchone() is not None
    except Exception:
        return False


def main() -> int:
    parser = argparse.ArgumentParser(
        description="根据 partition_key_candidates 生成各分区键下的分区基数"
    )
    script_dir = Path(__file__).resolve().parent
    parser.add_argument(
        "--candidates", "-c",
        default=str(script_dir / "distribution_candidates.json"),
        help="distribution_candidates.json 路径",
    )
    parser.add_argument(
        "--output", "-o",
        default=str(script_dir / "partition_cardinalities.json"),
        help="输出 JSON 路径",
    )
    parser.add_argument(
        "--db", "-d",
        default="tpch10",
        help="数据库名",
    )
    args = parser.parse_args()

    if mysql is None:
        print("需要 mysql-connector-python，请安装: pip install mysql-connector-python")
        return 1

    with open(args.candidates, "r", encoding="utf-8") as f:
        data = json.load(f)
    pk_candidates = data.get("summary", {}).get("partition_key_candidates", {})
    if not pk_candidates:
        print("未找到 partition_key_candidates")
        return 1

    DB_CONFIG["database"] = args.db
    conn = mysql.connector.connect(
        host=DB_CONFIG["host"],
        port=DB_CONFIG["port"],
        user=DB_CONFIG["user"],
        password=DB_CONFIG.get("password", ""),
        database=DB_CONFIG["database"],
        autocommit=True,
        charset="utf8mb4",
        use_unicode=True,
    )
    cursor = conn.cursor()

    result: Dict[str, Dict[str, List[Dict[str, Any]]]] = {}
    for table, keys in pk_candidates.items():
        if not table_exists(cursor, table):
            print(f"[SKIP] 表 {table} 不存在")
            continue
        result[table] = {}
        for pk in keys:
            if pk == "":
                total = get_no_partition_total(cursor, table)
                result[table][""] = [
                    {"range": "[0, 1)", "cardinality": total}
                ]
                print(f"  {table} (无分区): {total}")
                continue
            if is_range_column(cursor, table, pk):
                parts = get_range_partitions(cursor, table, pk)
                result[table][pk] = parts
                total = sum(p.get("cardinality", 0) for p in parts)
                print(f"  {table}.{pk} (range x{N_RANGE_PARTITIONS}): {total} rows")
            else:
                parts = get_list_partitions(cursor, table, pk)
                result[table][pk] = parts
                total = sum(p.get("cardinality", 0) for p in parts)
                print(f"  {table}.{pk} (list x{len(parts)}): {total} rows")

    cursor.close()
    conn.close()

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, ensure_ascii=False)
    print(f"\n结果已保存到 {args.output}")
    return 0


if __name__ == "__main__":
    exit(main())
