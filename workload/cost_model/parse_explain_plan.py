#!/usr/bin/env python3
"""
从 SQL 文件读取查询，连接数据库执行 EXPLAIN，解析执行计划中的：
  - TABLE FULL SCAN 算子：表名、过滤条件
  - HASH JOIN 算子：涉及的表、等值连接条件、其他条件

输出结构化信息，便于后续估算 SQL 执行代价。

用法:
  python parse_explain_plan.py --sql-file workload/test/tpch_queries.sql
  python parse_explain_plan.py --sql-file tpch_queries.sql --query Q3 --vector "[0.1,0.2,...]" --limit 10
  python parse_explain_plan.py --sql-file tpch_queries.sql --db-name tpch10_5 --db-port 10200
  python parse_explain_plan.py -f tpch_queries.sql -q Q3 --no-db -p q3_plan.txt  # 离线解析计划文件
"""

import re
import json
import argparse
from typing import Optional, Tuple, List, Dict, Any
from pathlib import Path

try:
    import mysql.connector
except ImportError:
    mysql = None


# --- 默认配置 ---
CONFIG = {
    'db_host': '127.0.0.1',
    'db_port': 10200,
    'db_user': 'root',
    'db_name': 'tpch10',
    'vector_file': None,  # 若需替换 {VECTOR}，可指定 .fbin 向量文件
    'vector_limit': 1,
    'result_limit': 10,
}


def load_queries(sql_file: str) -> Dict[str, str]:
    """从 SQL 文件加载查询，格式 --Q1, --Q2, ..."""
    with open(sql_file, 'r', encoding='utf-8') as f:
        content = f.read()
    queries = {}
    matches = re.findall(r'--Q(\d+)(.*?)(?=--Q\d+|$)', content, re.DOTALL)
    for q_id, q_sql in matches:
        queries[f"Q{q_id}"] = q_sql.strip()
    return queries


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


def get_explain_plan(cursor, sql: str) -> Optional[str]:
    """
    执行 EXPLAIN EXTENDED 获取执行计划文本。
    OceanBase 将计划放在 Query Plan 列，可能多行。
    """
    explain_sql = f"EXPLAIN EXTENDED {sql}"
    success, err, rows = safe_execute_query(cursor, explain_sql)
    if not success:
        # 回退到普通 EXPLAIN
        explain_sql = f"EXPLAIN {sql}"
        success, err, rows = safe_execute_query(cursor, explain_sql)
    if not success or not rows:
        return None

    # 拼接计划文本：可能单列多行，或每行一个字段
    lines = []
    for row in rows:
        if isinstance(row, (list, tuple)):
            for cell in row:
                if cell is not None and str(cell).strip():
                    lines.append(str(cell).strip())
        elif row is not None and str(row).strip():
            lines.append(str(row).strip())

    return '\n'.join(lines) if lines else None


def _extract_paren_content(text: str, prefix: str) -> Optional[str]:
    """提取 prefix( 后的括号内容，支持嵌套括号"""
    m = re.search(re.escape(prefix) + r'\s*\(\s*', text)
    if not m:
        return None
    start = m.end()
    depth = 1
    i = start
    while i < len(text) and depth > 0:
        if text[i] == '(':
            depth += 1
        elif text[i] == ')':
            depth -= 1
        i += 1
    return text[start : i - 1].strip() if depth == 0 else None


def _parse_filter_equal_conds(
    op_id: int,
    text: str,
    id_to_filter: dict,
    id_to_equal_conds: dict,
    id_to_other_conds: dict,
) -> None:
    """从算子块文本中解析 filter、equal_conds、other_conds"""
    # filter([...]) 或 filter(nil) - 支持嵌套括号
    fval = _extract_paren_content(text, 'filter')
    if fval and fval.lower() != 'nil':
        id_to_filter[op_id] = fval

    # equal_conds([cond1], [cond2], ...) - 提取所有 [xxx] 内容
    eval_content = _extract_paren_content(text, 'equal_conds')
    if eval_content:
        conds = re.findall(r'\[([^\]]*)\]', eval_content)
        id_to_equal_conds[op_id] = [c.strip() for c in conds if c.strip()]

    # other_conds([...]) 或 other_conds(nil)
    oval = _extract_paren_content(text, 'other_conds')
    if oval:
        if oval.lower() != 'nil':
            id_to_other_conds[op_id] = [oval]
        else:
            id_to_other_conds[op_id] = id_to_other_conds.get(op_id, [])


def parse_plan_operators(plan_text: str) -> Dict[str, Any]:
    """
    解析 OceanBase EXPLAIN 输出，提取 TABLE FULL SCAN 和 HASH JOIN 算子。

    返回:
      {
        "table_scans": [
          {"id": 2, "table": "customer", "filter": "...", "est_rows": 299751},
          ...
        ],
        "hash_joins": [
          {"id": 1, "equal_conds": ["customer.C_CUSTKEY = orders.O_CUSTKEY"], "other_conds": [], "est_rows": 424371},
          ...
        ]
      }
    """
    result = {
        "table_scans": [],
        "hash_joins": [],
    }

    if not plan_text:
        return result

    id_to_operator = {}
    id_to_name = {}
    id_to_est_rows = {}
    id_to_filter = {}
    id_to_equal_conds = {}
    id_to_other_conds = {}

    # 1. 解析算子树：|ID|OPERATOR|NAME|EST.ROWS|EST.TIME(us)|
    for line in plan_text.split('\n'):
        if 'TABLE FULL SCAN' in line.upper():
            m = re.search(r'\|\s*(\d+)\s*\|', line)
            if m:
                op_id = int(m.group(1))
                id_to_operator[op_id] = 'TABLE FULL SCAN'
                cols = [c.strip() for c in line.split('|') if c.strip()]
                # 列顺序: ID, OPERATOR, NAME, EST.ROWS, EST.TIME
                if len(cols) >= 3:
                    id_to_name[op_id] = cols[2]  # NAME 列是表名
                if len(cols) >= 4:
                    try:
                        id_to_est_rows[op_id] = int(float(cols[3].replace(',', '')))
                    except (ValueError, TypeError):
                        pass
        elif 'HASH JOIN' in line.upper():
            m = re.search(r'\|\s*(\d+)\s*\|', line)
            if m:
                op_id = int(m.group(1))
                id_to_operator[op_id] = 'HASH JOIN'
                cols = [c.strip() for c in line.split('|') if c.strip()]
                # 列顺序: ID, OPERATOR, NAME(可能空), EST.ROWS, EST.TIME
                # NAME 为空时 cols[2]=EST.ROWS, cols[3]=EST.TIME(us)
                if len(cols) >= 4:
                    try:
                        id_to_est_rows[op_id] = int(float(cols[2].replace(',', '')))
                    except (ValueError, TypeError):
                        pass

    # 2. 解析 Outputs & filters：按 "|   N - " 分块
    block_pattern = re.compile(r'(?:^|\n)[\|\s]*(\d+)\s+-\s+', re.MULTILINE)
    matches = list(block_pattern.finditer(plan_text))
    for i, m in enumerate(matches):
        op_id = int(m.group(1))
        start = m.end()
        end = matches[i + 1].start() if i + 1 < len(matches) else len(plan_text)
        block = plan_text[start:end]
        _parse_filter_equal_conds(op_id, block, id_to_filter, id_to_equal_conds, id_to_other_conds)

    # 3. 组装结果
    for op_id, op_type in id_to_operator.items():
        if op_type == 'TABLE FULL SCAN':
            result["table_scans"].append({
                "id": op_id,
                "table": id_to_name.get(op_id, ""),
                "filter": id_to_filter.get(op_id),
                "est_rows": id_to_est_rows.get(op_id),
            })
        elif op_type == 'HASH JOIN':
            equal_conds = id_to_equal_conds.get(op_id, [])
            other_conds = id_to_other_conds.get(op_id, [])
            result["hash_joins"].append({
                "id": op_id,
                "equal_conds": equal_conds,
                "other_conds": other_conds,
                "est_rows": id_to_est_rows.get(op_id),
            })

    result["table_scans"].sort(key=lambda x: x["id"])
    result["hash_joins"].sort(key=lambda x: x["id"])
    return result


def main():
    parser = argparse.ArgumentParser(
        description="解析 SQL 执行计划，提取 TABLE FULL SCAN 和 HASH JOIN 算子信息"
    )
    parser.add_argument("--sql-file", "-f", required=True, help="SQL 文件路径")
    parser.add_argument("--query", "-q", default=None, help="指定查询 ID，如 Q3；不指定则解析所有")
    parser.add_argument("--vector", "-v", default=None, help="替换 {VECTOR} 的向量字符串，如 [0.1,0.2,...]")
    parser.add_argument("--limit", "-l", type=int, default=10, help="替换 {LIMIT} 的值")
    parser.add_argument("--db-host", default="127.0.0.1", help="数据库主机")
    parser.add_argument("--db-port", type=int, default=10200, help="数据库端口")
    parser.add_argument("--db-user", default="root", help="数据库用户")
    parser.add_argument("--db-name", default="tpch10", help="数据库名")
    parser.add_argument("--output", "-o", default=None, help="输出 JSON 文件路径")
    parser.add_argument("--no-db", action="store_true", help="不连接数据库，仅解析本地计划文件（需 --plan-file）")
    parser.add_argument("--plan-file", "-p", default=None, help="本地计划文本文件，用于离线解析")
    parser.add_argument("--vector-file", default=None, help=".fbin 向量文件，用于替换 {VECTOR}（取第一个向量）")
    args = parser.parse_args()

    queries = load_queries(args.sql_file)
    if not queries:
        print("未找到任何查询")
        return 1

    if args.query:
        if args.query not in queries:
            print(f"未找到查询 {args.query}")
            return 1
        to_process = [(args.query, queries[args.query])]
    else:
        to_process = list(queries.items())

    all_results = {}

    vector_str = args.vector
    if not vector_str and args.vector_file and Path(args.vector_file).exists():
        try:
            import numpy as np
            with open(args.vector_file, 'rb') as f:
                header = np.fromfile(f, count=2, dtype=np.int32)
                _, dim = header[0], header[1]
                data = np.fromfile(f, count=dim, dtype=np.float32)
                vector_str = '[' + ','.join(map(str, data)) + ']'
        except Exception as e:
            print(f"警告: 无法从 {args.vector_file} 读取向量: {e}")

    for q_name, sql_template in to_process:
        sql = sql_template
        if '{VECTOR}' in sql:
            if vector_str:
                sql = sql.replace('{VECTOR}', vector_str)
            else:
                sql = sql.replace('{VECTOR}', '[0.0]')  # 占位
        if '{LIMIT}' in sql:
            sql = sql.replace('{LIMIT}', str(args.limit))

        if args.no_db and args.plan_file:
            if len(to_process) > 1:
                print("使用 --no-db --plan-file 时请指定 --query 以解析单个查询的计划")
            with open(args.plan_file, 'r', encoding='utf-8') as f:
                plan_text = f.read()
        else:
            if mysql is None:
                print("连接数据库需要 mysql-connector-python，请安装: pip install mysql-connector-python")
                return 1
            conn = mysql.connector.connect(
                host=args.db_host,
                port=args.db_port,
                user=args.db_user,
                database=args.db_name,
                autocommit=True,
                allow_local_infile=True,
                sql_mode='',
                charset='utf8mb4',
                use_unicode=True,
            )
            cursor = conn.cursor()
            try:
                cursor.execute("SET SESSION optimizer_dynamic_sampling = 0")
            except Exception:
                pass

            plan_text = get_explain_plan(cursor, sql)
            cursor.close()
            conn.close()

            if not plan_text:
                print(f"[{q_name}] 无法获取执行计划")
                all_results[q_name] = {"error": "无法获取执行计划"}
                continue

        parsed = parse_plan_operators(plan_text)
        all_results[q_name] = {
            "table_scans": parsed["table_scans"],
            "hash_joins": parsed["hash_joins"],
        }

        print(f"\n=== {q_name} ===")
        print("TABLE FULL SCAN:")
        for ts in parsed["table_scans"]:
            print(f"  ID={ts['id']} 表={ts['table']} 过滤={ts.get('filter', '无')} est_rows={ts.get('est_rows')}")
        print("HASH JOIN:")
        for hj in parsed["hash_joins"]:
            print(f"  ID={hj['id']} equal_conds={hj['equal_conds']} other_conds={hj['other_conds']} est_rows={hj.get('est_rows')}")

    if args.output:
        with open(args.output, 'w', encoding='utf-8') as f:
            json.dump(all_results, f, indent=2, ensure_ascii=False)
        print(f"\n结果已保存到 {args.output}")

    return 0


if __name__ == "__main__":
    exit(main())
