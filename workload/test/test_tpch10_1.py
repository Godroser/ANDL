#!/usr/bin/env python3
import os
import re
import time
import json
import numpy as np
import mysql.connector
from rich.console import Console
from rich.table import Table
from rich.panel import Panel

# --- 配置 ---
CONFIG = {
    'db_host': '127.0.0.1',
    'db_port': 10200,
    'db_user': 'root',
    'db_name': 'tpch10_1',
    'vector_file': '/data/dzh/seekdb/Exqutor/Vector-augmented_SQL_analytics/WIKI/queries.fbin',
    'sql_file': '/data/dzh/seekdb/workload/test/tpch_queries.sql',
    'vector_limit': 2,    # 测试多少个向量
    'result_limit': 10,
    'output_file': None#'split_latency_report.json'
}

# 预设一个纯向量查询的模板，用于测量底层向量检索延时
# 请根据你数据库真实的表名修改 'your_vector_table'
VECTOR_ONLY_TEMPLATE = "SELECT p_partkey FROM part ORDER BY l2_distance(text_embedding,'{VECTOR}') APPROXIMATE LIMIT {LIMIT};"

console = Console()

def safe_execute_query(cursor, sql, description=""):
    """
    安全执行SQL查询，确保完全消费所有结果集，避免 'Commands out of sync' 错误
    支持多语句查询（如 Q15 包含 CREATE VIEW, SELECT, DROP VIEW）
    """
    try:
        # 先清空任何未消费的结果（如果存在）
        try:
            while cursor.nextset():
                cursor.fetchall()
        except:
            pass
        
        # 执行查询
        cursor.execute(sql)
        
        # 获取第一个结果集（如果有）
        try:
            cursor.fetchall()
        except Exception:
            # 某些查询可能没有结果集（如 DDL 语句），这是正常的
            pass
        
        # 处理可能存在的多个结果集（多语句查询或 EXPLAIN 查询）
        while True:
            try:
                if cursor.nextset():
                    cursor.fetchall()
                else:
                    break
            except Exception:
                # 没有更多结果集了
                break
        
        return True, None
    except Exception as e:
        # 如果出错，尝试重置连接状态
        try:
            while cursor.nextset():
                cursor.fetchall()
        except:
            pass
        return False, str(e)

class DataLoader:
    @staticmethod
    def read_vectors(filename, limit):
        with open(filename, "rb") as f:
            header = np.fromfile(f, count=2, dtype=np.int32)
            total_nvecs, dim = header[0], header[1]
            read_count = min(limit, total_nvecs)
            data = np.fromfile(f, count=read_count * dim, dtype=np.float32)
            return data.reshape(-1, dim)

    @staticmethod
    def load_queries(filename):
        with open(filename, 'r') as f:
            content = f.read()
        queries = {}
        matches = re.findall(r'--Q(\d+)(.*?)(?=--Q\d+|$)', content, re.DOTALL)
        for q_id, q_sql in matches:
            queries[f"Q{q_id}"] = q_sql.strip()
        return queries

# ... [前面的 DataLoader 类和配置保持不变] ...

def run_benchmark():
    # 读取向量和SQL查询
    vectors = DataLoader.read_vectors(CONFIG['vector_file'], CONFIG['vector_limit'])
    queries = DataLoader.load_queries(CONFIG['sql_file'])
    
    console.print(f"[bold green]已加载 {len(vectors)} 个向量和 {len(queries)} 条SQL查询[/bold green]\n")
    
    conn = mysql.connector.connect(
        host=CONFIG['db_host'], port=CONFIG['db_port'],
        user=CONFIG['db_user'], database=CONFIG['db_name'],
        autocommit=True,
        allow_local_infile=True,
        sql_mode='',  # 允许更宽松的SQL模式
        charset='utf8mb4',
        use_unicode=True
    )
    cursor = conn.cursor()

    # 禁用动态采样，避免 optimizer 在复杂查询（含向量）上触发 dynamic sampling timeout (4016)
    try:
        cursor.execute("SET SESSION optimizer_dynamic_sampling = 0")
    except Exception as e:
        console.print(f"[yellow]警告: 无法禁用动态采样: {e}[/yellow]")

    report = {}
    all_vec_latencies = []  # 用于存储所有纯向量查询的样本

    # 按 Q1, Q2, ..., Q22 数字顺序测试（sorted() 默认按字符串排序会得到 Q1,Q10,Q11,...,Q2,Q20,...）
    def _query_sort_key(item):
        name = item[0] if isinstance(item, tuple) else item
        return int(name[1:]) if name.startswith('Q') and name[1:].isdigit() else 0
    sorted_queries = sorted(queries.items(), key=_query_sort_key)
    for q_name, q_template in sorted_queries:
        console.print(f"[*] 正在测试 {q_name}...")
        report[q_name] = {"sql_times": [], "has_vector": False}
        
        # 检查查询是否包含向量占位符
        has_vector_placeholder = '{VECTOR}' in q_template
        has_limit_placeholder = '{LIMIT}' in q_template
        report[q_name]["has_vector"] = has_vector_placeholder

        if has_vector_placeholder:
            # 对于包含向量的查询，使用每个向量进行测试
            for i, vec in enumerate(vectors):
                vec_str = "[" + ",".join(map(str, vec)) + "]"
                
                # --- 测试 1: 纯向量查询 (用于测量底层向量检索延时) ---
                vec_sql = VECTOR_ONLY_TEMPLATE.replace('{VECTOR}', vec_str).replace('{LIMIT}', str(CONFIG['result_limit']))
                start_v = time.perf_counter()
                success, error = safe_execute_query(cursor, vec_sql, "纯向量查询")
                if success:
                    all_vec_latencies.append((time.perf_counter() - start_v) * 1000)
                else:
                    console.print(f"  [yellow]纯向量查询警告: {error}[/yellow]")
                # 确保清空结果
                try:
                    while cursor.nextset():
                        cursor.fetchall()
                except:
                    pass

                # --- 测试 2: 综合业务 SQL (包含向量) ---
                full_sql = q_template.replace('{VECTOR}', vec_str)
                if has_limit_placeholder:
                    full_sql = full_sql.replace('{LIMIT}', str(CONFIG['result_limit']))
                start_s = time.perf_counter()
                success, error = safe_execute_query(cursor, full_sql, "SQL业务查询")
                if success:
                    report[q_name]["sql_times"].append((time.perf_counter() - start_s) * 1000)
                else:
                    console.print(f"  [red]SQL 业务查询出错: {error}[/red]")
                # 确保清空结果
                try:
                    while cursor.nextset():
                        cursor.fetchall()
                except:
                    pass
        else:
            # 对于不包含向量的查询，直接执行（不进行向量替换）
            # 只执行一次，因为这些查询不依赖向量
            full_sql = q_template
            if has_limit_placeholder:
                full_sql = full_sql.replace('{LIMIT}', str(CONFIG['result_limit']))
            start_s = time.perf_counter()
            success, error = safe_execute_query(cursor, full_sql, "SQL查询")
            if success:
                report[q_name]["sql_times"].append((time.perf_counter() - start_s) * 1000)
            else:
                console.print(f"  [red]SQL 查询出错: {error}[/red]")
            # 确保清空结果
            try:
                while cursor.nextset():
                    cursor.fetchall()
            except:
                pass

    # 计算统计结果
    global_avg_vec = np.mean(all_vec_latencies) if all_vec_latencies else 0
    
    console.print(f"\n[bold blue]基础指标:[/bold blue]")
    if all_vec_latencies:
        console.print(f"  [green]纯向量检索平均耗时 (Base Latency): {global_avg_vec:.2f} ms[/green]")
        console.print(f"  [green]纯向量检索样本数: {len(all_vec_latencies)}[/green]\n")
    else:
        console.print(f"  [yellow]未执行纯向量查询测试[/yellow]\n")

    # 输出所有查询的平均延时表格
    table = Table(title=f"TPC-H 22条查询性能测试结果 (共测试 {len(queries)} 条查询)")
    table.add_column("Query ID", style="cyan")
    table.add_column("是否包含向量", style="yellow", justify="center")
    table.add_column("测试次数", style="blue", justify="right")
    table.add_column("平均延时 (ms)", style="magenta", justify="right")
    table.add_column("最小延时 (ms)", style="green", justify="right")
    table.add_column("最大延时 (ms)", style="red", justify="right")

    total_avg = []
    for q_name in sorted(report.keys(), key=_query_sort_key):
        data = report[q_name]
        if data["sql_times"]:
            avg_sql = np.mean(data["sql_times"])
            min_sql = np.min(data["sql_times"])
            max_sql = np.max(data["sql_times"])
            total_avg.append(avg_sql)
            
            table.add_row(
                q_name,
                "是" if data["has_vector"] else "否",
                str(len(data["sql_times"])),
                f"{avg_sql:.2f}",
                f"{min_sql:.2f}",
                f"{max_sql:.2f}"
            )
        else:
            table.add_row(
                q_name,
                "是" if data["has_vector"] else "否",
                "0",
                "N/A",
                "N/A",
                "N/A"
            )

    console.print(table)
    
    # 输出总体统计
    if total_avg:
        overall_avg = np.mean(total_avg)
        console.print(f"\n[bold cyan]总体统计:[/bold cyan]")
        console.print(f"  [green]所有查询平均延时: {overall_avg:.2f} ms[/green]")
        console.print(f"  [green]成功测试的查询数: {len(total_avg)}/{len(queries)}[/green]")

    # 保存结果
    final_output = {
        "config": {
            "vector_limit": CONFIG['vector_limit'],
            "result_limit": CONFIG['result_limit'],
            "total_queries": len(queries),
            "total_vectors_tested": len(vectors)
        },
        "global_vector_base_ms": global_avg_vec,
        "overall_avg_latency_ms": np.mean(total_avg) if total_avg else 0,
        "queries": {}
    }
    
    # 为每条查询保存详细统计
    for q_name, data in sorted(report.items(), key=lambda x: _query_sort_key(x[0])):
        if data["sql_times"]:
            final_output["queries"][q_name] = {
                "has_vector": data["has_vector"],
                "test_count": len(data["sql_times"]),
                "avg_latency_ms": np.mean(data["sql_times"]),
                "min_latency_ms": float(np.min(data["sql_times"])),
                "max_latency_ms": float(np.max(data["sql_times"])),
                "all_latencies_ms": data["sql_times"]
            }
        else:
            final_output["queries"][q_name] = {
                "has_vector": data["has_vector"],
                "test_count": 0,
                "error": "No successful tests"
            }
    if CONFIG['output_file'] is not None:
        with open(CONFIG['output_file'], 'w') as f:
            json.dump(final_output, f, indent=4)
        
        console.print(f"\n[bold green]结果已保存到: {CONFIG['output_file']}[/bold green]")
    
    cursor.close()
    conn.close()

if __name__ == "__main__":
    run_benchmark()