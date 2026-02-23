#!/usr/bin/env python3
"""
导出 TPCH 数据库的所有表数据到 CSV 文件
"""
import os
import csv
import mysql.connector
from mysql.connector import Error
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.panel import Panel

# 配置
CONFIG = {
    'db_host': '127.0.0.1',
    'db_port': 10200,
    'db_user': 'root',
    'db_name': 'tpch',
    'output_dir': '/data/dzh/seekdb/workload/tpch_export'
}

console = Console()

def get_all_tables(conn):
    """获取数据库中的所有表名"""
    cursor = conn.cursor()
    cursor.execute("SHOW TABLES")
    tables = [row[0] for row in cursor.fetchall()]
    cursor.close()
    return tables

def get_table_columns(conn, table_name):
    """获取表的所有列信息"""
    cursor = conn.cursor()
    cursor.execute(f"DESCRIBE {table_name}")
    columns = cursor.fetchall()
    cursor.close()
    return columns

def export_table_to_csv(conn, table_name, output_dir):
    """导出单个表的数据到 CSV 文件"""
    csv_file = os.path.join(output_dir, f"{table_name}.csv")
    
    # 获取列信息
    columns_info = get_table_columns(conn, table_name)
    column_names = [col[0] for col in columns_info]
    
    # 查询数据
    cursor = conn.cursor()
    query = f"SELECT * FROM {table_name}"
    cursor.execute(query)
    
    # 写入 CSV 文件
    with open(csv_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f, delimiter='|')
        # 写入列名（可选）
        # writer.writerow(column_names)
        
        # 批量读取并写入数据
        batch_size = 10000
        while True:
            rows = cursor.fetchmany(batch_size)
            if not rows:
                break
            writer.writerows(rows)
    
    cursor.close()
    
    # 获取行数
    cursor = conn.cursor()
    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    row_count = cursor.fetchone()[0]
    cursor.close()
    
    return row_count

def main():
    console.print(Panel("[bold green]TPCH 数据库导出工具[/bold green]"))
    
    # 创建输出目录
    output_dir = CONFIG['output_dir']
    os.makedirs(output_dir, exist_ok=True)
    console.print(f"[cyan]输出目录: {output_dir}[/cyan]\n")
    
    try:
        # 连接数据库
        console.print(f"[yellow]正在连接到数据库...[/yellow]")
        conn = mysql.connector.connect(
            host=CONFIG['db_host'],
            port=CONFIG['db_port'],
            user=CONFIG['db_user'],
            database=CONFIG['db_name']
        )
        console.print("[green]✓ 数据库连接成功[/green]\n")
        
        # 获取所有表
        tables = get_all_tables(conn)
        console.print(f"[cyan]找到 {len(tables)} 个表: {', '.join(tables)}[/cyan]\n")
        
        # 导出每个表
        results = {}
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console
        ) as progress:
            for table_name in tables:
                task = progress.add_task(f"导出 {table_name}...", total=None)
                try:
                    row_count = export_table_to_csv(conn, table_name, output_dir)
                    results[table_name] = {'status': 'success', 'rows': row_count}
                    progress.update(task, description=f"[green]✓ {table_name}: {row_count} 行[/green]")
                except Error as e:
                    results[table_name] = {'status': 'error', 'error': str(e)}
                    progress.update(task, description=f"[red]✗ {table_name}: 导出失败 - {e}[/red]")
        
        # 显示汇总信息
        console.print("\n[bold blue]导出汇总:[/bold blue]")
        from rich.table import Table
        table = Table(title="导出结果")
        table.add_column("表名", style="cyan")
        table.add_column("状态", style="magenta")
        table.add_column("行数", style="green", justify="right")
        
        total_rows = 0
        for table_name, result in results.items():
            if result['status'] == 'success':
                table.add_row(table_name, "✓ 成功", f"{result['rows']:,}")
                total_rows += result['rows']
            else:
                table.add_row(table_name, "✗ 失败", result.get('error', 'Unknown error'))
        
        console.print(table)
        console.print(f"\n[bold green]总计导出: {total_rows:,} 行数据[/bold green]")
        console.print(f"[cyan]所有文件已保存到: {output_dir}[/cyan]")
        
        conn.close()
        
    except Error as e:
        console.print(f"[red]数据库连接错误: {e}[/red]")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())
