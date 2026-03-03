import mysql.connector
from mysql.connector import Error

# 数据库基础配置
BASE_CONFIG = {
    'db_host': '127.0.0.1',
    'db_port': 10200,
    'db_user': 'root',
    'password': '',  # 如果有密码请在此填写
}

# 需要处理的数据库列表
TARGET_DATABASES = ['tpch10_6']

def run_analyze_on_databases():
    for db_name in TARGET_DATABASES:
        print(f"\n" + "="*50)
        print(f"正在开始处理数据库: {db_name}")
        print("="*50)
        
        conn = None
        try:
            # 建立连接
            conn = mysql.connector.connect(
                host=BASE_CONFIG['db_host'], 
                port=BASE_CONFIG['db_port'],
                user=BASE_CONFIG['db_user'], 
                password=BASE_CONFIG.get('password', ''),
                database=db_name,
                autocommit=True,
                allow_local_infile=True,
                sql_mode='',
                charset='utf8mb4',
                use_unicode=True
            )
            cursor = conn.cursor()

            # 1. 获取当前数据库下的所有表
            cursor.execute("SHOW TABLES")
            tables = [row[0] for row in cursor.fetchall()]

            if not tables:
                print(f"警告: 数据库 {db_name} 中没有表。")
                continue

            print(f"找到 {len(tables)} 张表，准备执行 ANALYZE TABLE...")

            # 2. 依次对每张表执行 ANALYZE
            for table_name in tables:
                try:
                    print(f"  -> 正在收集表统计信息: {table_name} ...", end="", flush=True)
                    cursor.execute(f"ANALYZE TABLE `{table_name}`")
                    # 消耗掉结果集，防止对下一条执行产生影响
                    cursor.fetchall() 
                    print(" [完成]")
                except Error as table_err:
                    print(f" [失败] 错误原因: {table_err}")

        except Error as db_err:
            print(f"无法连接或处理数据库 {db_name}: {db_err}")
        
        finally:
            if conn and conn.is_connected():
                cursor.close()
                conn.close()
                print(f"已断开与 {db_name} 的连接。")

    print("\n任务全部完成！")

if __name__ == "__main__":
    run_analyze_on_databases()