#!/bin/bash

# 数据导入脚本
# 用于导入约100GB的TPCH100数据到数据库

set -e  # 遇到错误立即退出

# 数据库连接配置
DB_HOST="127.0.0.1"
DB_PORT="10200"
DB_USER="root"
DB_NAME="tpch10_4"

echo "=========================================="
echo "开始数据导入流程"
echo "=========================================="

# 1. 设置数据库配置参数（针对100GB大数据量）
echo ""
echo "步骤 1: 设置数据库配置参数..."
mysql -uroot -h127.0.0.1 -P10200 <<EOF
-- 1. 设置更大的网络包大小，防止大数据行报错
SET GLOBAL max_allowed_packet = 1073741824; 

-- 2. 延长事务超时时间（100G数据建议设置极大值，单位为微秒）
SET GLOBAL ob_query_timeout = 360000000000; -- 10小时
SET GLOBAL ob_trx_timeout = 360000000000;   -- 10小时

-- 验证设置
SELECT @@max_allowed_packet AS max_allowed_packet;
SELECT @@ob_query_timeout AS ob_query_timeout;
SELECT @@ob_trx_timeout AS ob_trx_timeout;
EOF

if [ $? -ne 0 ]; then
    echo "错误: 数据库配置设置失败"
    exit 1
fi

echo "数据库配置设置完成"
echo ""



# 3. 执行 SQL 脚本导入其他表
echo "步骤 3: 执行 load_table.sql 导入其他表..."
mysql -uroot -h127.0.0.1 -P10200 ${DB_NAME} < /data/dzh/seekdb/workload/tmp-ddl.sql

if [ $? -ne 0 ]; then
    echo "错误: load_table.sql 执行失败"
    exit 1
fi

echo "load_table.sql 执行完成"
echo ""

# # 2. 执行 Python 脚本导入 partsupp 和 part 表（包含向量数据）
# echo "步骤 2: 执行 insert_data.py 导入 partsupp 和 part 表..."
# # 激活虚拟环境
# source /data/dzh/venv/bin/activate
# python /data/dzh/seekdb/workload/insert_data.py

# if [ $? -ne 0 ]; then
#     echo "错误: insert_data.py 执行失败"
#     exit 1
# fi

# echo "insert_data.py 执行完成"
# echo ""

# echo "=========================================="
# echo "数据导入流程全部完成！"
# echo "=========================================="
