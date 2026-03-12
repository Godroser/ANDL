#!/bin/bash

# 定义要监控的进程名称
PROCESS_NAME="analyze_tables.py"

echo "开始监控 $PROCESS_NAME..."

# 循环检查进程是否存在
# pgrep -f 会匹配完整命令行，只要进程还在运行，循环就继续
while pgrep -f "$PROCESS_NAME" > /dev/null; do
    sleep 100  # 每10秒检查一次，节省系统资源
done

echo "检测到 $PROCESS_NAME 已结束，准备启动后续任务..."

# 执行你的后续命令
source /data/dzh/venv/bin/activate
cd /data/dzh/seekdb/workload/test
nohup python -u tmp_sql.py > tmp_sql.log 2>&1 & disown

echo "任务已在后台启动。"