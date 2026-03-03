#!/bin/bash
# 等待 import_data.sh 进程结束后，依次运行 test_tpch10.py 两次（不同 db_name），输出到对应日志

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TEST_SCRIPT="${SCRIPT_DIR}/test/test_tpch10.py"
PYTHON_SCRIPT="${SCRIPT_DIR}/test/test_tpch10.py"
VENV_ACTIVATE="/data/dzh/venv/bin/activate"

# 激活虚拟环境（Python 脚本依赖此环境）
source "$VENV_ACTIVATE"

# 可选：传入 PID 参数，用于等待指定进程结束；若不传则通过进程名检测
WAIT_PID="${1:-}"

echo "[$(date '+%Y-%m-%d %H:%M:%S')] 等待 import_data.sh 进程结束..."

if [ -n "$WAIT_PID" ]; then
    # 使用传入的 PID 等待
    while kill -0 "$WAIT_PID" 2>/dev/null; do
        sleep 2
    done
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] 进程 PID $WAIT_PID 已结束"
else
    # 通过进程名检测：等待 import_data.sh 结束
    while pgrep -f "import_data\.sh" >/dev/null 2>&1; do
        sleep 2
    done
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] import_data.sh 进程已结束"
fi

# 第一次运行：db_name 为 tpch10（默认），输出到 test_tpch10_227.log
echo "[$(date '+%Y-%m-%d %H:%M:%S')] 开始第一次运行 test_tpch10.py (db_name=tpch10)..."
cd "${SCRIPT_DIR}/test"
python3 test_tpch10.py 2>&1 | tee test_tpch10_227.log
echo "[$(date '+%Y-%m-%d %H:%M:%S')] 第一次运行完成"

# 修改 test_tpch10.py 中的 db_name 为 tpch10_1
echo "[$(date '+%Y-%m-%d %H:%M:%S')] 修改 CONFIG['db_name'] 为 'tpch10_1'..."
sed -i "s/'db_name': 'tpch10'/'db_name': 'tpch10_1'/" "${PYTHON_SCRIPT}"

# 第二次运行：db_name 为 tpch10_1，输出到 test_tpch10_1_227.log
echo "[$(date '+%Y-%m-%d %H:%M:%S')] 开始第二次运行 test_tpch10.py (db_name=tpch10_1)..."
python3 test_tpch10.py 2>&1 | tee test_tpch10_1_227.log
echo "[$(date '+%Y-%m-%d %H:%M:%S')] 第二次运行完成"

# 恢复 db_name 为 tpch10（可选，便于后续使用）
echo "[$(date '+%Y-%m-%d %H:%M:%S')] 恢复 CONFIG['db_name'] 为 'tpch10'..."
sed -i "s/'db_name': 'tpch10_1'/'db_name': 'tpch10'/" "${PYTHON_SCRIPT}"

echo "[$(date '+%Y-%m-%d %H:%M:%S')] 全部完成。日志: test_tpch10_227.log, test_tpch10_1_227.log"
