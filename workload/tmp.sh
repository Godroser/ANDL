#!/bin/bash
# 进入目录
mv /data/dzh/seekdb/Exqutor/Vector-augmented_SQL_analytics/tpc-h/TPC-H-Tool/TPC-HV3.0.1/dbgen/*.tbl /data/dzh/seekdb/workload/tpch100
cd /data/dzh/seekdb/workload/tpch100

# 开始转换
for i in *.tbl; do
    if [ -f "$i" ]; then
        echo "Processing $i ..."
        # 移除行尾 | 并存为 csv
        sed 's/|$//' "$i" > "${i%.tbl}.csv"
    fi
done