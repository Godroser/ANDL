#!/usr/bin/env python3
"""
从 part 和 partsupp 的 CSV 文件中提取主键和向量列，生成 part_vector 和 partsupp_vector 的 CSV 文件。
用于快速导入向量表，避免 INSERT INTO ... SELECT 的慢速执行。
"""

import csv
import os
import sys
from pathlib import Path

# 路径配置 原始表数据和新生成的表数据
SRC_DIR = Path("/data/dzh/seekdb/workload/tmp/vector_load_tpch10")
OUT_DIR = Path("/data/dzh/seekdb/workload/tpch_10_vector")

# part: 10列 -> part_vector 取 col1(p_partkey), col10(text_embedding)
# partsupp: 7列有效 -> partsupp_vector 取 col1(ps_partkey), col2(ps_suppkey), col6(ps_image_embedding), col7(ps_text_embedding)


def gen_part_vector_csv():
    """从 part_with_vec_*.csv 生成 part_vector_*.csv (p_partkey, text_embedding)"""
    out_dir = OUT_DIR / "part_vector"
    out_dir.mkdir(parents=True, exist_ok=True)

    for i in range(100):
        src_file = SRC_DIR / f"part_with_vec_{i}.csv"
        dst_file = out_dir / f"part_vector_{i}.csv"
        if not src_file.exists():
            print(f"Warning: {src_file} not found, skip")
            continue

        with open(src_file, "r", encoding="utf-8", errors="replace") as fin, \
             open(dst_file, "w", encoding="utf-8", newline="") as fout:
            reader = csv.reader(fin, delimiter="|")
            writer = csv.writer(fout, delimiter="|", lineterminator="\n")
            for row in reader:
                if len(row) >= 10:
                    # p_partkey=col1, text_embedding=col10 (col11可能因末尾|为空)
                    writer.writerow([row[0], row[9]])
                else:
                    print(f"Warning: {src_file} row has {len(row)} cols, skip")

        if (i + 1) % 20 == 0:
            print(f"part_vector: processed {i + 1}/100 files")

    print("part_vector CSV generation done.")


def gen_partsupp_vector_csv():
    """从 partsupp_with_vec_*.csv 生成 partsupp_vector_*.csv (ps_partkey, ps_suppkey, ps_image_embedding, ps_text_embedding)"""
    out_dir = OUT_DIR / "partsupp_vector"
    out_dir.mkdir(parents=True, exist_ok=True)

    for i in range(400):
        src_file = SRC_DIR / f"partsupp_with_vec_{i}.csv"
        dst_file = out_dir / f"partsupp_vector_{i}.csv"
        if not src_file.exists():
            print(f"Warning: {src_file} not found, skip")
            continue

        with open(src_file, "r", encoding="utf-8", errors="replace") as fin, \
             open(dst_file, "w", encoding="utf-8", newline="") as fout:
            reader = csv.reader(fin, delimiter="|")
            writer = csv.writer(fout, delimiter="|", lineterminator="\n")
            for row in reader:
                # partsupp: col1=partkey, col2=suppkey, col6=image_embedding(常为空), col7=text_embedding
                if len(row) >= 7:
                    img = row[5] if len(row) > 5 else ""
                    vec = row[6]
                    writer.writerow([row[0], row[1], img, vec])
                else:
                    print(f"Warning: {src_file} row has {len(row)} cols, skip")

        if (i + 1) % 80 == 0:
            print(f"partsupp_vector: processed {i + 1}/400 files")

    print("partsupp_vector CSV generation done.")


def main():
    print("Generating part_vector CSV files...")
    gen_part_vector_csv()

    print("Generating partsupp_vector CSV files...")
    gen_partsupp_vector_csv()

    print("All done. Output dirs:")
    print(f"  - {OUT_DIR / 'part_vector'}")
    print(f"  - {OUT_DIR / 'partsupp_vector'}")


if __name__ == "__main__":
    main()
