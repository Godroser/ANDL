import json
import numpy as np

def calculate_ranking_loss(list_a, list_b):
    """
    计算 Pairwise Ranking Loss
    0 表示排名完全一致, 1 表示排名完全相反
    """
    if len(list_a) != len(list_b):
        raise ValueError("两组数据长度不一致，请检查输入。")
    
    n = len(list_a)
    discordant_pairs = 0
    total_pairs = n * (n - 1) / 2
    
    for i in range(n):
        for j in range(i + 1, n):
            # 检查 A 组和 B 组在 i 和 j 两个位置上的大小关系是否一致
            # 如果 (a_i - a_j) * (b_i - b_j) < 0，说明排名发生反转
            if (list_a[i] - list_a[j]) * (list_b[i] - list_b[j]) < 0:
                discordant_pairs += 1
                
    return discordant_pairs / total_pairs

# 1. 给定的第二组数据 (根据您的图片/之前的数据)
# 请确保顺序与 JSON 中的查询顺序一致 (Q1-Q22)
given_data = [
    22177.05,
    619939.89,
    305620.02,
    59586.5,
    319769.93,
    7522.82,
    32764.41,
    490092.43,
    735263.38,
    391973.56,
    328609.48,
    315859.66,
    25204.07,
    142331.92,
    10058.39,
    650128.98,
    229563.51,
    342154.76,
    173250.99,
    757513.4,
    163136.19,
    18651.98
]

# 2. 从 cost_report.json 读取数据
try:
    with open('cost_report.json', 'r') as f:
        report_data = json.load(f)
    
    # 提取 total_scan_cost_ms
    # 假设 JSON 格式为 {"Q1": {"total_scan_cost_ms": 100}, ...}
    json_values = []
    # 按 Q1 到 Q22 的顺序提取，避免 key 排序混乱
    for i in range(1, 23):
        key = f"Q{i}"
        if key in report_data:
            json_values.append(report_data[key].get("total_scan_cost_ms", 0.0))
        else:
            print(f"Warning: {key} not found in JSON.")

    # 3. 计算 Ranking Loss
    loss = calculate_ranking_loss(json_values, given_data)
    
    print("-" * 30)
    print(f"已处理查询数量: {len(json_values)}")
    print(f"Ranking Loss (Pairwise Error Rate): {loss:.4f}")
    print(f"排名一致性 (1 - Loss): {1-loss:.4f}")
    print("-" * 30)
    
except FileNotFoundError:
    print("错误：未找到 cost_report.json 文件。")
except Exception as e:
    print(f"发生错误: {e}")