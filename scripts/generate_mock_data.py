import json
import random
import os
import hashlib

# ==========================================
# 1. 定义三种极具代表性的学生群体特征分布
# ==========================================
STUDENT_PROFILES = {
    "Profile_A_沉浸高驱型": {
        "weight": 0.35, # 占总人数 35%
        # Tier 1 & 2: 结构化数值范围 (最小值, 最大值)
        "metrics": {
            "gpa": (3.5, 4.0),
            "failed_courses": (0, 0),
            "library_visits_per_month": (15, 40),
            "late_return_count": (0, 2),
            "gaming_traffic_ratio": (0.01, 0.10), # 游戏流量占比 1%-10%
            "breakfast_frequency": (0.8, 1.0)     # 按时吃早饭概率 80%-100%
        },
        # Tier 4: 非结构化文本库 (论坛发帖、搜索词、讨论区)
        "text_pool": [
            "[论坛] 寻找组队打ACM的同学，目前已有两个大牛",
            "[搜索] 图书检索：深度学习花书 PDF",
            "[留言] 老师，第四章的反向传播算法推导，我对偏导数那一步还有疑问",
            "[论坛] 国家奖学金答辩PPT模板求推荐",
            "[搜索] Python 异步并发 asyncio 最佳实践",
            "[论坛] 刚发了一篇顶会，分享一下这半年的肝帝生活"
        ]
    },
    
    "Profile_B_学业高压焦虑型": {
        "weight": 0.30, # 占总人数 30%
        "metrics": {
            "gpa": (2.0, 3.0),
            "failed_courses": (0, 2),
            "library_visits_per_month": (5, 20),
            "late_return_count": (3, 8),
            "gaming_traffic_ratio": (0.10, 0.30),
            "breakfast_frequency": (0.3, 0.6)
        },
        "text_pool": [
            "[论坛] 大三下学期了，保研无望，考研复习进度慢得想哭",
            "[搜索] 心理咨询中心预约电话",
            "[论坛] 真的感觉自己选错专业了，每天上课像听天书，极度内耗",
            "[搜索] 往届生春招补录信息",
            "[留言] 这次期中考试太难了，求老师海底捞",
            "[树洞] 昨晚失眠到凌晨四点，不知道未来在哪里，焦虑到头秃"
        ]
    },
    
    "Profile_C_边缘潜水娱乐型": {
        "weight": 0.35, # 占总人数 35%
        "metrics": {
            "gpa": (1.8, 2.8),
            "failed_courses": (1, 4),
            "library_visits_per_month": (0, 5),
            "late_return_count": (8, 15),
            "gaming_traffic_ratio": (0.60, 0.95), # 游戏流量占比 60%-95%
            "breakfast_frequency": (0.0, 0.3)
        },
        "text_pool": [
            "[论坛] 周末北门网吧五连坐，缺个打野，速来",
            "[搜索] 怎么绕过校园网晚上12点的断网限制",
            "[树洞] 这学期的水课有哪些？求推荐不用点名的",
            "[论坛] 新出的那个3A大作简直神作，昨晚通宵通关了",
            "[搜索] LOL 最新赛季上分攻略",
            "[留言] 签到。 (毫无营养的互动)"
        ]
    }
}

# ==========================================
# 2. 核心生成逻辑
# ==========================================
def generate_mock_students(num_students=1000):
    students_data = []
    
    # 按照权重分配三种学生的人数
    profiles = list(STUDENT_PROFILES.keys())
    weights = [STUDENT_PROFILES[p]["weight"] for p in profiles]
    
    for i in range(1, num_students + 1):
        # 生成哈希脱敏学号
        raw_id = f"2023{str(i).zfill(4)}"
        hashed_id = hashlib.md5(raw_id.encode()).hexdigest()[:10] 
        
        # 随机抽取当前学生的所属群体 (基于权重)
        chosen_profile = random.choices(profiles, weights=weights, k=1)[0]
        profile_data = STUDENT_PROFILES[chosen_profile]
        
        # 1. 组装结构化数据 (转为 float 格式以契合你们之前的 Dict[str, float] 定义)
        metrics = profile_data["metrics"]
        structured_data = {
            "gpa": round(random.uniform(*metrics["gpa"]), 2),
            "failed_courses": float(random.randint(*metrics["failed_courses"])),
            "library_visits_per_month": float(random.randint(*metrics["library_visits_per_month"])),
            "late_return_count": float(random.randint(*metrics["late_return_count"])),
            "gaming_traffic_ratio": round(random.uniform(*metrics["gaming_traffic_ratio"]), 3),
            "breakfast_frequency": round(random.uniform(*metrics["breakfast_frequency"]), 2)
        }
        
        # 2. 组装非结构化文本 (从该群体的词库中随机抽取 2-4 条)
        num_texts = random.randint(2, 4)
        unstructured_texts = random.sample(profile_data["text_pool"], num_texts)
        
        # 3. 构造最终的一条 JSON 记录
        student_record = {
            "student_id": f"STU_{hashed_id}",
            "structured_data": structured_data,
            "unstructured_texts": unstructured_texts
        }
        
        students_data.append(student_record)
        
    # 打乱顺序，防止同类数据扎堆
    random.shuffle(students_data)
    return students_data

if __name__ == "__main__":
    print("🚀 开始生成多维度仿真学生数据...")
    
    # 生成 1000 条数据
    TOTAL_STUDENTS = 1000
    mock_data = generate_mock_students(TOTAL_STUDENTS)
    
    # 确保输出到项目根目录下的 data 目录
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    data_dir = os.path.join(project_root, "data")
    os.makedirs(data_dir, exist_ok=True)
    
    # 保存文件
    output_path = os.path.join(data_dir, "mock_students.json")
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(mock_data, f, ensure_ascii=False, indent=2)
        
    print(f"✅ 成功生成 {TOTAL_STUDENTS} 条学生数据，已保存至 {output_path}")