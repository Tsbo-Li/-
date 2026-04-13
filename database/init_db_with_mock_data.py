import os
import json
import sys
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# 将项目根目录加入系统路径，以便顺利导入其他模块
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root)

# 导入我们之前写的配置和模型
from configs.database_cfg import DatabaseConfig
from database.models import Base, StudentMetric, StudentText, StudentProfile

def init_database_and_seed_data():
    print("⏳ 1. 正在连接 PostgreSQL 数据库...")
    engine = create_engine(DatabaseConfig.get_pg_uri(), echo=False)
    
    print("🛠️ 2. 开始扫描 models.py 并同步表结构...")
    # 这行代码会自动比对数据库，缺失的表会自动创建
    Base.metadata.create_all(engine)
    print("✅ 表结构同步完成！")

    # 创建一个与数据库交互的 Session (会话)
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        # 防呆设计：检查数据库里是不是已经有数据了，防止重复插入报错
        if session.query(StudentMetric).first():
            print("⚠️ 数据库中已存在数据，跳过初始数据填充 (Seeding)。")
            return

        # 寻找之前生成的 mock_students.json 文件
        mock_data_path = os.path.join(project_root, 'data', 'mock_students.json')
        if not os.path.exists(mock_data_path):
            print(f"⚠️ 未找到仿真数据文件：{mock_data_path}。只创建了空表。")
            return

        print("📂 3. 正在读取 JSON 仿真数据并写入数据库 (可能需要几秒钟)...")
        with open(mock_data_path, 'r', encoding='utf-8') as f:
            students_data = json.load(f)

        # 遍历那 1000 个学生的 JSON 数据，组装成 SQLAlchemy 对象
        for data in students_data:
            # --- a. 插入基础特征表 ---
            metrics = data['structured_data']
            student = StudentMetric(
                student_id=data['student_id'],
                gpa=metrics['gpa'],
                failed_courses=metrics['failed_courses'],
                library_visits_per_month=metrics['library_visits_per_month'],
                late_return_count=metrics['late_return_count'],
                gaming_traffic_ratio=metrics['gaming_traffic_ratio'],
                breakfast_frequency=metrics['breakfast_frequency']
            )
            session.add(student)

            # --- b. 插入文本交互表 (一对多关联) ---
            for text_content in data['unstructured_texts']:
                text_record = StudentText(
                    student_id=data['student_id'],
                    content=text_content,
                    source_platform="仿真爬虫抓取"
                )
                session.add(text_record)

            # --- c. 初始化一个空的画像表 ---
            # (占个位置，后续跑完 DBSCAN 和 BERTopic 算法后再来 update 这个表)
            profile = StudentProfile(
                student_id=data['student_id']
            )
            session.add(profile)

        # 将以上所有的插入操作一次性提交到数据库
        session.commit()
        print(f"🎉 4. 大功告成！成功将 {len(students_data)} 条学生仿真数据写入 PostgreSQL！")

    except Exception as e:
        # 如果中间发生任何错误（比如字段不匹配），全部撤回，保证数据库干净
        session.rollback()
        import traceback
        print(f"❌ 写入数据时发生严重错误，已回滚事务。错误摘要: {e}")
        print("👇 详细的报错追踪信息如下：")
        print(traceback.format_exc())
    finally:
        session.close()

if __name__ == "__main__":
    init_database_and_seed_data()